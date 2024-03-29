/*
 * NVMeDirect Device Driver
 *
 * Copyright (c) 2016 Computer Systems Laboratory, Sungkyunkwan University.
 * http://csl.skku.edu
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

#ifndef _NVMED_MODULE_H
#define _NVMED_MODULE_H

#include <linux/list.h>
#include <linux/kthread.h>
#include <linux/version.h>
#include <linux/blkdev.h>
#include <linux/interrupt.h>
#include <linux/msi.h>

#include "../include/nvmed.h"

#define NVMED_ERR(string, args...) printk(KERN_ERR string, ##args)
#define NVMED_INFO(string, args...) printk(KERN_INFO string, ##args)
#define NVMED_DEBUG(string, args...) printk(KERN_DEBUG string, ##args)

#define PCI_CLASS_NVME	0x010802

#define KERNEL_VERSION_CODE	KERNEL_VERSION(KERNEL_VERSION_MAJOR, \
										KERNEL_VERSION_MINOR, 0)

/* nvme dev entry向上找到pci_dev最终找到设备驱动模型的dev */
#define	DEV_ENTRY_TO_DEVICE(dev_entry) &dev_entry->pdev->dev
/* ns entry向上找到nvmed device entry最终找到nvme dev */
#define NS_ENTRY_TO_DEV(ns_entry) ns_entry->dev_entry->dev

#include "../include/nvme_hdr.h"

#if KERNEL_VERSION_CODE == KERNEL_VERSION(4,9,0)
	#define KERN_490
	#include "nvme.h"
#endif

#define DEV_TO_ADMINQ(dev) dev->ctrl.admin_q
#define DEV_TO_INSTANCE(dev) dev->ctrl.instance
#define DEV_TO_HWSECTORS(dev) dev->ctrl.max_hw_sectors
#define DEV_TO_STRIPESIZE(dev) dev->ctrl.stripe_size
#define DEV_TO_VWC(dev) dev->ctrl.vwc
#define DEV_TO_NS_LIST(dev) dev->ctrl.namespaces

#define TRUE	1
#define FALSE	0

#ifdef NVMED_CORE_HEADERS


#define NVMED_SET_FEATURES(dev_entry, fid, dword11, dma_addr, result) \
			nvmed_set_features_fn(&dev_entry->dev->ctrl, fid, dword11, NULL, 0, result)
#define NVMED_GET_FEATURES(dev_entry, fid, result) \
			nvmed_get_features_fn(&dev_entry->dev->ctrl, fid, 0, NULL, 0, result)
int (*nvmed_set_features_fn)(struct nvme_ctrl *dev, unsigned fid, unsigned dword11,
	void *buffer, size_t buflen, u32 *result) = NULL;
int (*nvmed_get_features_fn)(struct nvme_ctrl *dev, unsigned fid, unsigned nsid,
	void *buffer, size_t buflen, u32 *result) = NULL;

int (*nvmed_submit_cmd_mq)(struct request_queue *q, struct nvme_command *cmd,
	void *buf, unsigned bufflen) = NULL;

int (*nvmed_submit_cmd)(struct nvme_dev *, struct nvme_command *, 
		u32 *result) = NULL;

unsigned char admin_timeout = 60;
#define ADMIN_TIMEOUT		(admin_timeout * HZ)

struct proc_dir_entry *NVMED_PROC_ROOT;

static LIST_HEAD(nvmed_dev_list);

#endif //NVMED_CORE_HEADERS

#define NVMED_MSIX_HANDLER_V2

struct async_cmd_info {
	struct kthread_work work;
	struct kthread_worker *worker;
	struct request *req;
	u32 result;
	int status;
	void *ctx;
};

/* 几乎和内核的pci.c中定义的struct nvme_queue完全一致 */
struct nvme_queue {
	/* 通用设备驱动模型的dev,指向pci func */
	struct device *q_dmadev;
	struct nvme_dev *dev;
	spinlock_t q_lock;
	struct nvme_command *sq_cmds;
	struct nvme_command __iomem *sq_cmds_io;
	volatile struct nvme_completion *cqes;
	dma_addr_t sq_dma_addr;
	dma_addr_t cq_dma_addr;
	/* db的地址 */
	u32 __iomem *q_db;
	u16 q_depth;
	u16 sq_head;
	u16 sq_tail;
	u16 cq_head;
	u16 qid;
	u8 cq_phase;
	u8 cqe_seen;
};

typedef struct nvmed_user_quota_entry {
	kuid_t uid;
	unsigned int queue_max;
	unsigned int queue_used;

	/* 链表成员,链表头是nvmed_ns_entry->list */
	struct list_head list;
} NVMED_USER_QUOTA_ENTRY;

struct nvme_irq_desc {
	/* cq使用的msix entry的index */
	int cq_vector;
	irq_handler_t handler;
	irq_handler_t thread_fn;
	const struct cpumask *affinity_hint;
	const char *irqName;
	void *queue;
};

/* nvmed设备entry,记录nvme设备/pci设备以及中断 */
typedef struct nvmed_dev_entry {
	struct nvme_dev *dev;
	struct pci_dev *pdev;

	spinlock_t ctrl_lock;

	/* 用户调用ioctl创建的queue pair */
	unsigned int num_user_queue;
	/* 一个nvmed entry最多65536个queue */
	DECLARE_BITMAP(queue_bmap, 65536);

	struct list_head list;
	/* 链表头,用于连接所有ns */
	struct list_head ns_list;
	
	// Intr Support
	unsigned int vec_max;  // 0 based
	unsigned int vec_kernel;
	unsigned long *vec_bmap;
	unsigned int vec_bmap_max;
	struct nvme_irq_desc *desc;
	/* kernel中pci.h有定义.msix_entry数组 */
	struct msix_entry *msix_entry;
} NVMED_DEV_ENTRY;

/* 记录sys下的entry以及nvme ns */
typedef struct nvmed_ns_entry {
	NVMED_DEV_ENTRY *dev_entry;

	struct nvme_ns *ns;
	
	struct proc_dir_entry *ns_proc_root;
	struct proc_dir_entry *proc_admin;
	struct proc_dir_entry *proc_sysfs_link;

	/* 用于连接到nvmed entry */
	struct list_head list;

	struct list_head queue_list;
	/* 链表头,链表成员是nvmed_user_quota_entry->list */
	struct list_head user_list;
	
	int partno;

	sector_t start_sect;
	sector_t nr_sects;
} NVMED_NS_ENTRY;

typedef struct nvmed_queue_entry {
	NVMED_NS_ENTRY *ns_entry;
	
	struct proc_dir_entry *queue_proc_root;
	struct proc_dir_entry *proc_sq;
	struct proc_dir_entry *proc_cq;
	struct proc_dir_entry *proc_db;

	struct nvme_queue* nvmeq;

	kuid_t owner;

	struct list_head list;
	
	// Intr Support
	unsigned int irq_vector;
	char* irq_name;
	atomic_t nr_intr;
} NVMED_QUEUE_ENTRY;

static inline bool check_msix(NVMED_DEV_ENTRY *dev_entry) {
	struct pci_dev *pdev = dev_entry->pdev;

	return pdev->msix_enabled;
}

/* nvmed-core.c */
NVMED_QUEUE_ENTRY* nvmed_get_queue_from_qid(NVMED_NS_ENTRY *ns_entry, 
		unsigned int qid);

/* nvmed-intr.c */
NVMED_RESULT nvmed_register_intr_handler(NVMED_DEV_ENTRY *dev_entry,
		NVMED_QUEUE_ENTRY *queue, unsigned int irq_vector);
NVMED_RESULT nvmed_free_intr_handler(NVMED_DEV_ENTRY *dev_entry, NVMED_QUEUE_ENTRY *queue,
		unsigned int qid);
int nvmed_irq_comm(NVMED_NS_ENTRY *ns_entry, unsigned long __user *__qid);

#endif
