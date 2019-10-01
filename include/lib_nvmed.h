/*
 * NVMeDirect Userspace Library
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

#ifndef _LIB_NVMED_H
#define _LIB_NVMED_H

#ifdef __cplusplus
extern "C" {
#endif

#include <unistd.h>
#include <linux/types.h>
#include <sys/types.h>
#include <sys/queue.h>
#include <pthread.h>
#include "./radix-tree.h"

#define PAGE_SIZE		sysconf(_SC_PAGESIZE)

#define NVMED_BUF_MAGIC	0x4E564DED		//NVM'ED'
#define NVMED_NUM_PREALLOC_PRP	64
#define NVMED_CACHE_FORCE_EVICT_MAX	4
/* 4MB个page.共16GB的cache */
#define NVMED_CACHE_INIT_NUM_PAGES	(256*1024*16)	// 256 -> 1MB

#define COMPILER_BARRIER() asm volatile("" ::: "memory")

#define QtoD(queue)		queue->nvmed
#define HtoQ(handle) 	handle->queue
#define HtoD(handle) 	QtoD(HtoQ(handle))

typedef enum {
	NVMED_FALSE = 0,
	NVMED_TRUE = 1,
} NVMED_BOOL;

#define FLAG_SET(obj, items) (obj)->flags |= items
#define FLAG_SET_FORCE(obj, items) (obj)->flags = items
//#define FLAG_SET_SYNC(obj, items) __sync_or_and_fetch(&obj->flags, items)
#define FLAG_SET_SYNC(obj, items) FLAG_SET(obj, items)
#define FLAG_UNSET(obj, items) (obj)->flags &= ~items
//#define FLAG_UNSET_SYNC(obj, items) __sync_and_and_fetch(&obj->flags, ~items)
#define FLAG_UNSET_SYNC(obj, items) FLAG_UNSET(obj, items)
#define __FLAG_ISSET(flags, items) (flags & items)? NVMED_TRUE:NVMED_FALSE
#define __FLAG_ISSET_SYNC(flags, items) (flags & items)? NVMED_TRUE:NVMED_FALSE
//#define __FLAG_ISSET_SYNC(obj , items) __sync_and_and_fetch(&obj->flags, items)? true:false
#define FLAG_ISSET(obj, items) __FLAG_ISSET((obj)->flags, items)
#define FLAG_ISSET_SYNC(obj, items) __FLAG_ISSET((obj)->flags, items)
//#define FLAG_ISSET_SYNC(obj, items) __FLAG_ISSET_SYNC(obj, items)

#define INC_SYNC(obj)	__sync_add_and_fetch(&obj, 1);
#define DEC_SYNC(obj)	__sync_sub_and_fetch(&obj, 1);
/*
 * type __sync_and_and_fetch (type *ptr, type value, ...)
 * 将*ptr与value相与，结果更新到*ptr，并返回操作之后新*ptr的值
*/
#define INIT_SYNC(obj)	__sync_and_and_fetch(&obj, 0);

typedef __u64 u64;
typedef __u32 u32;
typedef __u16 u16;
typedef __u8  u8;

//FLAGS for NVMED
enum {
	NVMED_NO_CACHE			= 1 << 0,
	NVMED_CACHE_SIZE		= 1	<< 1,
	NVMED_CACHE_LAZY_INIT	= 1	<< 2,

	NVMED_NUM_FLAGS			= 3,
};

//FLAGS for NVMED_QUEUE
enum {
	QUEUE_MANUAL_COMPLETION	= 1 << 0,
	QUEUE_INTERRUPT			= 1 << 1,

	QUEUE_NUM_FLAGS			= 2
};
/* 中断开启的queue,自己有独立的中断监控线程处理cqe */
#define QUEUE_MANUAL_CQ (QUEUE_MANUAL_COMPLETION | QUEUE_INTERRUPT)

//FLAGS For NVMED_HANDLE
enum {
	HANDLE_DIRECT_IO		= 1 << 0,
	/* 写完一个cmnd,需要等到io返回.不能在io_head上积攒cache entry批量写入 */
	HANDLE_SYNC_IO			= 1 << 1,
	/* 从nvmed_get_buffer中获取nvme_buf */
	HANDLE_HINT_DMEM 		= 1 << 2,
	/* 一个handle对应多个queue */
	HANDLE_MQ				= 1 << 3,

	HANDLE_INTERRUPT		= 1 << 4,

	HANDLE_NUM_FLAGS		= 5,
};

//STATUS for Process CQ, IOSCHED thread
enum {
	TD_STATUS_RUNNING		= 1 << 0,
	TD_STATUS_REQ_STOP		= 1 << 1,
	/* 初始化时cq thread状态是TD_STATUS_STOP的 */
	TD_STATUS_STOP			= 1 << 2,
	TD_STATUS_REQ_SUSPEND	= 1 << 3,
	TD_STATUS_SUSPEND		= 1 << 4,

	TD_STATUS_NUM_FLAGS		= 5,
};

//FLAGS For NVMED_CACHE
enum {
	/* 为了支持lazy_init. lazy init的cache在从freelist上取出cache的时候才做内存映射 */
	CACHE_UNINIT			= 0,
	/*
	 * 设置: 在buf write中将cache放入iod的数组中,准备读数据填充cache的时候.
	 * 		 在buf read调用填充cache的时候,也会lock cache.
	 * 取消: 在iod完成时会清除lock并且释放所有cache
	 * 判断: buf write的时候获取block对应的cache,这个时候需要确保cache是没有在io下发流程
	 */
	CACHE_LOCKED			= 1 << 0,
	/* 从freelist上拿下就要去掉这个标志 */
	CACHE_FREE				= 1 << 1,
	/* 在LRU上 */
	CACHE_LRU				= 1 << 2,
	/*
	 * 设置: buf write将用户buffer拷贝到cache中设置dirty.
	 * 取消: 在iod完成时会清除dirty
	 * 判断: 在从lru上拿cache的时候判断.不能将dirty的cache分配给其他人
	 */
	CACHE_DIRTY				= 1 << 3,
	/* 在读取数据填充cache的时候,以及io完成的时候会设置 */
	CACHE_UPTODATE			= 1 << 4,
	/* cache在io_head上经过积攒,需要一次性回写 */
	CACHE_WRITEBACK			= 1 << 5,

	CACHE_NUM_FLAGS			= 6,
};

//STATUS FOR IOD
enum {
	/* 发起io */
	IO_INIT					= 1 << 0,
	/* iod已经完成,所有资源都已经释放 */
	IO_COMPLETE				= 1	<< 1,
	IO_ERROR				= 1 << 2,

	IO_NUM_FLAGS			= 3,
};

//STATUS FOR IOD INTERRUPT
enum {
	/* 不使用中断 */
	IOD_INTR_INACTIVE		= 0,
	/* 使用中断 */
	IOD_INTR_INIT			= 1 << 0,
	/* 等待中断 */
	IOD_INTR_WAITING		= 1 << 1,
	IOD_INTR_COMPLETE		= 1 << 2,

	IOD_INTR_NUM_FLAGS		= 3,
};

//FLAGS FOR AIO
enum {
	NVMED_AIO_ERROR			= 1 << 0,
	NVMED_AIO_BUSY			= 1	<< 1,
	NVMED_AIO_QUEUED		= 1	<< 2,

	NVMED_AIO_STATUS		= 3,
};

enum {
	AIO_INIT				= 0,
	AIO_PROCESS			 	= 1 << 1,
	AIO_COMPLETE			= 1 << 2,
	
	AIO_NUM_FLAGS			= 3,
};

typedef struct nvmed_device_info NVMED_DEVICE_INFO;

typedef struct nvmed {
	/* proc下admin节点的路径 */
	char*	ns_path;
	/* proc下admin节点的路径fd */
	int 	ns_fd;
	u32 	flags;
	
	NVMED_DEVICE_INFO *dev_info;
	
	pthread_spinlock_t 	mngt_lock;

	/* queue的个数 */
	int numQueue;
	LIST_HEAD(queue_list, nvmed_queue) queue_head;

	unsigned int num_cache_size;
	/* cache entry的个数 */
	unsigned int num_cache_usage;

	//LRU TAILQ Order :
	//	[HEAD] LRu ------------- MRu [TAIL]
	/*
	 * free_head上保留还未使用的cache.
	 * cache命中会将cache插入LRU的尾部.LRU的头部最少使用,会优先替换
	 */
	TAILQ_HEAD(cache_list, nvmed_cache) lru_head, free_head;
	/* 保存slot.每一个slot获取一段内存分给cache entry */
	LIST_HEAD(slot_list, nvmed_cache_slot) slot_head;
	pthread_rwlock_t cache_radix_lock;
	pthread_spinlock_t cache_list_lock;

	/*
	 * 正在使用的cache.如果一个cache被从lru上取下来分配给其他人,
	 * 那么cache也需要从radix tree上取下来.之后会重新插入
	 */
	struct radix_tree_root cache_root;

	pthread_t process_cq_td;
	volatile unsigned int process_cq_status;
	pthread_mutex_t process_cq_mutex;	
	pthread_cond_t  process_cq_cond;

} NVMED;

typedef struct nvmed_queue {
	NVMED* 	nvmed;
	u32 	flags;

	pthread_spinlock_t 	mngt_lock;
	pthread_spinlock_t 	sq_lock;
	pthread_spinlock_t 	cq_lock;
	
	u16 qid;
	u32 q_depth;

	int sq_fd;
	int cq_fd;
	int db_fd;
	struct nvme_command 	*sq_cmds;
	volatile struct nvme_completion *cqes;
	volatile struct nvme_completion *cqe;
	void *dbs;
	u32 *sq_db;
	u32 *cq_db;

	u16 sq_head, sq_tail, cq_head;
	/* cq_phase需要和nvme completion的p bit相同(p bit是controller设置的) */
	u8	cq_phase, cqe_seen;

	struct nvmed_iod* iod_arr;
	unsigned int	  iod_pos;
	pthread_spinlock_t iod_arr_lock;

	int numHandle;

	unsigned int aio_q_head;

	pthread_t process_cq_intr;

	LIST_HEAD(handle_list, nvmed_handle) handle_head;

	LIST_ENTRY(nvmed_queue) queue_list;
} NVMED_QUEUE;

typedef struct nvmed_handle {
	struct nvmed_queue* queue;
	struct nvmed_queue** queue_mq;
	u32	flags;

	ssize_t (*read_func)(struct nvmed_handle*, u8, 
			void*, unsigned long, unsigned int, NVMED_BOOL, void*);
	ssize_t (*write_func)(struct nvmed_handle*, u8, 
			void*, unsigned long, unsigned int, NVMED_BOOL, void*);

	/* 磁盘上的位置,以字节为单位 */
	off_t	offset;
	off_t bufOffs;

	/* 对应的queue的个数 */
	int num_mq;
	NVMED_QUEUE* (*mq_get_queue)(struct nvmed_handle*, u8, 
			unsigned long, unsigned int);

	void* prpBase;
	pthread_spinlock_t prpBuf_lock;
	/* void *数组.每一个prpBuf指向了一个page大小的虚拟地址 */
	void** prpBuf;
	/* prpBuf对应的物理地址数组 */
	u64* pa_prpBuf;
	int prpBuf_size;
	/* prp的可用page */
	int prpBuf_curr;
	/* 指向已经获取的page */
	int prpBuf_head;
	/* 指向下一个要释放page的位置 */
	int prpBuf_tail;

	unsigned int dispatched_io;
	/* 用于积攒io. 128个连续的io才下发. 只要io挂到io_head上就需要设置CACHE_WRITEBACK */
	TAILQ_HEAD(io_list, nvmed_cache) io_head;
	/* io_head上entry的数量 */
	int num_io_head;
	pthread_spinlock_t io_head_lock;

	LIST_HEAD(handle_cache_list, nvmed_cache) dirty_list;
	pthread_spinlock_t 	dirty_list_lock;

	LIST_ENTRY(nvmed_handle) handle_list;
} NVMED_HANDLE;

typedef struct nvmed_aio_ctx {
	NVMED_HANDLE* handle;
	off_t start_lba;
	size_t len;
	void* buf;
	u64* prpList;

	u8 opcode;
	volatile int status;
	int num_init_io;
	int num_complete_io;
	
	void* private_data;
	void* cb_userdata;
	/* 当一个io执行完成后执行 */
	void (*aio_callback)(const struct nvmed_aio_ctx *context, void *userdata);
} NVMED_AIO_CTX;

typedef struct nvmed_cache {
	/* 4k的block index */
	unsigned int lpaddr;
	volatile u32 flags;
	/* 读命中和miss的统计. */
	u32 ref;
	/* ptr的物理地址 */
	u64 paddr;
	/* 用户buffer数据会拷贝到这里 */
	void* ptr;

	NVMED_HANDLE* handle;

	/* 用于加入lru */
	TAILQ_ENTRY(nvmed_cache) cache_list;
	/* 用于加入temp list和io_head */
	TAILQ_ENTRY(nvmed_cache) io_list;
	/* 用于加入dirty_list */
	LIST_ENTRY(nvmed_cache) handle_cache_list;
} NVMED_CACHE;

typedef struct nvmed_cache_slot {
	struct nvmed_cache *cache_info;
	void* cache_ptr;
	unsigned int size;

	LIST_ENTRY(nvmed_cache_slot) slot_list;
} NVMED_CACHE_SLOT;

typedef struct nvmed_iod {
	/* 所属queue的sq tail */
	u16 sq_id;

	NVMED_HANDLE* nvmed_handle;

	/* prp page的虚拟地址 */
	void* prp_addr;
	/* prp page的物理地址 */
	u64 prp_pa;

	unsigned int status;
	unsigned int num_cache;
	struct nvmed_cache** cache;

	/* For Intrrupt completion */
	u32 intr_status;
	pthread_mutex_t intr_cq_mutex;	
	pthread_cond_t  intr_cq_cond;

	/* For AIO */
	NVMED_AIO_CTX* context;
} NVMED_IOD;

NVMED* nvmed_open(char* PATH, int flags);
int nvmed_close(NVMED*);
int nvmed_feature_get(NVMED* nvmed, int feature);
int nvmed_feature_set(NVMED* nvmed, int feature, int value);

NVMED_QUEUE* nvmed_queue_create(NVMED*, int);
int nvmed_queue_destroy(NVMED_QUEUE*);

NVMED_HANDLE* nvmed_handle_create(NVMED_QUEUE*, int);
int nvmed_handle_destroy(NVMED_HANDLE*);

NVMED_HANDLE* nvmed_handle_create_mq(NVMED_QUEUE**, int, int,
		NVMED_QUEUE* (*func)(NVMED_HANDLE*, u8, unsigned long, unsigned int));
int nvmed_handle_destroy_mq(NVMED_HANDLE*);

int nvmed_handle_feature_get(NVMED_HANDLE*, int);
int nvmed_handle_feature_set(NVMED_HANDLE*, int, int);

void* nvmed_get_buffer(NVMED*, unsigned int num_pages);
void nvmed_put_buffer(void*);

off_t nvmed_lseek(NVMED_HANDLE*, off_t, int);
ssize_t nvmed_read(NVMED_HANDLE*, void*, size_t);
ssize_t nvmed_pread(NVMED_HANDLE*, void*, size_t, off_t);
ssize_t nvmed_write(NVMED_HANDLE*, void*, size_t);
ssize_t nvmed_pwrite(NVMED_HANDLE*, void*, size_t, off_t);

void nvmed_flush(NVMED_HANDLE*);
int nvmed_discard(NVMED_HANDLE*, unsigned long, unsigned int);

int nvmed_aio_queue_submit(NVMED_HANDLE*);
int nvmed_aio_read(NVMED_AIO_CTX*);
int nvmed_aio_write(NVMED_AIO_CTX*);
int nvmed_aio_handle_complete(NVMED_HANDLE*);

int nvmed_set_user_quota(NVMED*, uid_t, unsigned int, 
		unsigned int*, unsigned int *);
int nvmed_get_user_quota(NVMED*, uid_t, 
		unsigned int*, unsigned int *);

int virt_to_phys(NVMED* nvmed, void* addr, u64* paArr, unsigned int num_bytes);

#ifdef __cplusplus
}
#endif

#endif /* _LIB_NVMED_H */
