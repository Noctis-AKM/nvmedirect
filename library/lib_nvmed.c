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

#include "../include/nvmed.h"
#include "../include/nvme_hdr.h"
#include "../include/lib_nvmed.h"
#include "../include/radix-tree.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <malloc.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/ioctl.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <pthread.h>
#include <errno.h>

#define nvmed_printf(fmt, args...) fprintf(stdout, fmt, ##args)
#define nvmed_err(fmt, args...) fprintf(stderr, fmt, ##args)

ssize_t nvmed_io_rw(NVMED_HANDLE* nvmed_handle, u8 opcode, void* buf,
		unsigned long start_lba, unsigned int len, NVMED_BOOL pio, void* private);
ssize_t nvmed_cache_io_rw(NVMED_HANDLE* nvmed_handle, u8 opcode, NVMED_CACHE *__cache,
		unsigned long start_lba, unsigned int len, int __flag);
ssize_t nvmed_buffer_read(NVMED_HANDLE* nvmed_handle, u8 opcode, void* buf,
		unsigned long start_lba, unsigned int len, NVMED_BOOL pio, void* private);
ssize_t nvmed_buffer_write(NVMED_HANDLE* nvmed_handle, u8 opcode, void* buf,
		unsigned long start_lba, unsigned int len, NVMED_BOOL pio, void* private);

void nvmed_flush_handle(NVMED_HANDLE* nvmed_handle);
/*
 * Translate virtual memory address to physical memory address
 */
int virt_to_phys(NVMED* nvmed, void* addr, u64* paArr, unsigned int num_bytes) {
	struct nvmed_buf nvmed_buf;
	unsigned int num_pages;
	int ret;

	num_pages = num_bytes / PAGE_SIZE;
	if(num_bytes % PAGE_SIZE > 0) num_pages++;

	nvmed_buf.addr = addr;
	nvmed_buf.size = num_pages;
	nvmed_buf.pfnList = paArr;

	ret = ioctl(nvmed->ns_fd, NVMED_IOCTL_GET_BUFFER_ADDR, &nvmed_buf);
	if(ret < 0) return 0;

	return num_pages;
}

/*
 * Allocation cache slot and memory
 */
int nvmed_cache_alloc(NVMED* nvmed, unsigned int size, NVMED_BOOL lazy_init) {
	int i;
	unsigned int req_size;
	NVMED_CACHE_SLOT *slot;
	NVMED_CACHE *info;
	u64 *paList;


	if(size == 0) return -NVMED_FAULT;
	if(size == nvmed->num_cache_size) return 0;
	/* cache不支持shrink */
	if(size < nvmed->num_cache_size) {
		nvmed_printf("%s: Cache shrinking is not supported\n", nvmed->ns_path);
		return -NVMED_FAULT;
	}

	pthread_spin_lock(&nvmed->mngt_lock);

	/* 额外请求的page个数. */
	req_size = size - nvmed->num_cache_size;
	slot = malloc(sizeof(NVMED_CACHE_SLOT));
	/* 分配req_size个NVME_CACHE */
	slot->cache_info = calloc(req_size, sizeof(NVMED_CACHE));
	/* 给cache分配req_size个PAGE,使用MAP_LOCKED,提前fault */
	slot->cache_ptr = mmap(NULL, PAGE_SIZE * req_size, PROT_READ | PROT_WRITE, 
			MAP_ANONYMOUS | MAP_LOCKED | MAP_SHARED, -1, 0);
	if(slot->cache_ptr == NULL) {
		nvmed_err("Failed: Cache Allocation : %u pages\n", req_size);
		pthread_spin_unlock(&nvmed->mngt_lock);
		return -1;
	}
	slot->size = req_size;
	/* 将slot插入nvmed的链表 */
	LIST_INSERT_HEAD(&nvmed->slot_head, slot, slot_list);

	/* Initialize memory and translate virt to phys addr */
	if(!lazy_init) {
		paList = calloc(1, sizeof(u64) * req_size);
		/* 获取cache_ptr的物理地址,保存在paList中 */
		virt_to_phys(nvmed, slot->cache_ptr, paList, PAGE_SIZE * req_size);
	}

	/* fill cache info and add to free list */
	for(i=0; i<req_size; i++) {
		/* 初始化slot中的所有cache entry */
		info = slot->cache_info + i;
		info->lpaddr = 0;
		info->ref = 0;
		if(lazy_init == NVMED_FALSE) {
			info->paddr = paList[i];
			/* 非lazy_init的cache.在建立cache的时候已经做了地址映射 */
			FLAG_SET(info, CACHE_FREE);
		}
		else {
			info->paddr = 0;
			/* lazy_init的cache,从freelist上取出cache的时候再做映射.设置CACHE_UNINIT */
			FLAG_SET(info, CACHE_UNINIT | CACHE_FREE);
		}
		/* cache的虚拟地址 */
		info->ptr = slot->cache_ptr + (i*PAGE_SIZE);

		TAILQ_INSERT_HEAD(&nvmed->free_head, info, cache_list);
	}

	free(paList);
	/* nvmed中cache的数量 */
	nvmed->num_cache_size = size;

	pthread_spin_unlock(&nvmed->mngt_lock);

	return req_size;
}

/*
 * Get PRP Buffer of Handle
 */
/* 从handle中获取一个prp buffer */
void* nvmed_handle_get_prp(NVMED_HANDLE* nvmed_handle, u64* pa) {
	void* ret_addr;
	int head;

	/* 如果获取不到prp就一直循环 */
	while(1) {
		pthread_spin_lock(&nvmed_handle->prpBuf_lock);
		/* prpBuf中可以使用的buf */
		if(nvmed_handle->prpBuf_curr != 0) {
			head = nvmed_handle->prpBuf_head;
			/* 获取到可用的buf */
			ret_addr = nvmed_handle->prpBuf[head];
			/* 同时返回物理地址 */
			*pa = nvmed_handle->pa_prpBuf[head];
			/* 如果到了尾部,head需要重新设置成0 */
			if(++head == nvmed_handle->prpBuf_size) head = 0;
			nvmed_handle->prpBuf_head = head;
			/* 可用的prp page减少 */
			nvmed_handle->prpBuf_curr--;
			pthread_spin_unlock(&nvmed_handle->prpBuf_lock);
			break;
		}
		pthread_spin_unlock(&nvmed_handle->prpBuf_lock);
	}

	return ret_addr;
}

/*
 * Put PRP Buffer of Handle
 */
int nvmed_handle_put_prp(NVMED_HANDLE* nvmed_handle, void* buf, u64 pa) {
	int tail;
	pthread_spin_lock(&nvmed_handle->prpBuf_lock);
	tail = nvmed_handle->prpBuf_tail;
	/* prp buf指向buf头部 */
	nvmed_handle->prpBuf[tail] = buf;
	nvmed_handle->pa_prpBuf[tail] = pa;
	/* tail递增 */
	if(++tail == nvmed_handle->prpBuf_size) tail = 0;
	nvmed_handle->prpBuf_tail = tail;
	/* prp可用page增加 */
	nvmed_handle->prpBuf_curr++;
	pthread_spin_unlock(&nvmed_handle->prpBuf_lock);

	return 0;
}

/*
 * Complete IOD
 * (put PRP Buffer or AIO Callback
 */
void nvmed_complete_iod(NVMED_IOD* iod) {
	NVMED_HANDLE* nvmed_handle;
	NVMED* nvmed;
	NVMED_CACHE* cache;
	//NVMED_QUEUE* nvmed_queue;
	int i;

	nvmed_handle = iod->nvmed_handle;
	//nvmed_queue = HtoQ(nvmed_handle);
	nvmed = HtoD(nvmed_handle);
	/* io完成,释放iod的prp page到handle的prp队列中,供其他人使用 */
	if(iod->prp_addr != NULL)
		nvmed_handle_put_prp(nvmed_handle, iod->prp_addr, iod->prp_pa);

	if(iod->context != NULL) {
		/* 完成的io个数增加 */
		iod->context->num_complete_io++;
		if(iod->context->num_init_io == iod->context->num_complete_io) {
			/* 异步io的所有io完成,调用异步io的callback */
			iod->context->status = AIO_COMPLETE;
			if(iod->context->aio_callback) {
				iod->context->aio_callback(iod->context, iod->context->cb_userdata);
				iod->context = NULL;
			}
		}
	}

	if(iod->intr_status) {
		/* 中断监控函数调用进来的,唤醒中断等待线程 */
		while(iod->intr_status != IOD_INTR_WAITING);

		pthread_mutex_lock(&iod->intr_cq_mutex);
		pthread_cond_signal(&iod->intr_cq_cond);
		pthread_mutex_unlock(&iod->intr_cq_mutex);
	}

	/* 处理iod上的cache */
	if(iod->num_cache != 0) {
		//pthread_spin_lock(&nvmed_queue->iod_arr_lock);
		for(i=0; i<iod->num_cache; i++) {
			cache = iod->cache[i];
			pthread_spin_lock(&nvmed->cache_list_lock);

			pthread_spin_lock(&nvmed_handle->dirty_list_lock);
			/* 取消cache的locked和writeback标志 */
			FLAG_UNSET_SYNC(cache, CACHE_LOCKED);
			FLAG_UNSET_SYNC(cache, CACHE_WRITEBACK);

			if(FLAG_ISSET_SYNC(cache, CACHE_DIRTY)) {
				LIST_REMOVE(cache, handle_cache_list);
				FLAG_UNSET_SYNC(cache, CACHE_DIRTY);
			}
			else {
				//nvmed_err("Rxxxxx %u\n", cache->lpaddr);
			}

			/* 设置cache uptodate标记 */
			FLAG_SET_SYNC(cache, CACHE_UPTODATE);
			pthread_spin_unlock(&nvmed_handle->dirty_list_lock);

			pthread_spin_unlock(&nvmed->cache_list_lock);
		}
		/* 处理完cache,就可以释放它 */
		free(iod->cache);
		//pthread_spin_unlock(&nvmed_queue->iod_arr_lock);
	}
	/* 派发的io数量-1 */
	__sync_fetch_and_sub(&nvmed_handle->dispatched_io, 1);
	/* io完成 */
	iod->status = IO_COMPLETE;
}

/*
 * Polling specific Completion queue for AIO
 * return : number of completed I/O
 */
#define COMPLETE_QUEUE_MAX_PROC 32
int nvmed_queue_complete(NVMED_QUEUE* nvmed_queue) {
	NVMED* nvmed;
	NVMED_IOD* iod;
	volatile struct nvme_completion *cqe;
	u16 head, phase;
	int num_proc = 0;

	nvmed = nvmed_queue->nvmed;

	pthread_spin_lock(&nvmed_queue->cq_lock);
	head = nvmed_queue->cq_head;
	phase = nvmed_queue->cq_phase;
	/* 来一次中断,清理所有的cqe */
	for(;;) {
		cqe = (volatile struct nvme_completion *)&nvmed_queue->cqes[head];
		/* phase bit不一样,说明cq已经空了 */
		if((cqe->status & 1) != nvmed_queue->cq_phase)
			break;

		/* 返回头部 */
		if(++head == nvmed->dev_info->q_depth) {
			head = 0;
			/* phase翻转 */
			phase = !phase;
		}

		iod = nvmed_queue->iod_arr + cqe->command_id;
		nvmed_complete_iod(iod);
		num_proc++;
		if(head == 0 || num_proc == COMPLETE_QUEUE_MAX_PROC) break;
	}
	/* 没有cqe完成 */
	if(head == nvmed_queue->cq_head && phase == nvmed_queue->cq_phase) {
		pthread_spin_unlock(&nvmed_queue->cq_lock);
		return num_proc;
	}

	/* 更新db需要加barrier,同时更新cq的head和phase */
	COMPILER_BARRIER();
	*(volatile u32 *)nvmed_queue->cq_db = head;
	nvmed_queue->cq_head = head;
	nvmed_queue->cq_phase = phase;
	pthread_spin_unlock(&nvmed_queue->cq_lock);

	return num_proc;
}

/*
 * I/O Completion of specific handle ( for AIO )
 */
int nvmed_aio_handle_complete(NVMED_HANDLE* nvmed_handle) {
	NVMED_QUEUE* nvmed_queue = HtoQ(nvmed_handle);

	return nvmed_queue_complete(nvmed_queue);
}

/*
 * I/O Completion of specific I/O
 * target_id : submission id
 */
void nvmed_io_polling(NVMED_HANDLE* nvmed_handle, u16 target_id) {
	NVMED* nvmed;
	NVMED_QUEUE* nvmed_queue;
	NVMED_IOD* iod;
	volatile struct nvme_completion *cqe;
	u16 head, phase;
	nvmed_queue = HtoQ(nvmed_handle);
	nvmed = HtoD(nvmed_handle);

	pthread_spin_lock(&nvmed_queue->cq_lock);

	while(1) {
		head = nvmed_queue->cq_head;
		phase = nvmed_queue->cq_phase;
		/* 找到特定的iod */
		iod = nvmed_queue->iod_arr + target_id;
		/* 特定的io完成了,那么polling就退出 */
		if(iod->status == IO_COMPLETE) {
			break;
		}
		/* 从cq的head开始搜索 */
		cqe = (volatile struct nvme_completion *)&nvmed_queue->cqes[head];
		for (;;) {
			/* 如果cq是空的就一直循环.nvmed_queue_complete则是遇到队列是空就退出 */
			if((cqe->status & 1) == nvmed_queue->cq_phase)
				break;
		}

		/* head回环 */
		if(++head == nvmed->dev_info->q_depth) {
			head = 0;
			phase = !phase;
		}

		/* 有io返回,那么就处理iod array对应的iod */
		iod = nvmed_queue->iod_arr + cqe->command_id;
		nvmed_complete_iod(iod);

		COMPILER_BARRIER();
		/* 更新db */
		*(volatile u32 *)nvmed_queue->cq_db = head;
		nvmed_queue->cq_head = head;
		nvmed_queue->cq_phase = phase;
	}
	pthread_spin_unlock(&nvmed_queue->cq_lock);
}

/*
 * Create I/O handle
 */
/* 一个queue上可以有多个handle */
NVMED_HANDLE* nvmed_handle_create(NVMED_QUEUE* nvmed_queue, int flags) {
	NVMED_HANDLE* nvmed_handle;
	void* tempPtr;
	int i;

	pthread_spin_lock(&nvmed_queue->mngt_lock);

	nvmed_handle = calloc(1, sizeof(NVMED_HANDLE));
	nvmed_handle->queue = nvmed_queue;

	/* 如果队列不支持中断,那么handle也不支持 */
	if(__FLAG_ISSET(flags, HANDLE_INTERRUPT)) {
		if(!FLAG_ISSET(nvmed_queue, QUEUE_INTERRUPT))
			flags &= ~(HANDLE_INTERRUPT);
	}

	nvmed_handle->flags = flags;
	nvmed_handle->offset = 0;
	nvmed_handle->bufOffs = 0;

	nvmed_handle->dispatched_io = 0;

	/* PRP Buffer Create */
	/* 每个handle预分配64个prp page.每个page用来保存prp entry */
	nvmed_handle->prpBuf_size = NVMED_NUM_PREALLOC_PRP;
	nvmed_handle->prpBuf = calloc(1, sizeof(void *) * nvmed_handle->prpBuf_size);
	nvmed_handle->pa_prpBuf = calloc(1, sizeof(u64) * nvmed_handle->prpBuf_size);
	tempPtr = mmap(NULL, PAGE_SIZE * nvmed_handle->prpBuf_size, PROT_READ | PROT_WRITE,
			MAP_ANONYMOUS | MAP_LOCKED | MAP_SHARED, -1, 0);

	memset(tempPtr, 0, PAGE_SIZE * nvmed_handle->prpBuf_size);

	/* 虚拟地址转换成物理地址 */
	virt_to_phys(nvmed_queue->nvmed, tempPtr, nvmed_handle->pa_prpBuf,
			PAGE_SIZE * nvmed_handle->prpBuf_size);

	/* 获取prpBuf */
	for(i=0; i<nvmed_handle->prpBuf_size; i++) {
		nvmed_handle->prpBuf[i] = tempPtr + (PAGE_SIZE*i);
	}

	/* prpBuf_curr初始化可以使用全部的prp buffer */
	nvmed_handle->prpBuf_curr = nvmed_handle->prpBuf_size;
	nvmed_handle->prpBuf_head = 0;
	nvmed_handle->prpBuf_tail = 0;

	if(__FLAG_ISSET(flags, HANDLE_DIRECT_IO)) {
		nvmed_handle->read_func 	= nvmed_io_rw;
		nvmed_handle->write_func	= nvmed_io_rw;
	}
	else {
		nvmed_handle->read_func 	= nvmed_buffer_read;
		nvmed_handle->write_func 	= nvmed_buffer_write;
	}

	LIST_INIT(&nvmed_handle->dirty_list);
	pthread_spin_init(&nvmed_handle->dirty_list_lock, 0);

	nvmed_queue->numHandle++;
	LIST_INSERT_HEAD(&nvmed_queue->handle_head, nvmed_handle, handle_list);

	TAILQ_INIT(&nvmed_handle->io_head);
	nvmed_handle->num_io_head = 0;

	pthread_spin_init(&nvmed_handle->prpBuf_lock, 0);

	pthread_spin_init(&nvmed_handle->io_head_lock, 0);

	pthread_spin_unlock(&nvmed_queue->mngt_lock);

	return nvmed_handle;
}

/*
 * Destroy I/O handle
 */
int nvmed_handle_destroy(NVMED_HANDLE* nvmed_handle) {
	NVMED_QUEUE* nvmed_queue;

	if(nvmed_handle == NULL) return -NVMED_NOENTRY;
	nvmed_flush_handle(nvmed_handle);

	/* 等待dispatch的io完成 */
	while(nvmed_handle->dispatched_io) {}

	nvmed_queue = HtoQ(nvmed_handle);

	pthread_spin_lock(&nvmed_queue->mngt_lock);

	nvmed_queue->numHandle--;
	LIST_REMOVE(nvmed_handle, handle_list);

	pthread_spin_destroy(&nvmed_handle->prpBuf_lock);

	//munmap(nvmed_handle->prpBuf[0], PAGE_SIZE * nvmed_handle->prpBuf_size);
	free(nvmed_handle->prpBuf);
	free(nvmed_handle->pa_prpBuf);
	free(nvmed_handle);

	pthread_spin_unlock(&nvmed_queue->mngt_lock);

	return NVMED_SUCCESS;
}

/*
 * Create MQ Handle
 * (*func) should defined - callback function for pick I/O queue from MQ
 *							Argument - Handle, ops, offset, len
 */
NVMED_HANDLE* nvmed_handle_create_mq(NVMED_QUEUE** nvmed_queue, int num_mq, int flags,
		NVMED_QUEUE* (*func)(NVMED_HANDLE*, u8, unsigned long, unsigned int)) {
	NVMED_HANDLE* nvmed_handle;
	int i;

	if(func == NULL)
		return NULL;

	nvmed_handle = nvmed_handle_create(nvmed_queue[0], flags);
	if(nvmed_handle != NULL) {
		for(i=1; i<num_mq; i++) {
			pthread_spin_lock(&nvmed_queue[i]->mngt_lock);
			nvmed_queue[i]->numHandle++;
			pthread_spin_unlock(&nvmed_queue[i]->mngt_lock);
		}
		nvmed_handle->queue_mq = nvmed_queue;
		nvmed_handle->num_mq = num_mq;
		nvmed_handle->flags |= HANDLE_MQ;
		nvmed_handle->mq_get_queue = func;
	}
	else return NULL;

	return nvmed_handle;
}

/*
 * Destroy MQ Handle
 */
int nvmed_handle_destroy_mq(NVMED_HANDLE* nvmed_handle) {
	int i;

	if(nvmed_handle == NULL) return -NVMED_NOENTRY;
	if(!FLAG_ISSET(nvmed_handle, HANDLE_MQ)) {
		nvmed_printf("%s: failed to destroy MQ - not MQ\n",
				HtoD(nvmed_handle)->ns_path);

		return -NVMED_FAULT;
	}
	for(i=1; i<nvmed_handle->num_mq; i++) {
		pthread_spin_lock(&nvmed_handle->queue_mq[i]->mngt_lock);
		nvmed_handle->queue_mq[i]->numHandle--;
		pthread_spin_unlock(&nvmed_handle->queue_mq[i]->mngt_lock);
	}

	free(nvmed_handle->queue_mq);

	return nvmed_handle_destroy(nvmed_handle);
}

/*
 * Get Handle Features
 */
int nvmed_handle_feature_get(NVMED_HANDLE* nvmed_handle, int feature) {
	return FLAG_ISSET(nvmed_handle, feature);
}

/*
 * Set Handle Features
 */
int nvmed_handle_feature_set(NVMED_HANDLE* nvmed_handle, int feature, int value) {
	switch(feature) {
		case HANDLE_DIRECT_IO:
			if(value)
				FLAG_SET(nvmed_handle, HANDLE_DIRECT_IO);
			else
				FLAG_UNSET(nvmed_handle, HANDLE_DIRECT_IO);
			break;

		/* 同步io,在下发一个命令后,必须等待io完成 */
		case HANDLE_SYNC_IO:
			if(value)
				FLAG_SET(nvmed_handle, HANDLE_SYNC_IO);
			else
				FLAG_UNSET(nvmed_handle, HANDLE_SYNC_IO);
			break;

		case HANDLE_HINT_DMEM:
			if(value)
				FLAG_SET(nvmed_handle, HANDLE_HINT_DMEM);
			else
				FLAG_UNSET(nvmed_handle, HANDLE_HINT_DMEM);
			break;

		case HANDLE_INTERRUPT:
			if(!FLAG_ISSET(nvmed_handle->queue, QUEUE_INTERRUPT))
				return -NVMED_INVALID;

			if(value)
				FLAG_SET(nvmed_handle, HANDLE_INTERRUPT);
			else
				FLAG_UNSET(nvmed_handle, HANDLE_INTERRUPT);
			break;
	}

	return value;
}

/*
 * Process CQ Interrupt Handling
 */
void* nvmed_process_cq_intr(void *data) {
	NVMED_QUEUE* nvmed_queue = data;
	NVMED* nvmed = QtoD(nvmed_queue);
	unsigned long qid = nvmed_queue->qid;
	int ret;

	while(1) {
		/* 通过ioctl检查内核中对应queue的中断计数 */
		ret = ioctl(nvmed->ns_fd, NVMED_IOCTL_INTERRUPT_COMM, &qid);
		if(ret < 0) break;

		/* 发现收到中断,这里qid从内核获取的是中断次数 */
		if(qid != 0) {
			nvmed_queue_complete(nvmed_queue);
		}
	}

	pthread_exit((void *)NULL);
};

/*
 * Create User-space I/O queue and map to User virtual address
 */
NVMED_QUEUE* nvmed_queue_create(NVMED* nvmed, int flags) {
	int ret;
	NVMED_QUEUE* nvmed_queue;
	char pathBase[1024];
	char pathBuf[1024];
	u32 *q_dbs;
	NVMED_CREATE_QUEUE_ARGS create_args;

	pthread_spin_lock(&nvmed->mngt_lock);

	memset(&create_args, 0x0, sizeof(create_args));

	if(__FLAG_ISSET(flags, QUEUE_INTERRUPT))
		create_args.reqInterrupt = NVMED_TRUE;

	/* Request Create I/O Queues */
	ret = ioctl(nvmed->ns_fd, NVMED_IOCTL_QUEUE_CREATE, &create_args);
	if(ret < 0) {
		nvmed_printf("%s: fail to create I/O queue\n", nvmed->ns_path);

		return NULL;
	}

	nvmed_queue = calloc(1, sizeof(NVMED_QUEUE));
	nvmed_queue->nvmed = nvmed;
	nvmed_queue->flags = flags;
	nvmed_queue->qid = create_args.qid;

	if(nvmed->dev_info->part_no != 0) {
		sprintf(pathBase, "/proc/nvmed/nvme%dn%dp%d/%d",
			nvmed->dev_info->instance, nvmed->dev_info->ns_id, nvmed->dev_info->part_no, nvmed_queue->qid);
	}
	else {
		sprintf(pathBase, "/proc/nvmed/nvme%dn%d/%d",
			nvmed->dev_info->instance, nvmed->dev_info->ns_id, nvmed_queue->qid);
	}

	/* Map SQ */
	/* 在用户态获取sq/cq/db的地址 */
	sprintf(pathBuf, "%s/sq", pathBase);
	nvmed_queue->sq_fd = open(pathBuf, O_RDWR);
	nvmed_queue->sq_cmds = mmap(0, SQ_SIZE(nvmed->dev_info->q_depth),
			PROT_READ | PROT_WRITE, MAP_SHARED, nvmed_queue->sq_fd, 0);

	/* Map CQ */
	sprintf(pathBuf, "%s/cq", pathBase);
	nvmed_queue->cq_fd = open(pathBuf, O_RDWR);
	nvmed_queue->cqes = mmap(0, CQ_SIZE(nvmed->dev_info->q_depth),
			PROT_READ | PROT_WRITE, MAP_SHARED, nvmed_queue->cq_fd, 0);

	/* Map DQ */
	sprintf(pathBuf, "%s/db", pathBase);
	nvmed_queue->db_fd = open(pathBuf, O_RDWR);
	nvmed_queue->dbs = mmap(0, PAGE_SIZE*2, PROT_READ | PROT_WRITE, MAP_SHARED, nvmed_queue->db_fd, 0);

	/* db寄存器在BAR空间的4k位置 */
	q_dbs = nvmed_queue->dbs + PAGE_SIZE;
	/* 每一个qid都有一个cq和sq */
	nvmed_queue->sq_db = &q_dbs[nvmed_queue->qid * 2 * nvmed->dev_info->db_stride];
	nvmed_queue->cq_db = &q_dbs[(nvmed_queue->qid * 2 * nvmed->dev_info->db_stride) + nvmed->dev_info->db_stride];
	nvmed_queue->sq_head = 0;
	nvmed_queue->sq_tail = 0;
	nvmed_queue->cq_head = 0;
	/* phase初始化为1,因为controller在第一轮写cq phase是1. */
	nvmed_queue->cq_phase = 1;

	/* 提前分配，用来记录每一个io的状态 */
	nvmed_queue->iod_arr = calloc(nvmed->dev_info->q_depth, sizeof(NVMED_IOD));
	nvmed_queue->iod_pos = 0;

	pthread_spin_init(&nvmed_queue->iod_arr_lock, 0);
	pthread_spin_init(&nvmed_queue->mngt_lock, 0);
	pthread_spin_init(&nvmed_queue->sq_lock, 0);
	pthread_spin_init(&nvmed_queue->cq_lock, 0);

	LIST_INSERT_HEAD(&nvmed->queue_head, nvmed_queue, queue_list);
	nvmed->numQueue++;

	nvmed_queue->numHandle = 0;

	/* 用一个单独的线程去处理内核中断 */
	if(__FLAG_ISSET(flags, QUEUE_INTERRUPT)) {
		pthread_create(&nvmed_queue->process_cq_intr, NULL, &nvmed_process_cq_intr, (void*)nvmed_queue);
	}

	pthread_spin_unlock(&nvmed->mngt_lock);

	return nvmed_queue;
}

/*
 * Destroy User-space I/O queue
 */
int nvmed_queue_destroy(NVMED_QUEUE* nvmed_queue) {
	NVMED* nvmed;
	void *status;
	int ret;

	if(nvmed_queue == NULL) return -NVMED_NOENTRY;
	if(nvmed_queue->numHandle) return -NVMED_FAULT;

	nvmed = nvmed_queue->nvmed;

	pthread_spin_lock(&nvmed->mngt_lock);
	/* 需要移除一个queue,确保cq处理线程suspend */
	if(nvmed->process_cq_status != TD_STATUS_STOP) {
		while(nvmed->process_cq_status == TD_STATUS_SUSPEND ||
				nvmed->process_cq_status == TD_STATUS_REQ_SUSPEND);

		if(nvmed->process_cq_status == TD_STATUS_RUNNING) {
			nvmed->process_cq_status = TD_STATUS_REQ_SUSPEND;
			while(nvmed->process_cq_status == TD_STATUS_REQ_SUSPEND);
		}
	}

	pthread_spin_lock(&nvmed_queue->mngt_lock);

	munmap(nvmed_queue->dbs, PAGE_SIZE * 2);
	close(nvmed_queue->db_fd);

	munmap((void *)nvmed_queue->cqes, CQ_SIZE(nvmed->dev_info->q_depth));
	close(nvmed_queue->cq_fd);

	munmap(nvmed_queue->sq_cmds, SQ_SIZE(nvmed->dev_info->q_depth));
	close(nvmed_queue->sq_fd);

	pthread_spin_destroy(&nvmed_queue->sq_lock);
	pthread_spin_destroy(&nvmed_queue->cq_lock);

	ret = ioctl(nvmed->ns_fd, NVMED_IOCTL_QUEUE_DELETE, &nvmed_queue->qid);
	if(ret==0) {
		LIST_REMOVE(nvmed_queue, queue_list);
		pthread_mutex_lock(&nvmed->process_cq_mutex);
		pthread_cond_signal(&nvmed->process_cq_cond);
		pthread_mutex_unlock(&nvmed->process_cq_mutex);

		nvmed->numQueue--;
	}

	free(nvmed_queue->iod_arr);
	pthread_join(nvmed_queue->process_cq_intr, &status);
	pthread_spin_unlock(&nvmed_queue->mngt_lock);
	pthread_spin_destroy(&nvmed_queue->mngt_lock);
	free(nvmed_queue);

	pthread_spin_unlock(&nvmed->mngt_lock);

	return NVMED_SUCCESS;
}

/*
 * Process CQ Thread
 */
void* nvmed_process_cq(void *data) {
	NVMED* nvmed = data;
	NVMED_QUEUE* nvmed_queue;

	while(1) {
		/* 请求suspend时 */
		if(nvmed->process_cq_status == TD_STATUS_REQ_SUSPEND) {
			pthread_mutex_lock(&nvmed->process_cq_mutex);
			nvmed->process_cq_status = TD_STATUS_SUSPEND;
			/* 睡眠,等待pthread_cond_signal唤醒 */
			pthread_cond_wait(&nvmed->process_cq_cond, &nvmed->process_cq_mutex);
			pthread_mutex_unlock(&nvmed->process_cq_mutex);
			nvmed->process_cq_status = TD_STATUS_RUNNING;
		}

		/* 请求cq处理线程退出 */
		if(nvmed->process_cq_status == TD_STATUS_REQ_STOP) {
			break;
		}
		/* 运行状态,处理nvmed上每一个queue */
		for (nvmed_queue = nvmed->queue_head.lh_first;
				nvmed_queue != NULL; nvmed_queue = nvmed_queue->queue_list.le_next) {
			/* 中断开启的线程或者自己处理cqe的线程,自己处理cq */
			if(FLAG_ISSET(nvmed_queue, QUEUE_MANUAL_CQ))
				continue;

			pthread_spin_lock(&nvmed_queue->mngt_lock);
			/* 处理单个queue上所有的cqe */
			nvmed_queue_complete(nvmed_queue);
			pthread_spin_unlock(&nvmed_queue->mngt_lock);
		}
	};

	nvmed->process_cq_status = TD_STATUS_STOP;

	pthread_exit((void *)NULL);
}

/*
 * Translate /dev/nvmeXnY -> /proc/nvmed/nvmeXnY/admin
 *							or /proc/nvmed/nvmeXnYpZ/admin
 */
int get_path_from_blkdev(char* blkdev, char** admin_path) {
	char temp_path[16];
	char *proc_path;
	int path_len;

	/* 跳过/dev/nvme */
	strcpy(temp_path, blkdev+9);
	/* /proc/nvmed/nvme%s/admin中固定部分的长度 */
	path_len = 23;
	path_len+= strlen(temp_path);

	/* admin完整路径 */
	proc_path = malloc(sizeof(char) * path_len);
	sprintf(proc_path, "/proc/nvmed/nvme%s/admin", temp_path);

	if(access(proc_path, F_OK) < 0) {
		free(proc_path);
		return -NVMED_NOENTRY;
	}

	*admin_path = proc_path;
	return NVMED_SUCCESS;
}

/*
 * Open NVMe device
 */
NVMED* nvmed_open(char* path, int flags) {
	char* admin_path;
	int result;
	NVMED_DEVICE_INFO *dev_info;
	NVMED* nvmed;
	int fd;
	int ret;
	unsigned int num_cache;
	int idx;

	result = get_path_from_blkdev(path, &admin_path);
	if(result < 0) {
		nvmed_printf("%s: fail to open nvme device file\n", path);
		return NULL;
	}
	/* 打开admin节点 */
	fd = open(admin_path, 0);
	if(fd < 0) {
		nvmed_printf("%s: fail to open nvme device file\n", admin_path);
		return NULL;
	}

	//IOCTL - Get Device Info
	/* 1.获取nvme的device info */
	dev_info = calloc(1, sizeof(*dev_info));
	ret = ioctl(fd, NVMED_IOCTL_NVMED_INFO, dev_info);
	if(ret<0) {
		close(fd);

		return NULL;
	}

	/* 如果硬件一次性可以处理超过2MB的io.也会设置最大单个io大小2MB */
	if(dev_info->max_hw_sectors > 4096) dev_info->max_hw_sectors = 4096;

	nvmed = calloc(1, sizeof(*nvmed));
	if(nvmed==NULL) {
		free(admin_path);
		free(dev_info);
		close(fd);

		nvmed_printf("%s: fail to allocation nvmed buffer\n", admin_path);

		return NULL;
	}

	// Getting NVMe Device Info
	nvmed->ns_path = admin_path;
	nvmed->ns_fd = fd;
	nvmed->dev_info = dev_info;
	nvmed->flags = flags;

	// QUEUE
	nvmed->numQueue = 0;
	LIST_INIT(&nvmed->queue_head);

	pthread_spin_init(&nvmed->mngt_lock, 0);

	// PROCESS_CQ THREAD
	/* 初始化时cq thread状态是TD_STATUS_STOP的 */
	nvmed->process_cq_status = TD_STATUS_STOP;

	pthread_cond_init(&nvmed->process_cq_cond, NULL);
	pthread_mutex_init(&nvmed->process_cq_mutex, NULL);

	if(!__FLAG_ISSET(flags, NVMED_NO_CACHE)) {
		// CACHE
		/* 如果使用了cache.最少使用4MB个page作为cache */
		if((nvmed->dev_info->max_hw_sectors * 512) / PAGE_SIZE > NVMED_CACHE_INIT_NUM_PAGES)
			num_cache = (nvmed->dev_info->max_hw_sectors * 512) / PAGE_SIZE;
		else
			num_cache = NVMED_CACHE_INIT_NUM_PAGES;

		nvmed->num_cache_usage = 0;

		TAILQ_INIT(&nvmed->lru_head);
		TAILQ_INIT(&nvmed->free_head);
		pthread_rwlock_init(&nvmed->cache_radix_lock, 0);
		pthread_spin_init(&nvmed->cache_list_lock, 0);

		// CACHE - INIT
		LIST_INIT(&nvmed->slot_head);
		/* 每次作为一个slot,申请256 * 100个cache,共100MB一次 */
		for(idx=0; idx<=num_cache; idx+=(256 * 100))
			nvmed_cache_alloc(nvmed, idx,
				__FLAG_ISSET(flags, NVMED_CACHE_LAZY_INIT));

		INIT_RADIX_TREE(&nvmed->cache_root);
		radix_tree_init();
	}

	return nvmed;
}

/*
 * Close NVMe device
 */
int nvmed_close(NVMED* nvmed) {
	NVMED_CACHE_SLOT* slot;
	void *status;

	if(nvmed==NULL || nvmed->ns_fd==0) return -NVMED_NOENTRY;
	if(nvmed->numQueue) return -NVMED_FAULT;

	close(nvmed->ns_fd);

	free(nvmed->ns_path);
	free(nvmed->dev_info);

	//END PROCESS_CQ THREAD
	if(nvmed->process_cq_status != TD_STATUS_STOP) {
		if(nvmed->process_cq_status == TD_STATUS_RUNNING) {
			/* 对于正在running的cq处理线程,先要让它suspend,才能发送stop命令 */
			nvmed->process_cq_status = TD_STATUS_REQ_SUSPEND;
		}

		/* 等待cq处理线程suspend */
		while(nvmed->process_cq_status == TD_STATUS_REQ_SUSPEND);

		if(nvmed->process_cq_status == TD_STATUS_SUSPEND) {
			pthread_mutex_lock(&nvmed->process_cq_mutex);
			pthread_cond_signal(&nvmed->process_cq_cond);
			pthread_mutex_unlock(&nvmed->process_cq_mutex);
		}

		while(nvmed->process_cq_status != TD_STATUS_RUNNING);
		/* nvmed关闭的时候,cq处理线程退出 */
		nvmed->process_cq_status = TD_STATUS_REQ_STOP;
		pthread_join(nvmed->process_cq_td, (void **)&status);
	}
	//CACHE REMOVE
	while (nvmed->slot_head.lh_first != NULL) {
		slot = nvmed->slot_head.lh_first;
		free(slot->cache_info);
		munmap(slot->cache_ptr, PAGE_SIZE * slot->size);
		LIST_REMOVE(slot, slot_list);
		free(slot);
	}

	pthread_rwlock_destroy(&nvmed->cache_radix_lock);
	pthread_spin_destroy(&nvmed->cache_list_lock);
	pthread_mutex_destroy(&nvmed->process_cq_mutex);
	pthread_cond_destroy(&nvmed->process_cq_cond);
	pthread_spin_destroy(&nvmed->mngt_lock);

	free(nvmed);

	return NVMED_SUCCESS;
}

/*
 * Get NVMeD (NVMeDirect wide) features
 */
int nvmed_feature_get(NVMED* nvmed, int feature) {
	switch(feature) {
		case NVMED_CACHE_LAZY_INIT:
			return FLAG_ISSET(nvmed, NVMED_CACHE_LAZY_INIT);
			break;
		case NVMED_CACHE_SIZE:
			return nvmed->num_cache_size;
	};

	return 0;
}

/*
 * Set NVMeD (NVMeDirect wide) features
 */
int nvmed_feature_set(NVMED* nvmed, int feature, int value) {
	switch(feature) {
		case NVMED_CACHE_LAZY_INIT:
			FLAG_SET(nvmed, NVMED_CACHE_LAZY_INIT);
			break;
		case NVMED_CACHE_SIZE:
			return nvmed_cache_alloc(nvmed, value,
					nvmed_feature_get(nvmed, NVMED_CACHE_LAZY_INIT));
	};

	return value;
}

/*
 * Send I/O to submission queue and ring SQ Doorbell
 */
ssize_t nvmed_io(NVMED_HANDLE* nvmed_handle, u8 opcode,
		u64 prp1, u64 prp2, void* prp2_addr, NVMED_CACHE *__cache,
		unsigned long start_lba, unsigned int len, int flags, NVMED_AIO_CTX* context) {
	NVMED_QUEUE* nvmed_queue;
	NVMED* nvmed;
	struct nvme_command *cmnd;
	NVMED_IOD* iod;
	u16	target_id;
	NVMED_CACHE *cache = NULL;
	int i, num_cache;

	nvmed_queue = HtoQ(nvmed_handle);

	nvmed = HtoD(nvmed_handle);

	pthread_spin_lock(&nvmed_queue->sq_lock);

	/* 获取不到iod就一直循环 */
	while(1) {
		target_id = nvmed_queue->iod_pos++;
		iod = nvmed_queue->iod_arr + target_id;
		if(nvmed_queue->iod_pos == nvmed->dev_info->q_depth)
			nvmed_queue->iod_pos = 0;
		/* 找到一个未出化的iod */
		if(iod->status != IO_INIT)
			break;
	}
	iod->sq_id = nvmed_queue->sq_tail;
	/* 用于在nvmed_complete_iod中释放prp page */
	iod->prp_addr = prp2_addr;
	iod->prp_pa = prp2;
	/* 发起io */
	iod->status = IO_INIT;
	iod->num_cache = 0;
	iod->cache = NULL;
	iod->nvmed_handle = nvmed_handle;
	iod->context = context;
	if(iod->context!=NULL) {
		iod->context->num_init_io++;
		iod->context->status = AIO_PROCESS;
	}

	if(FLAG_ISSET(nvmed_handle, HANDLE_INTERRUPT)) {
		iod->intr_status = IOD_INTR_INIT;
		pthread_mutex_init(&iod->intr_cq_mutex, NULL);
	}
	else {
		iod->intr_status = IOD_INTR_INACTIVE;
	}

	/* 将cache关联到iod,用于在io结束的时候更新cache状态 */
	if(__cache != NULL) {
		pthread_spin_lock(&nvmed_queue->iod_arr_lock);
		/* cache的个数 */
		num_cache = len / PAGE_SIZE;
		cache = __cache;
		iod->cache = calloc(num_cache, sizeof(NVMED_CACHE*));
		for(i=0; i<num_cache; i++) {
			/* iod获取cache */
			iod->cache[i] = cache;
			cache = cache->io_list.tqe_next;
		}
		iod->num_cache = num_cache;
		pthread_spin_unlock(&nvmed_queue->iod_arr_lock);
	}

	/* 从sq_tail中取出cmnd,更行这个cmnd,然后写doorbell */
	cmnd = &nvmed_queue->sq_cmds[nvmed_queue->sq_tail];
	memset(cmnd, 0, sizeof(*cmnd));

	//remap start_lba
	start_lba += nvmed->dev_info->start_sect;

	switch(opcode) {
		case nvme_cmd_flush:
			cmnd->rw.opcode = nvme_cmd_flush;
			cmnd->rw.command_id = target_id;
			cmnd->rw.nsid = nvmed->dev_info->ns_id;

			break;

		case nvme_cmd_write:
		case nvme_cmd_read:
			cmnd->rw.opcode = opcode;
			cmnd->rw.command_id = target_id;
			cmnd->rw.nsid = nvmed->dev_info->ns_id;
			cmnd->rw.prp1 = prp1;
			cmnd->rw.prp2 = prp2;
			cmnd->rw.slba = start_lba >> nvmed->dev_info->lba_shift;
			cmnd->rw.length = (len >> nvmed->dev_info->lba_shift) - 1;
			cmnd->rw.control = 0;
			cmnd->rw.dsmgmt = 0;

			break;

		case nvme_cmd_dsm:
			cmnd->dsm.opcode = nvme_cmd_dsm;
			cmnd->dsm.command_id = target_id;
			cmnd->dsm.nsid = nvmed->dev_info->ns_id;
			cmnd->dsm.prp1 = prp1;
			cmnd->dsm.prp2 = 0;
			cmnd->dsm.nr = 0;
			/* trim */
			cmnd->dsm.attributes = NVME_DSMGMT_AD;

			break;
	}

	if(++nvmed_queue->sq_tail == nvmed->dev_info->q_depth)
		nvmed_queue->sq_tail = 0;

	COMPILER_BARRIER();
	*(volatile u32 *)nvmed_queue->sq_db = nvmed_queue->sq_tail;
	/* gcc内置的原子操作函数,给dispatched_io加1,返回之前的dispatched_io */
	__sync_fetch_and_add(&nvmed_handle->dispatched_io, 1);
	//nvmed_handle->dispatched_io++;

	pthread_spin_unlock(&nvmed_queue->sq_lock);

	/* If Sync I/O => Polling */
	if(__FLAG_ISSET(flags, HANDLE_SYNC_IO)) {
		if(iod->intr_status == IOD_INTR_INACTIVE) {
			/* 使用polling的方式等待特定io完成 */
			nvmed_io_polling(nvmed_handle, target_id);
		}
		else {
			pthread_mutex_lock(&iod->intr_cq_mutex);
			iod->intr_status = IOD_INTR_WAITING;
			/* 等待中断唤醒.中断监控线程会调用nvmed_complete_iod唤醒当前线程 */
			pthread_cond_wait(&iod->intr_cq_cond, &iod->intr_cq_mutex);
			pthread_mutex_unlock(&iod->intr_cq_mutex);

			pthread_mutex_destroy(&iod->intr_cq_mutex);
			pthread_cond_destroy(&iod->intr_cq_cond);
		}
	}

	return len;
}

/* 将handle上io_head的cache全部写入,释放cache */
unsigned int __evict_handle_dirty_page(NVMED_HANDLE* handle) {
	unsigned long io_start_lba;
	unsigned int io_len;

	io_start_lba = TAILQ_FIRST(&handle->io_head)->lpaddr;
	io_len = TAILQ_LAST(&handle->io_head, io_list)->lpaddr;
	io_len-= io_start_lba;
	io_len+= 1;

	/* 将cache全部写入设备 */
	nvmed_cache_io_rw(handle, nvme_cmd_write, \
			handle->io_head.tqh_first, \
			io_start_lba * PAGE_SIZE, io_len * PAGE_SIZE, handle->flags | HANDLE_SYNC_IO);
	/* 清空io_head上的cache.在io完成以后会清除dirty标志 */
	while (handle->io_head.tqh_first != NULL)
		TAILQ_REMOVE(&handle->io_head, handle->io_head.tqh_first, io_list);

	//		TAILQ_INIT(&handle->io_head);

	handle->num_io_head = 0;

	return io_len;
}

/* Get CACHE from free list or evict */
NVMED_CACHE* nvmed_get_cache(NVMED_HANDLE* nvmed_handle) {
	NVMED* nvmed = HtoD(nvmed_handle);
	NVMED_HANDLE* handle;
	NVMED_CACHE *cache;
	NVMED_CACHE *ret_cache;
	TAILQ_HEAD(cache_list, nvmed_cache) temp_head;

	pthread_rwlock_wrlock(&nvmed->cache_radix_lock);
	pthread_spin_lock(&nvmed->cache_list_lock);

/* 获取不到cache, 就会一直goto restart */
restart:
	cache = nvmed->free_head.tqh_first;
	if(cache==NULL)  {
		/* 如果free_head上没有cache,就从lru头部上拿 */
		//HEAD -> LRU, //TAIL -> MRU
		//EVICT - LRU
		cache = nvmed->lru_head.tqh_first;
		/* 干净的cache才能被使用 */
		if(!FLAG_ISSET(cache, CACHE_DIRTY)) {
			TAILQ_REMOVE(&nvmed->lru_head, cache, cache_list);
			radix_tree_delete(&nvmed->cache_root, cache->lpaddr);
			FLAG_SET_FORCE(cache, 0);
			ret_cache = cache;
		}
		else {
			//////////// handle io_list evict!!
			TAILQ_INIT(&temp_head);

			/* 如果lru head上取出的cache有被锁或者读操作命中率较高,那么就sleep,重新扫描 */
			while(FLAG_ISSET_SYNC(cache, CACHE_LOCKED) || cache->ref != 0) {
				pthread_spin_unlock(&nvmed->cache_list_lock);
				pthread_rwlock_unlock(&nvmed->cache_radix_lock);
				usleep(1);
				pthread_rwlock_wrlock(&nvmed->cache_radix_lock);
				pthread_spin_lock(&nvmed->cache_list_lock);
				goto restart;
			}

			pthread_spin_unlock(&nvmed->cache_list_lock);
			pthread_rwlock_unlock(&nvmed->cache_radix_lock);

			handle = cache->handle;
			pthread_spin_lock(&handle->io_head_lock);

			if(!FLAG_ISSET(cache, CACHE_DIRTY)) {
				pthread_rwlock_wrlock(&nvmed->cache_radix_lock);
				pthread_spin_lock(&nvmed->cache_list_lock);
				goto restart;
			}

			/*
			 * 将脏cache全部写入设备.io_head上所有的io在完成后会清除dirty标志.
			 * 之后在lru上就能获取到可用的cache了.
			 */
			__evict_handle_dirty_page(handle);

			pthread_spin_unlock(&handle->io_head_lock);

			pthread_rwlock_wrlock(&nvmed->cache_radix_lock);
			pthread_spin_lock(&nvmed->cache_list_lock);

			goto restart;
		}
	} else {
		// Remove From Free Queue
		/* 从freelist上取出这个free的cache */
		TAILQ_REMOVE(&nvmed->free_head, cache, cache_list);
		FLAG_UNSET_SYNC(cache, CACHE_FREE);
		if(FLAG_ISSET(cache, CACHE_UNINIT)) {
			/* lazy init的cache在从freelist上取出cache的时候才做内存映射 */
			memset(cache->ptr, 0, PAGE_SIZE);
			virt_to_phys(nvmed, cache->ptr, &cache->paddr, 4096);
			FLAG_UNSET_SYNC(cache, CACHE_UNINIT);
		}
		ret_cache = cache;
	}

	/* 初始化为0 */
	INIT_SYNC(ret_cache->ref);
	pthread_spin_unlock(&nvmed->cache_list_lock);
	pthread_rwlock_unlock(&nvmed->cache_radix_lock);
	//fprintf(stderr, "%s: %p\n", __func__, ret_cache);
	return ret_cache;
}

/* buffer format:  [User Buf(4KB * num_pages)][MAGIC(4Bytes)][Num Pages(4Bytes)][PA LIST u64 * num_pages] */
unsigned int nvmed_check_buffer(void* nvmed_buf) {
	size_t buf_size;
	unsigned int predict_size, actual_size=0;
	unsigned int *magic;

	if(nvmed_buf == NULL) return 0;

	buf_size = malloc_usable_size(nvmed_buf);
	if(buf_size < PAGE_SIZE) return 0;

	predict_size = buf_size / PAGE_SIZE;
	if(predict_size >= 512)
		predict_size -= predict_size >> 9;

	magic = nvmed_buf + (PAGE_SIZE * predict_size);
	if(*magic == NVMED_BUF_MAGIC) {
		actual_size = *(++magic);
	}
	else {
		predict_size--;
		magic = nvmed_buf + (PAGE_SIZE * predict_size);
		if(*magic == NVMED_BUF_MAGIC) {
			actual_size = *(++magic);
		}
	}

	return actual_size;
}

/*
 *  Buffer Format
 *  [User Buf(4KB * num_pages)][MAGIC(4Bytes)][Num Pages(4Bytes)][PA LIST u64 * num_pages]
 */
void* nvmed_get_buffer(NVMED* nvmed, unsigned int num_pages) {
	struct nvmed_buf nvmed_buf = {0};
	void *bufAddr;
	int ret;
	unsigned int *magic, *size;
	/* magic和num pages各占4 bytes */
	int req_size = (PAGE_SIZE * num_pages) + (sizeof(u64) * num_pages) + 8;

	if(num_pages == 0) return NULL;
	/* 分配内存并且mlock住 */
	posix_memalign(&bufAddr, PAGE_SIZE, req_size);
	if(bufAddr == NULL) return NULL;
	mlock(bufAddr, PAGE_SIZE * req_size);

	memset(bufAddr, 0, req_size);

	nvmed_buf.addr = bufAddr;
	nvmed_buf.size = num_pages;
	nvmed_buf.pfnList = bufAddr + (PAGE_SIZE * num_pages) + 8;

	ret = ioctl(nvmed->ns_fd, NVMED_IOCTL_GET_BUFFER_ADDR, &nvmed_buf);

	if(ret < 0) {
		free(bufAddr);
		return NULL;
	}

	magic = bufAddr + (PAGE_SIZE * num_pages);
	*magic = NVMED_BUF_MAGIC;
	size = bufAddr + (PAGE_SIZE * num_pages)+4;
	*size = num_pages;
	/* 返回nvme_buf */
	return bufAddr;
}

void nvmed_put_buffer(void* nvmed_buf) {
	int buf_size, buf_len;
	buf_size = nvmed_check_buffer(nvmed_buf);
	buf_len = *(int *)(nvmed_buf + (PAGE_SIZE * buf_size) + 4);
	buf_len = (PAGE_SIZE * buf_len) + (sizeof(u64) * buf_len) + 8;

	munlock(nvmed_buf, buf_len);

	free(nvmed_buf);

	return;
}

/*
 * Make PRP List for Multiple page I/O from user buffer
 */
int make_prp_list(NVMED_HANDLE* nvmed_handle, void* buf,
		unsigned long lba_offs, unsigned int io_size, u64* __paBase,
		u64* prp1, u64* prp2, void** prp2_addr) {
	/* 对应哪个buffer,一个buffer一个page大小.startBufPos是pa list中entry的下标off */
	unsigned int startBufPos = lba_offs / PAGE_SIZE;
	unsigned int numBuf = io_size / PAGE_SIZE;
	unsigned int i;
	u64 *prpTmp;
	u64 *prpBuf;

	u64 *paBase = __paBase;
	u64 __prp1, __prp2;

	u64* paList;
	unsigned int bufOffs;

	*prp2_addr = NULL;

	if(io_size % PAGE_SIZE > 0) numBuf ++;

	paList = malloc(sizeof(u64) * numBuf);

	if(paBase == NULL) {
		/* 不是通过nvmed_get_buffer的buffer,需要调用virt_to_phys获取物理地址 */
		numBuf = virt_to_phys(HtoD(nvmed_handle), buf, paList, numBuf * PAGE_SIZE);
		bufOffs = (unsigned long)buf % PAGE_SIZE;
		/* __prp1起始地址,物理地址 */
		__prp1 = paList[0] + bufOffs;
		if(numBuf == 1) {
			/* 只需要一个buf的话,prp2就是0 */
			__prp2 = 0;
		}
		else if(numBuf == 2) {
			/* 需要2个buffer,__prp2可以直接指向第二个物理地址 */
			__prp2 = paList[1];
		}
		else {
			/* 超过2个page,需要构造一个prp page. __prp2指向这个prp page */
			prpBuf = nvmed_handle_get_prp(nvmed_handle, &__prp2);
			*prp2_addr = prpBuf;
			/* 填充这个prp page. 这个page中每一个entry记录一个64 bit的物理地址 */
			for(i = 1; i < numBuf; i++) {
				prpBuf[i-1] = paList[i];
			}
		}
	}
	else {
		/* 通过nvmed_get_buffer分配的buffer,已经填充了pa list. */
		paBase += startBufPos;
		prpTmp = paBase;
		__prp1 = *prpTmp;
		if(numBuf == 1) {
			__prp2 = 0;
		}
		else if(numBuf == 2) {
			__prp2 = *(prpTmp+1);
		}
		else {
			/* 获取prp page */
			prpBuf = nvmed_handle_get_prp(nvmed_handle, &__prp2);
			*prp2_addr = prpBuf;
			/* prp entry指向buffer的物理地址 */
			for(i = 1; i < numBuf; i++) {
				prpBuf[i-1] = paBase[i];
			}
		}

	}

	free(paList);

	*prp1 = __prp1;
	*prp2 = __prp2;

	return 0;
}

/*
 * Make PRP List for Multiple page I/O from NVMeDirect Cache
 */
int make_prp_list_from_cache(NVMED_HANDLE* nvmed_handle, NVMED_CACHE *__cache,
		int num_list, u64* prp1, u64* prp2, void** prp2_addr) {
	NVMED_CACHE *cache;
	u64 *prpBuf;
	u64 __prp1, __prp2 = 0;
	int i;

	*prp2_addr = NULL;

	cache = __cache;
	/* cache在分配的时候就已经确定了ptr的物理内存地址 */
	__prp1 = cache->paddr;
	if(num_list == 2) {
		cache = cache->io_list.tqe_next;
		/* 下一个cache的物理地址 */
		__prp2 = cache->paddr;
	}
	else {
		/* prp page */
		prpBuf = nvmed_handle_get_prp(nvmed_handle, &__prp2);
		*prp2_addr = prpBuf;
		for(i=1; i<num_list; i++) {
			cache = cache->io_list.tqe_next;
			prpBuf[i-1] = cache->paddr;
		}
	}

	*prp1 = __prp1;
	*prp2 = __prp2;

	return 0;
}

/*
 * Make I/O request from NVMeDirect Cache
 */
ssize_t nvmed_cache_io_rw(NVMED_HANDLE* nvmed_handle, u8 opcode, NVMED_CACHE *__cache,
		unsigned long start_lba, unsigned int len, int __flag) {
	NVMED_QUEUE* nvmed_queue;
	NVMED* nvmed;
	NVMED_CACHE* cache;
	int num_cache;
	int flag;
	ssize_t remain = 0;
	ssize_t io_size, io = 0, total_io = 0;
	unsigned long io_lba;

	u64 prp1, prp2;
	void* prp2_addr;

	if(len % PAGE_SIZE) return 0;

	if(__flag != 0)
		flag = __flag;
	else
		flag = nvmed_handle->flags;

	if(FLAG_ISSET(nvmed_handle, HANDLE_MQ)) {
		nvmed_queue = nvmed_handle->mq_get_queue(nvmed_handle, opcode,
				start_lba, len);

		if(nvmed_queue == NULL)
			return 0;
	}
	else {
		nvmed_queue = HtoQ(nvmed_handle);
	}
	nvmed = nvmed_queue->nvmed;

	/*
	 * nvmed处理cq的线程初始化的时候是TD_STATUS_STOP的.
	 * 一旦有cached io,那么就会启动异步处理cq的线程
	 */
	if(nvmed->process_cq_status == TD_STATUS_STOP) {
		nvmed->process_cq_status = TD_STATUS_REQ_SUSPEND;
		pthread_create(&nvmed->process_cq_td, NULL, &nvmed_process_cq, (void*)nvmed);

		while(nvmed->process_cq_status != TD_STATUS_SUSPEND);

		pthread_mutex_lock(&nvmed->process_cq_mutex);
		pthread_cond_signal(&nvmed->process_cq_cond);
		pthread_mutex_unlock(&nvmed->process_cq_mutex);
	}

	remain = len;
	cache = __cache;

	num_cache = len / PAGE_SIZE;
	/* 锁住所有要下发的cache */
	while(num_cache-- > 0) {
		FLAG_SET_SYNC(cache, CACHE_LOCKED);
		cache = cache->io_list.tqe_next;
	}

	cache = __cache;
	while(remain > 0) {
		if(remain > nvmed->dev_info->max_hw_sectors * 512 )
			io_size = nvmed->dev_info->max_hw_sectors * 512;
		else
			io_size = remain;

		io_lba = total_io + start_lba;

		num_cache = io_size / PAGE_SIZE;

		make_prp_list_from_cache(nvmed_handle, cache, num_cache, &prp1, &prp2, &prp2_addr);
		io = nvmed_io(nvmed_handle, opcode, prp1, prp2, prp2_addr, cache,
				io_lba, io_size, flag, NULL);

		if(io <= 0) break;

		remain -= io;
		total_io += io;
		io_lba += io;

		/* 跳过num_cache个cache */
		while(num_cache-- > 0)
			cache = cache->io_list.tqe_next;
	}

	return total_io;
}

NVMED_BOOL nvmed_rw_verify_area(NVMED_HANDLE* nvmed_handle,
		unsigned long __start_lba, unsigned int len) {
	NVMED *nvmed = HtoD(nvmed_handle);
	NVMED_DEVICE_INFO *dev_info = nvmed->dev_info;
	unsigned long nr_sects = dev_info->nr_sects << dev_info->lba_shift;
	unsigned long start_lba = nvmed->dev_info->start_sect + __start_lba;

	/* lba溢出 */
	if(start_lba < dev_info->start_sect)
		return NVMED_FALSE;

	if((dev_info->start_sect + nr_sects) < start_lba)
		return NVMED_FALSE;

	if((dev_info->start_sect + nr_sects) < (start_lba + len))
		return NVMED_FALSE;

	return NVMED_TRUE;
}

/*
 * Make I/O request from User memory
 */
/* nvme_write/nvme_pwrite private都是NULL */
ssize_t nvmed_io_rw(NVMED_HANDLE* nvmed_handle, u8 opcode, void* buf,
		unsigned long start_lba, unsigned int len, NVMED_BOOL pio, void* private) {
	NVMED_AIO_CTX* context = private;
	NVMED_QUEUE* nvmed_queue;
	NVMED* nvmed;

	unsigned long io_lba;
	unsigned int io_size;
	ssize_t remain = 0;
	ssize_t io = 0, total_io = 0;
	unsigned int nvmed_buf_size = 0;
	u64* paBase = NULL;
	u64 prp1, prp2;
	void* next_buf = buf;
	void* prp2_addr;

	// DIRECT - No copy - must do sync
	// Buffered - Copy to buffer
	//  - Using page cache
	// Sync - Polling
	// Async - return wo/poll
	/* len必须以512对齐 */
	if(len % 512) return 0;

	if(!nvmed_rw_verify_area(nvmed_handle, start_lba, len))
		return -1;

	if(FLAG_ISSET(nvmed_handle, HANDLE_MQ)) {
		nvmed_queue = nvmed_handle->mq_get_queue(nvmed_handle, opcode,
				start_lba, len);

		if(nvmed_queue == NULL)
			return 0;
	}
	else {
		nvmed_queue = HtoQ(nvmed_handle);
	}
	nvmed = nvmed_queue->nvmed;

	/* 非同步io,完成的io需要一个独立的线程去处理 */
	if((!FLAG_ISSET(nvmed_handle, HANDLE_SYNC_IO)) &&
			private == NULL &&
			nvmed->process_cq_status == TD_STATUS_STOP) {
		nvmed->process_cq_status = TD_STATUS_REQ_SUSPEND;
		/* 创建cq线程,并且让线程suspend */
		pthread_create(&nvmed->process_cq_td, NULL, &nvmed_process_cq, (void*)nvmed);
		/* 等待线程suspend */
		while(nvmed->process_cq_status != TD_STATUS_SUSPEND);

		pthread_mutex_lock(&nvmed->process_cq_mutex);
		/* 让cq线程开始执行 */
		pthread_cond_signal(&nvmed->process_cq_cond);
		pthread_mutex_unlock(&nvmed->process_cq_mutex);
	}

	remain = len;
	if(FLAG_ISSET(nvmed_handle, HANDLE_HINT_DMEM)) {
		/* 从nvmed_get_buffer中获取nvme_buf */
		nvmed_buf_size = nvmed_check_buffer(buf);
		/* pa list的地址 */
		paBase = buf + (PAGE_SIZE * nvmed_buf_size) + 8;
	}
	/* 使用context提前准备好的paBase */
	if(context != NULL && context->prpList != NULL)
		paBase = context->prpList;

	// if Buf aligned -> Fn ==> non-cp fn
	// Not aligned -> Fn ==> mem_cp fn
	while(remain > 0) {
		/* 单次最大处理的io */
		if(remain > nvmed->dev_info->max_hw_sectors * 512 )
			io_size = nvmed->dev_info->max_hw_sectors * 512;
		else
			io_size = remain;
		/* io的起始lba */
		io_lba = total_io + start_lba;
		make_prp_list(nvmed_handle, next_buf, total_io , io_size,
					paBase, &prp1, &prp2, &prp2_addr);
		/* prp2_addr是prp page的虚拟地址 */
		io = nvmed_io(nvmed_handle, opcode, prp1, prp2, prp2_addr, NULL,
				io_lba, io_size, nvmed_handle->flags, context);

		if(io <= 0) break;

		remain -= io;
		total_io += io;
		io_lba += io;
		if(!pio)
			nvmed_handle->offset += io;
		next_buf += io;
	}

	return total_io;
}

off_t nvmed_lseek(NVMED_HANDLE* nvmed_handle, off_t offset, int whence) {
	int ret = -1;

	if(whence == SEEK_SET) {
		if(offset < HtoD(nvmed_handle)->dev_info->capacity) {
			nvmed_handle->offset = offset;
			ret = nvmed_handle->offset;
		}
		else
			ret = -1;
	}
	else if(whence == SEEK_CUR) {
		if(offset + nvmed_handle->offset < HtoD(nvmed_handle)->dev_info->capacity) {
			nvmed_handle->offset += offset;
			ret = nvmed_handle->offset;
		}
		else
			ret = -1;
	}
	else if(whence == SEEK_END) {
		if(offset <= HtoD(nvmed_handle)->dev_info->capacity) {
			nvmed_handle->offset = HtoD(nvmed_handle)->dev_info->capacity - offset;
			ret = nvmed_handle->offset;
		}
		else
			ret = -1;
	}

	return ret;
}

/* aio提交的io个数 */
int nvmed_aio_queue_submit(NVMED_HANDLE* handle) {
	NVMED_QUEUE* nvmed_queue;
	int num_submit = 0;

	nvmed_queue = HtoQ(handle);

	num_submit = nvmed_queue->aio_q_head;
	nvmed_queue->aio_q_head = 0;

	return num_submit;
}

int nvmed_aio_enqueue(NVMED_AIO_CTX* context) {
	NVMED_HANDLE *handle = context->handle;
	NVMED_QUEUE *queue = HtoQ(handle);

	if(!nvmed_rw_verify_area(handle, context->start_lba, context->len))
		return NVMED_AIO_ERROR;

	queue->aio_q_head++;
	context->status = AIO_INIT;
	context->num_init_io = 0;
	context->num_complete_io = 0;

	nvmed_io_rw(context->handle, context->opcode,
				context->buf, context->start_lba, context->len, NVMED_FALSE, context);

	return NVMED_AIO_QUEUED;
}


int nvmed_aio_read(NVMED_AIO_CTX* context) {
	context->opcode = nvme_cmd_read;
	return nvmed_aio_enqueue(context);
}

int nvmed_aio_write(NVMED_AIO_CTX* context) {
	context->opcode = nvme_cmd_write;
	return nvmed_aio_enqueue(context);
}

ssize_t nvmed_buffer_read(NVMED_HANDLE* nvmed_handle, u8 opcode, void* buf,
		unsigned long start_lba, unsigned int len, NVMED_BOOL pio, void* private) {
	NVMED_QUEUE *nvmed_queue = HtoQ(nvmed_handle);
	NVMED *nvmed = nvmed_queue->nvmed;
	NVMED_CACHE **cacheP, *cache, *__cache;
	NVMED_CACHE **cacheTarget;
	ssize_t total_read = 0;
	unsigned long start_block, end_block, io_blocks;
	unsigned int  find_blocks, final_num_blocks;
	unsigned long	io_start, io_nums = 0;
	int i = 0, block_idx;
	int cache_idx = 0;
	unsigned int buf_offs = 0, buf_copy_size = 0, cache_offs = 0;
	TAILQ_HEAD(cache_list, nvmed_cache) temp_head;

	if(!nvmed_rw_verify_area(nvmed_handle, start_lba, len))
		return -1;

	start_block = start_lba / PAGE_SIZE;
	end_block = (start_lba + len - 1) / PAGE_SIZE;
	io_blocks = end_block - start_block + 1;

	cacheP = calloc(io_blocks, sizeof(NVMED_CACHE*));

	pthread_rwlock_rdlock(&nvmed->cache_radix_lock);
	find_blocks = radix_tree_gang_lookup(&nvmed->cache_root,
			(void **)cacheP, start_block, io_blocks);
	pthread_rwlock_unlock(&nvmed->cache_radix_lock);

	TAILQ_INIT(&temp_head);

	if(find_blocks > 0) {
		cache = *(cacheP + 0);
		if(cache->lpaddr > end_block)
			find_blocks = 0;
		else {
			final_num_blocks = 0;
			for(i=0; i<find_blocks; i++) {
				cache = *(cacheP + i);
				if(cache->lpaddr >= start_block && end_block >= cache->lpaddr)
					final_num_blocks++;
			}
			find_blocks = final_num_blocks;
		}
	}

	if(find_blocks == 0) {
		//read all
		/* cache完全没有命中 */
		for(i=0; i<io_blocks; i++) {
			/* 从handle中获取cache */
			cache = nvmed_get_cache(nvmed_handle);
			TAILQ_INSERT_TAIL(&temp_head, cache, io_list);
		}
		/* 采用同步io读取设备,填充所有在temp_head上的cache */
		nvmed_cache_io_rw(nvmed_handle, nvme_cmd_read, temp_head.tqh_first,
				start_block * PAGE_SIZE, io_blocks * PAGE_SIZE, HANDLE_SYNC_IO);

		cache_idx = 0;
		while(temp_head.tqh_first != NULL) {
			/* 从temp_head中获取cache,这个cache中包含了从设备中获取的数据 */
			cache = temp_head.tqh_first;

			TAILQ_REMOVE(&temp_head, cache, io_list);
			/* cache的lpaddr指向需要读取的block */
			cache->lpaddr = start_block + cache_idx;
			/* 设置cache entry标志 */
			FLAG_SET_SYNC(cache, CACHE_LRU | CACHE_UPTODATE);

			if(cache_idx==0) {
				cache_offs = start_lba % PAGE_SIZE;
				if(cache_offs + len <= PAGE_SIZE) {
					buf_copy_size  = len;
				}
				else {
					buf_copy_size = PAGE_SIZE - cache_offs;
				}
				memcpy(buf, cache->ptr + cache_offs, buf_copy_size);
				////?????? buf_offs = buf_copy_size;
				buf_offs+= buf_copy_size;
			}
			else if(cache_idx == io_blocks -1) {
				buf_copy_size = len - buf_offs;
				memcpy(buf + buf_offs, cache->ptr, buf_copy_size);
			}
			else {
				buf_copy_size = PAGE_SIZE;
				memcpy(buf + buf_offs, cache->ptr, buf_copy_size);
				buf_offs+= PAGE_SIZE;
			}
			/* 原子操作,cache的ref-- */
			DEC_SYNC(cache->ref);

			pthread_rwlock_wrlock(&nvmed->cache_radix_lock);
			pthread_spin_lock(&nvmed->cache_list_lock);
			/* cache插入lru尾部 */
			TAILQ_INSERT_TAIL(&nvmed->lru_head, cache, cache_list);
			/* cache加入radix tree */
			radix_tree_insert(&nvmed->cache_root, cache->lpaddr, cache);
			pthread_spin_unlock(&nvmed->cache_list_lock);
			pthread_rwlock_unlock(&nvmed->cache_radix_lock);

			/* 没有人使用了这个cache,设置ref == 0 */
			INIT_SYNC(cache->ref);
			nvmed->num_cache_usage++;
			cache_idx++;
		}
	}
	else {
		//find empty block
		if(find_blocks != io_blocks) {
		/* 部分命中,需要建立新的cache并且填充 */
			//Find Hole?

			cacheTarget = malloc(sizeof(NVMED_CACHE*) * io_blocks);

			io_nums = 0;
			io_start = 0;

			i=0;
			for(block_idx = start_block; block_idx <= end_block; block_idx++) {
				/* cacheP中保存命中的cache */
				cache = *(cacheP + i);
				if(cache != NULL && cache->lpaddr == block_idx) {
					/* 处理这个命中的cache */
					if(io_nums != 0) {
						/* 中间有io_nums个cache是新的,需要填充 */
						nvmed_cache_io_rw(nvmed_handle, nvme_cmd_read, temp_head.tqh_first,
							io_start * PAGE_SIZE, io_nums * PAGE_SIZE, HANDLE_SYNC_IO);

						pthread_rwlock_wrlock(&nvmed->cache_radix_lock);
						pthread_spin_lock(&nvmed->cache_list_lock);

						while(temp_head.tqh_first != NULL) {
							__cache = temp_head.tqh_first;
							/* 从temp_head取下来,之后就不需要调用nvmed_cache_io_rw填充了 */
							TAILQ_REMOVE(&temp_head, __cache, io_list);
							TAILQ_INSERT_TAIL(&nvmed->lru_head, __cache, cache_list);
							radix_tree_insert(&nvmed->cache_root, __cache->lpaddr, __cache);
							FLAG_SET_SYNC(__cache, CACHE_LRU);
						}

						pthread_spin_unlock(&nvmed->cache_list_lock);
						pthread_rwlock_unlock(&nvmed->cache_radix_lock);

						io_nums = 0;
					}
					else
						INC_SYNC(cache->ref);

					i++;

					cacheTarget[block_idx-start_block] = cache;
					pthread_spin_lock(&nvmed->cache_list_lock);
					TAILQ_REMOVE(&nvmed->lru_head, cache, cache_list);
					TAILQ_INSERT_TAIL(&nvmed->lru_head,cache, cache_list);
					pthread_spin_unlock(&nvmed->cache_list_lock);
				} else {
					/* cacheP中这个cache并不是对应这个block的,那么就从handle中分配一个cache entry */
					cache = nvmed_get_cache(nvmed_handle);
					cache->lpaddr = block_idx;
					TAILQ_INSERT_TAIL(&temp_head, cache, io_list);
					/* 从handle中分配了cache entry的统计计数 */
					io_nums++;
					/* 第一个没有命中的cache的物理地址. 通过nvmed_cache_io_rw进行填充 */
					if(io_nums == 1) io_start = cache->lpaddr;

					nvmed->num_cache_usage++;

					cacheTarget[block_idx-start_block] = cache;
				}
			}
			/* cacheTarget保存了全段的cache */
			for(i=0; i<io_blocks; i++)
				*(cacheP + i) = cacheTarget[i];

			free(cacheTarget);
		}

		/* 有部分cache是新分配的,需要从设备获取数据填充 */
		if(io_nums != 0) {
			nvmed_cache_io_rw(nvmed_handle, nvme_cmd_read, temp_head.tqh_first,
					io_start * PAGE_SIZE, io_nums * PAGE_SIZE, HANDLE_SYNC_IO);

			pthread_rwlock_wrlock(&nvmed->cache_radix_lock);
			pthread_spin_lock(&nvmed->cache_list_lock);

			while(temp_head.tqh_first != NULL) {
				__cache = temp_head.tqh_first;
				TAILQ_REMOVE(&temp_head, __cache, io_list);
				TAILQ_INSERT_TAIL(&nvmed->lru_head, __cache, cache_list);
				radix_tree_insert(&nvmed->cache_root, __cache->lpaddr, __cache);
				FLAG_SET_SYNC(__cache, CACHE_LRU);
			}

			pthread_spin_unlock(&nvmed->cache_list_lock);
			pthread_rwlock_unlock(&nvmed->cache_radix_lock);
		}

		/* cache已经完全建立,那么就将cache中的数据拷贝到buf */
		for(cache_idx=0; cache_idx<io_blocks; cache_idx++) {
			cache = *(cacheP + cache_idx);

			if(cache_idx==0) {
				cache_offs = start_lba % PAGE_SIZE;
				if(cache_offs + len <= PAGE_SIZE) {
					buf_copy_size  = len;
				}
				else {
					buf_copy_size = PAGE_SIZE - cache_offs;
				}
				////?????memcpy(buf, cache->ptr + cache_offs, buf_copy_size);
				memcpy(buf + buf_offs, cache->ptr + cache_offs, buf_copy_size);
				buf_offs = buf_copy_size;
			}
			else if(cache_idx == io_blocks -1) {
				buf_copy_size = len - buf_offs;
				memcpy(buf + buf_offs, cache->ptr, buf_copy_size);
			}
			else {
				buf_copy_size = PAGE_SIZE;
				memcpy(buf + buf_offs, cache->ptr, buf_copy_size);
				buf_offs+= PAGE_SIZE;
			}

		}
	}

	total_read = len;

	free(cacheP);

	if(!pio)
		nvmed_handle->offset += total_read;

	return total_read;
}

/* pio表示是否是pwrite调用进来的 */
ssize_t nvmed_buffer_write(NVMED_HANDLE* nvmed_handle, u8 opcode, void* buf,
		unsigned long start_lba, unsigned int len, NVMED_BOOL pio, void* private) {
	NVMED *nvmed = HtoD(nvmed_handle);
	NVMED_CACHE **cacheP, *cache;
	ssize_t total_write = 0;
	unsigned long start_block, end_block, io_blocks;
	unsigned int buf_offs, buf_copy_size, cache_offs;
	unsigned int  find_blocks, final_num_blocks;
	int i, block_idx=0, cache_idx=0;
	NVMED_BOOL found_from_cache;
	TAILQ_HEAD(cache_list, nvmed_cache) temp_head;
	//fprintf(stderr, "%s: %lu %u\n", __func__, start_lba, len);
	if(!nvmed_rw_verify_area(nvmed_handle, start_lba, len))
		return -1;

	/* 以page size为block size */
	start_block = start_lba / PAGE_SIZE;
	end_block = (start_lba + len - 1) / PAGE_SIZE;
	io_blocks = end_block - start_block + 1;

	/* 分配io_blocks个cache */
	cacheP = calloc(io_blocks, sizeof(NVMED_CACHE*));

	pthread_rwlock_rdlock(&nvmed->cache_radix_lock);
	/*
	 * 查找start_block开始的io_blocks个block的cache. 找到的cache数量放在find_blocks中.
	 * 找到的cache放到数组cacheP中
	 */
	find_blocks = radix_tree_gang_lookup(&nvmed->cache_root,
			(void **)cacheP, start_block, io_blocks);
	pthread_rwlock_unlock(&nvmed->cache_radix_lock);

	TAILQ_INIT(&temp_head);
	/* 检查cacheP数组中的cache,统计有多少cache在[start_blocks,end_block]范围内 */
	if(find_blocks > 0) {
		cache = *(cacheP + 0);
		if(cache->lpaddr > end_block)
			find_blocks = 0;
		else {
			final_num_blocks= 0;
			for(i=0; i<find_blocks; i++) {
				cache = *(cacheP + i);
				if(cache->lpaddr >= start_block && end_block <= cache->lpaddr)
					final_num_blocks++;
			}
			find_blocks = final_num_blocks;
		}
	}

	//find all in cache?
	/* cache全部命中 */
	if(find_blocks == io_blocks) {
		for(cache_idx=0; cache_idx<find_blocks; cache_idx++) {
			cache = *(cacheP + cache_idx);

			/* 等待cache释放lock */
			while(FLAG_ISSET_SYNC(cache, CACHE_LOCKED)) {
				usleep(1);
			}

			/* 第一个和最后一个cache entry有非对齐的情况 */
			if(cache_idx==0) {
				cache_offs = start_lba % PAGE_SIZE;
				if(cache_offs + len <= PAGE_SIZE) {
					buf_copy_size  = len;
				}
				else {
					buf_copy_size = PAGE_SIZE - cache_offs;
				}
				/* 将buf中未对齐的部分拷贝到cache */
				memcpy(cache->ptr + cache_offs, buf, buf_copy_size);
				/* 跳过已经拷贝的部分 */
				buf_offs = buf_copy_size;
			}
			else if(cache_idx == io_blocks -1) {
				buf_copy_size = len - buf_offs;
				memcpy(cache->ptr, buf + buf_offs, buf_copy_size);
			}
			else {
				buf_copy_size = PAGE_SIZE;
				memcpy(cache->ptr, buf + buf_offs, buf_copy_size);
				/* buf_offs记录拷贝的位置 */
				buf_offs+= PAGE_SIZE;
			}

			pthread_spin_lock(&nvmed->cache_list_lock);
			/* 将cache插入lru尾部 */
			TAILQ_REMOVE(&nvmed->lru_head, cache, cache_list);
			TAILQ_INSERT_TAIL(&nvmed->lru_head, cache, cache_list);

			/* 如果cache没有设置CACHE_WRITEBACK,那么就将cache加入temp_head */
			if(!FLAG_ISSET(cache, CACHE_WRITEBACK))
				TAILQ_INSERT_TAIL(&temp_head, cache, io_list);

			pthread_spin_lock(&nvmed_handle->dirty_list_lock);
			/* cache dirty了 */
			if(!FLAG_ISSET_SYNC(cache, CACHE_DIRTY)) {
				FLAG_SET_SYNC(cache, CACHE_DIRTY);
				LIST_INSERT_HEAD(&nvmed_handle->dirty_list, cache, handle_cache_list);
			}
			pthread_spin_unlock(&nvmed_handle->dirty_list_lock);

			pthread_spin_unlock(&nvmed->cache_list_lock);
		}
	}
	else {
		// partial write block ?
		// fill
		/* cache部分命中 */
		cache_idx=0;
		for(block_idx = start_block; block_idx <= end_block; block_idx++) {
			cache = *(cacheP + cache_idx);
			found_from_cache = NVMED_FALSE;
			if(cache != NULL &&cache->lpaddr == block_idx) {
				found_from_cache = NVMED_TRUE;
				/* cache命中,将cache从lru上移除,之后会重新插入lru尾部 */
				TAILQ_REMOVE(&nvmed->lru_head, cache, cache_list);
			}
			else {
				/* cache miss,从handle中获取一个cache entry */
				cache = nvmed_get_cache(nvmed_handle);
				cache->lpaddr = block_idx;
				nvmed->num_cache_usage++;
			}

			/* 其他地方使用了cache,等待lock释放 */
			if(found_from_cache)
				while(FLAG_ISSET_SYNC(cache, CACHE_LOCKED))
					usleep(1);

			if(cache_idx==0) {
				cache_offs = start_lba % PAGE_SIZE;
				if(cache_offs + len <= PAGE_SIZE) {
					buf_copy_size  = len;
				}
				else {
					buf_copy_size = PAGE_SIZE - cache_offs;
				}

				/* 如果这个cache未命中,又不是完全覆盖写，那么就需要先读取设备,填充cache entry */
				if(!found_from_cache && buf_copy_size != PAGE_SIZE) {
					nvmed_cache_io_rw(nvmed_handle, nvme_cmd_read, cache, \
						cache->lpaddr * PAGE_SIZE, PAGE_SIZE, HANDLE_SYNC_IO);
				}
				memcpy(cache->ptr + cache_offs, buf, buf_copy_size);
				buf_offs = buf_copy_size;
			}
			else if(cache_idx == io_blocks -1) {
				buf_copy_size = len - buf_offs;

				if(!found_from_cache && buf_copy_size != PAGE_SIZE) {
					nvmed_cache_io_rw(nvmed_handle, nvme_cmd_read, cache, \
						cache->lpaddr * PAGE_SIZE, PAGE_SIZE, HANDLE_SYNC_IO);
				}

				memcpy(cache->ptr, buf, buf_copy_size);
			}
			else {
				/* 全部覆盖写的情况下,即便cache未命中,也不需要从设备读取数据填充cache */
				buf_copy_size = PAGE_SIZE;
				memcpy(cache->ptr, buf, buf_copy_size);
				buf_offs+= PAGE_SIZE;
			}

			if(!found_from_cache) {
				pthread_rwlock_wrlock(&nvmed->cache_radix_lock);
				/* 新建的cache,根据block index加入radix tree */
				radix_tree_insert(&nvmed->cache_root, cache->lpaddr, cache);
				pthread_rwlock_unlock(&nvmed->cache_radix_lock);
			}
			else {
				/* 取数组cacheP下一个cache entry */
				cache_idx++;
			}

			pthread_spin_lock(&nvmed->cache_list_lock);

			/* 将cache entry插入lru尾部 */
			TAILQ_INSERT_TAIL(&nvmed->lru_head, cache, cache_list);

			pthread_spin_lock(&nvmed_handle->dirty_list_lock);
			if(!FLAG_ISSET_SYNC(cache, CACHE_DIRTY)) {
				FLAG_SET_SYNC(cache, CACHE_DIRTY);
				LIST_INSERT_HEAD(&nvmed_handle->dirty_list, cache, handle_cache_list);
			}
			pthread_spin_unlock(&nvmed_handle->dirty_list_lock);

			pthread_spin_unlock(&nvmed->cache_list_lock);
			/* 加入temp_head */
			TAILQ_INSERT_TAIL(&temp_head, cache, io_list);
		}
	}

	if(nvmed->process_cq_status == TD_STATUS_SUSPEND) {
		pthread_mutex_lock(&nvmed->process_cq_mutex);
		pthread_cond_signal(&nvmed->process_cq_cond);
		pthread_mutex_unlock(&nvmed->process_cq_mutex);
	}

	// IO MERGE AND EXEC
	/*
	 * handle没有设置HANDLE_SYNC_IO的时候才能积攒cache批量下发io.
	 * 将temp_head上的io按照lpaddr排序加入handle的io_head
	 */
	if(!FLAG_ISSET(nvmed_handle, HANDLE_SYNC_IO)) {
		pthread_spin_lock(&nvmed_handle->io_head_lock);
		/* 加入temp_head的cache要开始执行io */
		while(temp_head.tqh_first != NULL) {
			cache = temp_head.tqh_first;
			cache->handle = nvmed_handle;
			//if io_head empty?
			if(TAILQ_EMPTY(&nvmed_handle->io_head)) {
				/* io_head链表上原本就是空的 */
				FLAG_SET(cache, CACHE_WRITEBACK);
				TAILQ_REMOVE(&temp_head, cache, io_list);
				TAILQ_INSERT_HEAD(&nvmed_handle->io_head, cache, io_list);
				nvmed_handle->num_io_head++;
			}
			//back merge?
			else if(TAILQ_LAST(&nvmed_handle->io_head, io_list)->lpaddr + 1
					== cache->lpaddr) {
				/* io_head尾部的cache和当前从temp_head上取下来的cache物理地址连续 */
				FLAG_SET(cache, CACHE_WRITEBACK);
				TAILQ_REMOVE(&temp_head, cache, io_list);
				/* 将cache加入io_head的尾部 */
				TAILQ_INSERT_TAIL(&nvmed_handle->io_head, cache, io_list);
				nvmed_handle->num_io_head++;
			}
			//front merge?
			else if(TAILQ_FIRST(&nvmed_handle->io_head)->lpaddr - 1
					== cache->lpaddr) {
				/* cache和io_head第一个cache上物理连续 */
				FLAG_SET(cache, CACHE_WRITEBACK);
				TAILQ_REMOVE(&temp_head, cache, io_list);
				TAILQ_INSERT_HEAD(&nvmed_handle->io_head, cache, io_list);
				nvmed_handle->num_io_head++;
			}
			else {
				/* cache无法加入io_head,就需要将io_head上所有cache写入设备 */
				__evict_handle_dirty_page(nvmed_handle);
			}
		}

		/* cache entry个数不能超过18.也就是说达到512KB就需要写入一次 */
		if(nvmed_handle->num_io_head == 128) {
			__evict_handle_dirty_page(nvmed_handle);
		}

		pthread_spin_unlock(&nvmed_handle->io_head_lock);
	}
	else {
		/* handle设置了HANDLE_SYNC_IO,无法排序cache.也无法统一回刷handle->io_head */
		nvmed_cache_io_rw(nvmed_handle, nvme_cmd_write, temp_head.tqh_first, \
			start_block * PAGE_SIZE, io_blocks * PAGE_SIZE, nvmed_handle->flags);
	}

	/* 返回写入的长度 */
	total_write = len;

	/* 如果是通过nvme_write调用进来的 */
	if(!pio)
		nvmed_handle->offset += total_write;

	free(cacheP);

	return total_write;
}

//offset, length -> should be 512B Aligned

ssize_t nvmed_pread(NVMED_HANDLE* nvmed_handle, void* buf, size_t count, off_t offset) {
	ssize_t ret;
	//fprintf(stderr, "%s: %lu %lu\n", __func__, offset, count);
	ret = nvmed_handle->read_func(nvmed_handle, nvme_cmd_read,
			buf, offset, count, NVMED_TRUE, NULL);
	return ret;
}

ssize_t nvmed_read(NVMED_HANDLE* nvmed_handle, void* buf, size_t count) {
	ssize_t ret;

	ret = nvmed_handle->read_func(nvmed_handle, nvme_cmd_read,
			buf, nvmed_handle->offset, count, NVMED_FALSE, NULL);

	return ret;
}

//offset, length -> should be 512B Aligned
ssize_t nvmed_pwrite(NVMED_HANDLE* nvmed_handle, void* buf, size_t count, off_t offset) {
	ssize_t ret;
	//fprintf(stderr, "%s: %lu %lu\n", __func__, offset, count);
	ret = nvmed_handle->write_func(nvmed_handle, nvme_cmd_write,
			buf, offset, count, NVMED_TRUE, NULL);

	return ret;
}

ssize_t nvmed_write(NVMED_HANDLE* nvmed_handle, void* buf, size_t count) {
	ssize_t ret;

	ret = nvmed_handle->write_func(nvmed_handle, nvme_cmd_write,
			buf, nvmed_handle->offset, count, NVMED_FALSE, NULL);

	return ret;
}

/* 冲刷cache */
void nvmed_flush_handle(NVMED_HANDLE* nvmed_handle) {
	pthread_spin_lock(&nvmed_handle->io_head_lock);
		if(!TAILQ_EMPTY(&nvmed_handle->io_head)) {
			__evict_handle_dirty_page(nvmed_handle);
		}
	pthread_spin_unlock(&nvmed_handle->io_head_lock);
}

void nvmed_flush(NVMED_HANDLE* __nvmed_handle) {
	NVMED* nvmed = HtoD(__nvmed_handle);
	NVMED_QUEUE* nvmed_queue;
	NVMED_HANDLE* nvmed_handle;

	pthread_spin_lock(&nvmed->mngt_lock);
	/* flush一个nvmed上所有的queue */
	for (nvmed_queue = nvmed->queue_head.lh_first;
			nvmed_queue != NULL; nvmed_queue = nvmed_queue->queue_list.le_next) {
		// handle writeback
		/* flush一个queue上的所有handle */
		for (nvmed_handle = nvmed_queue->handle_head.lh_first;
				nvmed_handle != NULL; nvmed_handle = nvmed_handle->handle_list.le_next) {
			nvmed_flush_handle(nvmed_handle);
		}
	}
	pthread_spin_unlock(&nvmed->mngt_lock);

	//nvmed_flush_handle(nvmed_handle);

	nvmed_handle = __nvmed_handle;
	/* 执行nvme flush命令 */
	if(HtoD(nvmed_handle)->dev_info->vwc != 0)
		nvmed_io(nvmed_handle, nvme_cmd_flush, 0, 0, 0, NULL, 0, 0, HANDLE_SYNC_IO, NULL);

}

/* discard命令 */
int nvmed_discard(NVMED_HANDLE* nvmed_handle, unsigned long start, unsigned int len) {
	struct nvme_dsm_range *range;
	u64 __prp;

	if(start % 512 || len % 512) return -NVMED_FAULT;

	/* 获取一个prp page */
	range = nvmed_handle_get_prp(nvmed_handle, &__prp);

	range->cattr = 0;
	range->nlb = len >> nvmed_handle->queue->nvmed->dev_info->lba_shift;
	range->slba = start;

	/* trim */
	nvmed_io(nvmed_handle, nvme_cmd_dsm, __prp, 0, 0, NULL, start, len, HANDLE_SYNC_IO, NULL);

	return 0;
}

/*
 * (for nvmed_admin tools) Set NVMeDirect Queue Permission
 */
int nvmed_set_user_quota(NVMED* nvmed, uid_t uid, unsigned int num_queue,
		unsigned int* max_queue, unsigned int* current_queue) {
	NVMED_USER_QUOTA quota;
	int ret;

	quota.uid = uid;
	quota.queue_max = num_queue;

	ret = ioctl(nvmed->ns_fd, NVMED_IOCTL_SET_USER, &quota);
	if(ret < 0) return ret;

	if(max_queue != NULL) *max_queue = quota.queue_max;
	if(current_queue != NULL) *current_queue = quota.queue_used;

	return NVMED_SUCCESS;
}

/*
 * (for nvmed_admin tools) Get NVMeDirect Queue Permission
 */
int nvmed_get_user_quota(NVMED* nvmed, uid_t uid,
		unsigned int* max_queue, unsigned int* current_queue) {
	NVMED_USER_QUOTA quota;
	int ret;

	quota.uid = uid;

	ret = ioctl(nvmed->ns_fd, NVMED_IOCTL_GET_USER, &quota);
	if(ret < 0) return ret;

	if(max_queue != NULL) *max_queue = quota.queue_max;
	if(current_queue != NULL) *current_queue = quota.queue_used;

	return NVMED_SUCCESS;
}
