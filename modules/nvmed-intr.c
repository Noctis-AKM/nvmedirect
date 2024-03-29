#include <linux/printk.h>
#include <linux/pci.h>
#include <linux/interrupt.h>
#include <linux/irqdesc.h>
#include <linux/msi.h>
#include "./nvmed.h"

#ifndef dev_to_msi_list
	#define dev_to_msi_list(dev)		(&to_pci_dev((dev))->msi_list)
#endif

#ifndef for_each_msi_entry
	#define for_each_msi_entry(desc, pdev)	\
		list_for_each_entry((desc), dev_to_msi_list((pdev)), list)
#endif

/* 将msix的index转换成irq no */
#ifdef NVMED_MSIX_HANDLER_V1
	#define vector_to_irq(dev_entry, pdev, irq_vector) dev_entry->msix_entry[irq_vector].vector
#else
	#define vector_to_irq(dev_entry, pdev, irq_vector) pci_irq_vector(pdev, irq_vector)
#endif

#define QUEUE_BMAP_IDX(qid) qid/(sizeof(unsigned long) * 8)
#define QUEUE_BMAP_OFF(qid) qid%(sizeof(unsigned long) * 8)
int nvmed_irq_comm(NVMED_NS_ENTRY *ns_entry, unsigned long __user *__qid) {
	NVMED_DEV_ENTRY *dev_entry = ns_entry->dev_entry;
	struct nvme_dev *dev = NS_ENTRY_TO_DEV(ns_entry);
	NVMED_QUEUE_ENTRY *queue, *queue_next;
	unsigned long qid;
	unsigned long ret;
	unsigned long *qbmap;
	int nr_queue, nr_bmap;
	bool hasCQ = false;

	copy_from_user(&qid, __qid, sizeof(unsigned long));

	/* 清空设备上所有queue的中断计数清0 */
	if(qid == 0) {
		nr_queue = dev->queue_count + dev_entry->num_user_queue;
		/* 一个unsigned long表示一个bmap */
		nr_bmap = QUEUE_BMAP_IDX(nr_queue);
		if(QUEUE_BMAP_OFF(nr_queue) == 0) nr_bmap++;

		qbmap = kzalloc(sizeof(unsigned long) * nr_bmap, GFP_KERNEL);
		while(1) {
			/* 遍历一个ns上所有的queue */
			list_for_each_entry_safe(queue, queue_next, &ns_entry->queue_list, list) {
				/* 没有注册中断,跳过这个queue */
				if(queue->irq_vector == 0) continue;
				qid = queue->nvmeq->qid;

				ret = atomic_read(&queue->nr_intr);
				if(ret > 0) {
					/* 记录这个被清除中断计数的queue */
					qbmap[QUEUE_BMAP_IDX(qid)] |= 1<<QUEUE_BMAP_OFF(qid);
					atomic_set(&queue->nr_intr, 0);
					hasCQ = true;
				}
			}
			if(dev_entry->num_user_queue == 0)
				hasCQ = true;

			/* 一旦过中断就要返回 */
			if(hasCQ == true)
				break;
			else
				/* 没有清除过中断,就schedule */
				schedule();
		}

		/* 将中断清除的queue返回给用户 */
		copy_to_user(__qid, qbmap, sizeof(unsigned long) * nr_bmap);
		kfree(qbmap);

		if(dev_entry->num_user_queue == 0)
			return -EINVAL;
	}
	else {
		while(1) {
			/* 清除特定qid的中断计数 */
			queue = nvmed_get_queue_from_qid(ns_entry, qid);

			if(queue == NULL) return -EINVAL;
			/* 指定的queue没有注册中断,返回EINVAL */
			if(queue->irq_vector == 0) return -EINVAL;

			ret = atomic_read(&queue->nr_intr);
			if(ret == 0)
				schedule();
			else 
				break;
		};

		atomic_set(&queue->nr_intr, 0);
		copy_to_user(__qid, &ret, sizeof(unsigned long));
	}

	return NVMED_SUCCESS;
}

static irqreturn_t nvmed_irq_handler(int irq, void *data) {
	NVMED_QUEUE_ENTRY *queue = data;

	irqreturn_t result;

	/* 递增一个queue上的中断计数 */
	atomic_inc(&queue->nr_intr);

	result = IRQ_HANDLED;

	return result;
}

NVMED_RESULT nvmed_reinitialize_msix(NVMED_DEV_ENTRY *dev_entry, 
		unsigned long nr_vecs) {
#ifdef NVMED_MSIX_HANDLER_V1
	struct nvme_dev *dev = dev_entry->dev;
#endif
	struct pci_dev *pdev = dev_entry->pdev;
	struct msi_desc *msi_desc;
	struct irq_desc *irq_desc;
	struct irqaction *action;
	struct nvme_irq_desc *desc, *desc_arr;
#ifdef NVMED_MSIX_HANDLER_V1
	struct msix_entry *entry;
#endif
	int irq_idx = 0;
	int vecs;
	int dev_id, q_id;
	int i;
	int ret;

	if(!check_msix(dev_entry)) return -NVMED_FAULT;

	/* nr_vecs个nvme_irq_desc */
	desc_arr = kzalloc(sizeof(struct nvme_irq_desc) * nr_vecs, 
			GFP_KERNEL);
#ifdef NVMED_MSIX_HANDLER_V1
	/* 重新分配nr_vecs个msix entry */
	entry = kzalloc(sizeof(struct msix_entry) * nr_vecs, GFP_KERNEL);

	/* 设置msix entry的no */
	for(i=0; i<nr_vecs; i++) {
		entry[i].entry = i;
	}

#endif

	/* 遍历pci中设备驱动模型的dev关联的所有msi_desc */
	for_each_msi_entry(msi_desc, &pdev->dev) {
		/* 根据irq no在radix tree中获取irq_desc */
		irq_desc = irq_to_desc(msi_desc->irq);
		action = irq_desc->action;
		/* 处理共享中断 */
		while(action) {
			desc = &desc_arr[irq_idx];
			desc->handler = action->handler;
			desc->thread_fn = action->thread_fn;
			desc->affinity_hint = irq_desc->affinity_hint;
			desc->queue = action->dev_id;
			desc->irqName = action->name;

			/* 从中断名中获取dev_id和q_id */
			if(action->name[4] == 'd')
				sscanf(desc->irqName, "nvmed%dq%d", &dev_id, &q_id);
			else
				sscanf(desc->irqName, "nvme%dq%d", &dev_id, &q_id);

			/* misx entry的no */
			desc->cq_vector = (q_id-1 < 0)? 0:(q_id-1);

			irq_set_affinity_hint(action->irq, NULL);
			free_irq(action->irq, action->dev_id);

			action = action->next;
			/* 每个共享中断也要单独统计,之后执行中断申请 */
			irq_idx++;
		}
	}
	
	pci_disable_msix(pdev);

#ifdef NVMED_MSIX_HANDLER_V1
	/* 返回设置成功的entry个数 */
	vecs = pci_enable_msix_range(pdev, entry, 1, nr_vecs);

	if(dev_entry->msix_entry != NULL)
		kfree(dev_entry->msix_entry);

	dev_entry->msix_entry = entry;
#else
	vecs = pci_enable_msix_range(pdev, NULL, 1, nr_vecs);
#endif
	if(vecs > 0)
	/* 已有的misx中断全部重新注册中断 */
	for (i=0; i<irq_idx; i++) {
		desc = &desc_arr[i];
		/*
		 * 如果desc->thread_fn == NULL,那么就无法中断线程化.只能使用普通中断.这里使用
		 * request_threaded_irq是为了不改变之前的中断方式.
		 */
		ret = request_threaded_irq(vector_to_irq(dev_entry, pdev, desc->cq_vector),
					desc->handler, desc->thread_fn, IRQF_SHARED,
					desc->irqName, desc->queue);

		irq_set_affinity_hint(vector_to_irq(dev_entry, pdev, desc->cq_vector), desc->affinity_hint);
	}

#ifdef NVMED_MSIX_HANDLER_V1
	memcpy(dev->entry, entry, sizeof(struct msix_entry) * dev_entry->vec_kernel);
#endif
	kfree(desc_arr);

	return NVMED_SUCCESS;
	
}

NVMED_RESULT nvmed_register_intr_handler(NVMED_DEV_ENTRY *dev_entry,
		NVMED_QUEUE_ENTRY *queue, unsigned int irq_vector) {
	NVMED_RESULT result;
	int ret;
	//Need disable & re_enable msix range?
	/* 是否需要重新分配msix */
	if(irq_vector > dev_entry->vec_max) {
		/* irq_vector是0开始的.重新申请irq_vector + 1个msix entry */
		result = nvmed_reinitialize_msix(dev_entry, irq_vector + 1);
		if(!result)
			dev_entry->vec_max = irq_vector;
		else 
			goto error_nvmed_register_intr;
	}

	queue->irq_name = kzalloc(sizeof(char) * 32, GFP_KERNEL);
	sprintf(queue->irq_name, "nvmed%dq%d", DEV_TO_INSTANCE(dev_entry->dev), queue->nvmeq->qid);

	//Set new IRQ Handler
	/* 申请中断,中断号就是pci记录的irq */
	ret = request_irq(vector_to_irq(dev_entry, dev_entry->pdev, irq_vector),
			nvmed_irq_handler, IRQF_SHARED,
			queue->irq_name, queue);
	if(ret < 0) {
		pr_info("%s: Error on request_irq\n", __func__);
		goto error_set_handler;
	}
	//irq_set_affinity_hint(entry[desc->cq_vector].vector, affinity_mask);

	return NVMED_SUCCESS;

error_set_handler:
	kfree(queue->irq_name);
error_nvmed_register_intr:
	return -NVMED_FAULT;
}

NVMED_RESULT nvmed_free_intr_handler(NVMED_DEV_ENTRY *dev_entry, NVMED_QUEUE_ENTRY *queue,
		unsigned int qid) {
	free_irq(vector_to_irq(dev_entry, dev_entry->pdev, queue->irq_vector), queue);
	clear_bit(queue->irq_vector, dev_entry->vec_bmap);

	kfree(queue->irq_name);

	return NVMED_SUCCESS;
}
