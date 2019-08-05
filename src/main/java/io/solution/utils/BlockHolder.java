package io.solution.utils;

import io.solution.GlobalParams;
import io.solution.data.MyBlock;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author: laynehuang
 * @CreatedAt: 2019/8/2 0002
 */
public class BlockHolder {

    private BlockingQueue<MyBlock> blockQueue;

    private static BlockHolder ins;

    private Long threadId;

    private ReentrantLock applyLock;

    private int totalCount = 0;

    private BlockHolder() {
        applyLock = new ReentrantLock();
        blockQueue = new LinkedBlockingQueue<>();
        Thread thread = new Thread(this::work);
        thread.setName("BUFFER-THREAD");
        threadId = thread.getId();
        thread.start();
    }

    static BlockHolder getIns() {
        // double check locking
        if (ins != null) {
            return ins;
        } else {
            synchronized (BlockHolder.class) {
                if (ins == null) {
                    ins = new BlockHolder();
                }
            }
            return ins;
        }
    }

    private void work() {
        for (; ; ) {
            List<MyBlock> blocks = new ArrayList<>();
            try {
                synchronized (this) {
                    while (totalCount < GlobalParams.WRITE_COUNT_LIMIT) {
                        this.wait();
                    }
                    for (int i = 0; i < GlobalParams.WRITE_COUNT_LIMIT; ++i) {
                        MyBlock block = blockQueue.take();
                        blocks.add(block);
                    }
                    this.notifyAll();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            if (!blocks.isEmpty()) {
                // 归并
                blocks = SortUtil.myMergeSort(blocks);
                // Todo: 处理统计 & 维护索引
                // Todo: 写入缓冲
            }
        }
    }

    synchronized void commit(MyBlock block) {
        try {
            while (totalCount >= GlobalParams.BLOCK_COUNT_LIMIT) {
                this.wait();
            }
            blockQueue.add(block);
            this.notifyAll();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
