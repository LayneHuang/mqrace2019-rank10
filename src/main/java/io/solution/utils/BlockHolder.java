package io.solution.utils;

import io.solution.GlobalParams;
import io.solution.data.MyBlock;

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

    public static BlockHolder getIns() {
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

    }

    public MyBlock apply() {
        if (totalCount >= GlobalParams.BLOCK_COUNT_LIMIT) {
            return null;
        } else {
            MyBlock block = null;
            applyLock.lock();
            if (totalCount < GlobalParams.BLOCK_COUNT_LIMIT) {
                totalCount++;
                block = new MyBlock();
            }
            applyLock.unlock();
            return block;
        }
    }

    public void commit(MyBlock block) {
        blockQueue.add(block);
    }

}
