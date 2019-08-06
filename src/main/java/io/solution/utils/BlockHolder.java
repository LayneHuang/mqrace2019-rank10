package io.solution.utils;

import io.solution.GlobalParams;
import io.solution.data.MyBlock;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @Author: laynehuang
 * @CreatedAt: 2019/8/2 0002
 */
public class BlockHolder {

    private boolean isFinish = false;

    private LinkedBlockingQueue<MyBlock> blockQueue;

    private static BlockHolder ins;

    private BlockHolder() {
        blockQueue = new LinkedBlockingQueue<>(GlobalParams.BLOCK_COUNT_LIMIT);
        Thread workThread = new Thread(this::work);
        workThread.setName("BLOCK-HOLDER-THREAD");
        workThread.start();
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
            try {
                List<MyBlock> blocks = new ArrayList<>();
                for (int i = 0; i < GlobalParams.WRITE_COUNT_LIMIT; ++i) {
                    MyBlock block = blockQueue.poll(3, TimeUnit.SECONDS);
                    if (block != null) {
                        blocks.add(block);
                    }
                }
                if (!blocks.isEmpty()) {
                    // 归并
                    blocks = SortUtil.myMergeSort(blocks);
                    BufferHolder.getIns().commit(blocks);
                } else {
                    // 3s 没有数据直接认为进入下一个阶段
                    break;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    void commit(MyBlock block) {
        try {
            blockQueue.put(block);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    boolean isFinish() {
        return this.isFinish;
    }

}
