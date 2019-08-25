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

    private static BlockHolder ins = new BlockHolder();

    private BlockHolder() {
        blockQueue = new LinkedBlockingQueue<>(GlobalParams.BLOCK_COUNT_LIMIT);
        Thread workThread = new Thread(this::work);
        workThread.setName("BLOCK-HOLDER-THREAD");
        workThread.start();
    }

    static BlockHolder getIns() {
        return ins;
    }

    private void work() {
//        System.out.println("Block holder worker 开始工作~");
        while (!isFinish) {
            List<MyBlock> blocks = new ArrayList<>();
            for (int i = 0; i < GlobalParams.BLOCK_COMMIT_COUNT_LIMIT; ++i) {
                MyBlock block = blockQueue.poll();
                if (block != null) {
                    blocks.add(block);
                }
            }

            if (!blocks.isEmpty()) {
                // 归并
                blocks = SortUtil.sortByA(blocks);
                BufferHolder.getIns().commit(blocks);
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

    void flush() {
//        System.out.println("block holder flush");
        List<MyBlock> blocks = new ArrayList<>();
        while (!blockQueue.isEmpty()) {
            try {
                MyBlock block = blockQueue.poll(2, TimeUnit.SECONDS);
                if (block != null) {
                    blocks.add(block);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        if (!blocks.isEmpty()) {
            // 归并
            blocks = SortUtil.sortByA(blocks);
            BufferHolder.getIns().commit(blocks);
        }
        isFinish = true;
    }

}
