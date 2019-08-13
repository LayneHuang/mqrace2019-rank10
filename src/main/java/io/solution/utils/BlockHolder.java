package io.solution.utils;

import io.solution.GlobalParams;
import io.solution.data.MyBlock;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

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

//    private int outCount = 0;

    private void work() {
        System.out.println("Block holder worker 开始工作~");
        while (!isFinish) {
            List<MyBlock> blocks = new ArrayList<>();
            for (int i = 0; i < GlobalParams.BLOCK_COUNT_LIMIT; ++i) {
                MyBlock block = blockQueue.poll();
                if (block != null) {
//                    outCount++;
                    blocks.add(block);
//                    System.out.println("取出块个数:" + outCount);
                }
            }

            if (!blocks.isEmpty()) {
                // 归并
                // System.out.println("归并块个数:" + blocks.size());
                // blocks = SortUtil.myMergeSort(blocks);
                BufferHolder.getIns().commit(blocks);
            }
        }
    }

    void commit(MyBlock block) {
        try {
//            System.out.println("提交块,块大小:" + block.getPageAmount());
            // block.showSquare();
            blockQueue.put(block);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    void flush() {
        System.out.println("block holder flush");
        List<MyBlock> blocks = new ArrayList<>();
        while (!blockQueue.isEmpty()) {
            MyBlock block = null;
            try {
                block = blockQueue.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (block != null) {
//                outCount++;
//                System.out.println("block holder flush 取出块个数:" + outCount);
                blocks.add(block);
            }
        }
        if (!blocks.isEmpty()) {
            // 归并
//            System.out.println("归并块个数:" + blocks.size());
//            blocks = SortUtil.myMergeSort(blocks);
            BufferHolder.getIns().commit(blocks);
        }
        isFinish = true;
    }

}
