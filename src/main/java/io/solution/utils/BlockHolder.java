package io.solution.utils;

import io.solution.GlobalParams;
import io.solution.data.MyBlock;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @Author: laynehuang
 * @CreatedAt: 2019/8/2 0002
 */
public class BlockHolder {

    private boolean isFinish = false;

    private LinkedBlockingQueue<Integer> lockQueue;

    private BlockingQueue<MyBlock> readerQue;
    private BlockingQueue<MyBlock> writerQue;

    private static BlockHolder ins = new BlockHolder();

    private BlockHolder() {

        lockQueue = new LinkedBlockingQueue<>(GlobalParams.BLOCK_COUNT_LIMIT);

        readerQue = new PriorityBlockingQueue<>(
                GlobalParams.BLOCK_COUNT_LIMIT,
                Comparator.comparingLong(o -> o.minT)
        );
        writerQue = new PriorityBlockingQueue<>(
                GlobalParams.BLOCK_COUNT_LIMIT,
                Comparator.comparingLong(o -> o.minT)
        );

        Thread workThread = new Thread(this::work);
        workThread.setName("BLOCK-HOLDER-THREAD");
        workThread.start();
    }

    static BlockHolder getIns() {
        return ins;
    }

    void commit(MyBlock block) {
        try {
            lockQueue.put(0);
            writerQue.put(block);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private MyBlock myPoll() {
        if (readerQue.isEmpty()) {
            BlockingQueue<MyBlock> temp = readerQue;
            readerQue = writerQue;
            writerQue = temp;
            writerQue.clear();
        }
        MyBlock block = null;
        try {
            block = readerQue.poll(5, TimeUnit.SECONDS);
            if (block != null) {
                lockQueue.poll();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return block;
    }


    private void work() {
//        System.out.println("Block holder worker 开始工作~");
        while (!isFinish) {
            List<MyBlock> blocks = new ArrayList<>();
            for (int i = 0; i < GlobalParams.BLOCK_COMMIT_COUNT_LIMIT && !isFinish; ++i) {
                MyBlock block = myPoll();
                if (block != null) {
                    blocks.add(block);
                }
            }

            if (!blocks.isEmpty()) {
                // 归并
                blocks = SortUtil.myMergeSort(blocks);
                BufferHolder.getIns().commit(blocks);
                blocks.clear();
            }
        }
    }


    void flush() {
//        System.out.println("block holder flush");
        List<MyBlock> blocks = new ArrayList<>();
        while (readerQue.size() > 0) {
            MyBlock block = readerQue.poll();
            if (block != null) {
                blocks.add(block);
                if (blocks.size() == GlobalParams.BLOCK_COMMIT_COUNT_LIMIT) {
                    blocks = SortUtil.myMergeSort(blocks);
                    BufferHolder.getIns().commit(blocks);
                    blocks.clear();
                }
            }
        }

        while (writerQue.size() > 0) {
            MyBlock block = writerQue.poll();
            if (block != null) {
                blocks.add(block);
                if (blocks.size() == GlobalParams.BLOCK_COMMIT_COUNT_LIMIT) {
                    blocks = SortUtil.myMergeSort(blocks);
                    BufferHolder.getIns().commit(blocks);
                    blocks.clear();
                }
            }
        }
        if (!blocks.isEmpty()) {
            // 归并
            blocks = SortUtil.myMergeSort(blocks);
            BufferHolder.getIns().commit(blocks);
        }
        isFinish = true;
    }

}
