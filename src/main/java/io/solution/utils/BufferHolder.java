package io.solution.utils;

import io.solution.GlobalParams;
import io.solution.data.BlockInfo;
import io.solution.data.MyBlock;
import io.solution.map.MyHash;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 缓冲
 *
 * @Author: laynehuang
 * @CreatedAt: 2019/8/5 0005
 */
public class BufferHolder {

    private static BufferHolder ins;

    private LinkedBlockingQueue<MyBlock> blockQueue;

    ExecutorService executor = Executors.newFixedThreadPool(3);

    private BufferHolder() {
        blockQueue = new LinkedBlockingQueue<>(GlobalParams.WRITE_COUNT_LIMIT * 4);
        Thread workThread = new Thread(this::writeFile);
        workThread.setName("BUFFER-HOLDER-THREAD");
        workThread.start();
    }

    static BufferHolder getIns() {
        // double check locking
        if (ins != null) {
            return ins;
        } else {
            synchronized (BufferHolder.class) {
                if (ins == null) {
                    ins = new BufferHolder();
                }
            }
            return ins;
        }
    }

    void commit(List<MyBlock> blocks) throws InterruptedException {
        for (MyBlock block : blocks) {
            blockQueue.put(block);
        }
    }

    private void writeFile() {
        for (; ; ) {
            try {
                MyBlock block = blockQueue.take();
                // Todo : 写文件
                BlockInfo blockInfo = new BlockInfo();
                // Todo : 填上索引信息
                executor.execute(() -> MyHash.getIns().insert(blockInfo));
                this.notifyAll();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (GlobalParams.isStepOneFinished() && BlockHolder.getIns().isFinish()) {
                break;
            }
        }
    }

}
