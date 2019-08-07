package io.solution.utils;

import io.solution.GlobalParams;
import io.solution.data.BlockInfo;
import io.solution.data.MyBlock;
import io.solution.data.MyPage;
import io.solution.map.MyHash;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
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

    private FileChannel channel;

    private BufferHolder() {
        try {
            Path path = Paths.get("/alidata1/race2019/data");
            boolean isDebug = Boolean.valueOf(System.getProperty("debug", "false"));
            if (isDebug) {
                path = Paths.get(System.getProperty("user.dir"), "/data");
            }
            channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            e.printStackTrace();
        }
        blockQueue = new LinkedBlockingQueue<>((int) GlobalParams.WRITE_COUNT_LIMIT * 4);
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
        System.out.println("BufferHolder write file 开始工作~");
        for (; ; ) {
            try {
                MyBlock block = blockQueue.take();
                BlockInfo blockInfo = new BlockInfo();
                blockInfo.setPosition(channel.position());
                blockInfo.setSquare(block.getMaxT(), block.getMinT(), block.getMaxA(), block.getMinA());
                blockInfo.setAvg(block.getAvg());
                // blockInfo.setAmount(block.getPages().size());
                // 填上索引信息
                MyHash.getIns().insert(blockInfo);
                // 写文件
                for (MyPage page : block.getPages()) {
                    System.out.println("正在写入: " + page.getBuffer().get());
                    channel.write(page.getBuffer());
                }
                executor.execute(() -> MyHash.getIns().insert(blockInfo));
                this.notifyAll();
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
            if (GlobalParams.isStepOneFinished() && BlockHolder.getIns().isFinish()) {
                break;
            }
        }
        try {
            channel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
