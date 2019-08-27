package io.solution.utils;

import io.openmessaging.Message;
import io.solution.GlobalParams;
import io.solution.data.MyBlock;
import io.solution.map.MyHash;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 缓冲
 *
 * @Author: laynehuang
 * @CreatedAt: 2019/8/5 0005
 */
public class BufferHolder {

    private boolean isFinish = false;

    private static BufferHolder ins = new BufferHolder();

    private LinkedBlockingQueue<MyBlock> blockQueue;

    private FileChannel channelAT;
    private FileChannel channelB;

    private long totalPosAT;
    private long totalPosB;

    private ByteBuffer bBuffer = ByteBuffer.allocateDirect(
            GlobalParams.getBodySize() * GlobalParams.getBlockMessageLimit() * GlobalParams.WRITE_COMMIT_COUNT_LIMIT
    );

    private ByteBuffer atBuffer = ByteBuffer.allocateDirect(
            16 * GlobalParams.getBlockMessageLimit() * GlobalParams.WRITE_COMMIT_COUNT_LIMIT
    );

    private int nowWriteCount = 0;

    private BufferHolder() {
        try {

            Path pathT = GlobalParams.getPath(0);
            channelAT = FileChannel.open(
                    pathT,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND
            );

            Path pathBody = GlobalParams.getPath(2);
            channelB = FileChannel.open(
                    pathBody,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND
            );
            totalPosAT = channelAT.position();
            totalPosB = channelB.position();

        } catch (IOException e) {
            e.printStackTrace();
        }
        blockQueue = new LinkedBlockingQueue<>(GlobalParams.WRITE_COUNT_LIMIT);
        Thread workThread = new Thread(this::work);
        workThread.setName("BUFFER-HOLDER-THREAD");
        workThread.start();
    }

    public static BufferHolder getIns() {
        return ins;
    }

    void commit(MyBlock block) {
//        System.out.println("buffer 提交 " + block.getMessageAmount() + " " + GlobalParams.getBlockMessageLimit());
        try {
            blockQueue.put(block);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public long waitTime = 0;

    private void work() {
        System.out.println("BufferHolder write file 开始工作~");
        while (!isFinish) {
            try {
                long s0 = System.nanoTime();
                MyBlock block = blockQueue.poll(5, TimeUnit.SECONDS);
                waitTime += System.nanoTime() - s0;
//                System.out.println("buffer 提取" + blockQueue.size());
                if (block == null) {
                    isFinish = true;
                    break;
                } else {
                    solve(block);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        try {
            if (channelAT != null) {
                channelAT.close();
                channelAT = null;
            }
            if (channelB != null) {
                channelB.close();
                channelB = null;
            }
            atBuffer = null;
            bBuffer = null;
            System.out.println("BufferHolder write file 结束~");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void flush() {
        System.out.println("BufferHolder flush");
        while (!blockQueue.isEmpty()) {
            MyBlock block = blockQueue.poll();
            if (block != null) {
                solve(block);
            }
        }

        if (nowWriteCount > 0) {
            nowWriteCount = 0;
            writeFile();
        }
    }

    /**
     * 写操作
     */

    private synchronized void solve(MyBlock block) {

        MyHash.getIns().insert(block, totalPosAT, totalPosB);

        // 写文件
        for (int i = 0; i < block.getMessageAmount(); ++i) {
            Message message = block.getMessages()[i];
            atBuffer.putLong(message.getT());
            atBuffer.putLong(message.getA());
            bBuffer.put(message.getBody());
        }

        totalPosAT += 16 * block.getMessageAmount();
        totalPosB += GlobalParams.getBodySize() * block.getMessageAmount();
        nowWriteCount++;

        if (nowWriteCount == GlobalParams.WRITE_COMMIT_COUNT_LIMIT) {
            nowWriteCount = 0;
            writeFile();
        }
    }

    private synchronized void writeFile() {
//        System.out.println("Write File~");
        try {
            atBuffer.flip();
            channelAT.write(atBuffer);
            atBuffer.clear();

            bBuffer.flip();
            channelB.write(bBuffer);
            bBuffer.clear();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
