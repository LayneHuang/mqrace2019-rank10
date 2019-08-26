package io.solution.utils;

import io.openmessaging.Message;
import io.solution.GlobalParams;
import io.solution.data.MyBlock;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 缓冲
 *
 * @Author: laynehuang
 * @CreatedAt: 2019/8/5 0005
 */
class BufferHolder {

    private boolean isFinish = false;

    private static BufferHolder ins = new BufferHolder();

    private LinkedBlockingQueue<MyBlock> blockQueue;

    private FileChannel channelA;
    private FileChannel channelT;
    private FileChannel channelB;

    private ByteBuffer bBuffer = ByteBuffer.allocateDirect(
            GlobalParams.getBodySize() * GlobalParams.getBlockMessageLimit() * GlobalParams.WRITE_COMMIT_COUNT_LIMIT
    );

    private ByteBuffer aBuffer = ByteBuffer.allocateDirect(
            8 * GlobalParams.getBlockMessageLimit() * GlobalParams.WRITE_COMMIT_COUNT_LIMIT
    );

    private ByteBuffer tBuffer = ByteBuffer.allocateDirect(
            16 * GlobalParams.getBlockMessageLimit() * GlobalParams.WRITE_COMMIT_COUNT_LIMIT
    );


    private BufferHolder() {
        try {

            Path pathT = GlobalParams.getPath(0);
            channelT = FileChannel.open(
                    pathT,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND
            );

            Path pathA = GlobalParams.getPath(1);
            channelA = FileChannel.open(
                    pathA,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND
            );

            Path pathBody = GlobalParams.getPath(2);
            channelB = FileChannel.open(
                    pathBody,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND
            );


        } catch (IOException e) {
            e.printStackTrace();
        }
        blockQueue = new LinkedBlockingQueue<>(GlobalParams.WRITE_COUNT_LIMIT);
        Thread workThread = new Thread(this::work);
        workThread.setName("BUFFER-HOLDER-THREAD");
        workThread.start();
    }

    static BufferHolder getIns() {
        return ins;
    }

    void commit(List<MyBlock> blocks) {
        try {
            for (MyBlock block : blocks) {
                blockQueue.put(block);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void work() {
//        System.out.println("BufferHolder write file 开始工作~");
        while (!isFinish) {
            try {
                MyBlock block = blockQueue.poll(5, TimeUnit.SECONDS);
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
            if (channelT != null) {
                channelT.close();
                channelT = null;
            }
            if (channelA != null) {
                channelA.close();
                channelA = null;
            }
            if (channelB != null) {
                channelB.close();
                channelB = null;
            }
            aBuffer = null;
            tBuffer = null;
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

        if (bBuffer.position() > 0) {
            writeFile();
        }

    }

    /**
     * 写操作
     */

    private synchronized void solve(MyBlock block) {

        // 写文件
        for (int i = 0; i < block.getMessageAmount(); ++i) {
            Message message = block.getMessages()[i];
            tBuffer.putLong(message.getT());
            aBuffer.putLong(message.getA());
            bBuffer.put(message.getBody());
        }

        totalPosT += 8 * block.getMessageAmount();
        totalPosA += 8 * block.getMessageAmount();
        totalPosB += GlobalParams.getBodySize() * block.getMessageAmount();
        if (bBuffer.position() == bBuffer.limit()) {
            writeFile();
        }
    }

    private synchronized void writeFile() {
        try {
            tBuffer.flip();
            channelT.write(tBuffer);
            tBuffer.clear();

            aBuffer.flip();
            channelA.write(aBuffer);
            aBuffer.clear();

            bBuffer.flip();
            channelB.write(bBuffer);
            bBuffer.clear();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
