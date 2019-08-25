package io.solution.utils;

import io.solution.GlobalParams;
import io.solution.data.MyBlock;
import io.solution.map.MyHash;

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
            8 * GlobalParams.getBlockMessageLimit() * GlobalParams.WRITE_COMMIT_COUNT_LIMIT
    );

    /**
     * 总文件偏移量
     */
    private long totalPosT;
    private long totalPosA;
    private long totalPosB;

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

            totalPosT = channelT.position();
            totalPosA = channelA.position();
            totalPosB = channelB.position();


        } catch (IOException e) {
            e.printStackTrace();
        }
        blockQueue = new LinkedBlockingQueue<>(GlobalParams.WRITE_COUNT_LIMIT);
        Thread workThread = new Thread(this::writeFile);
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

    private void writeFile() {
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

    /**
     * 写操作
     */

    private synchronized void solve(MyBlock block) {

        try {

            MyHash.getIns().easyInsert(block, totalPosT, totalPosA, totalPosB);
            // 写文件
            for (int i = 0; i < block.getMessageAmount(); ++i) {
                tBuffer.putLong(block.getMessages()[i].getT());
                aBuffer.putLong(block.getMessages()[i].getA());
                bBuffer.put(block.getMessages()[i].getBody());
            }

            totalPosT += 8 * block.getMessageAmount();
            totalPosA += 8 * block.getMessageAmount();
            totalPosB += GlobalParams.getBodySize() * block.getMessageAmount();

            if (bBuffer.position() == bBuffer.capacity()) {

                tBuffer.flip();
                channelT.write(tBuffer);
                tBuffer.clear();

                aBuffer.flip();
                channelA.write(aBuffer);
                aBuffer.clear();

                bBuffer.flip();
                channelB.write(bBuffer);
                bBuffer.clear();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
