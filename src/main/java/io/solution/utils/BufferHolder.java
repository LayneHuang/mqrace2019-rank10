package io.solution.utils;

import io.solution.GlobalParams;
import io.solution.data.BlockInfo;
import io.solution.data.MyBlock;
import io.solution.map.MyHash;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

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
    private FileChannel channelBody;

    private ByteBuffer buffer = ByteBuffer.allocateDirect(
            GlobalParams.getBodySize() * GlobalParams.getBlockMessageLimit()
    );

    private ByteBuffer aBuffer = ByteBuffer.allocateDirect(
            8 * GlobalParams.getBlockMessageLimit()
    );

    private ByteBuffer tBuffer = ByteBuffer.allocateDirect(
            8 * GlobalParams.getBlockMessageLimit()
    );

    private ExecutorService executor = Executors.newFixedThreadPool(2);

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
            channelBody = FileChannel.open(
                    pathBody,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND
            );

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
                    System.out.println("BufferHolder write file 结束~");
                    break;
                } else {
                    solve(block);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        try {
            if (channelA != null) {
                channelA.close();
                channelA = null;
            }
            if (channelBody != null) {
                channelBody.close();
                channelBody = null;
            }
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
    }

    /**
     * 写操作
     */

    private synchronized void solve(MyBlock block) {

        try {
            long posBody = channelBody.position();
            long posA = channelA.position();
            long posT = channelT.position();

            executor.execute(() -> {
                BlockInfo blockInfo = new BlockInfo();
                long s = System.currentTimeMillis();
                blockInfo.initBlockInfo(block, posT, posA, posBody);
                long e = System.currentTimeMillis();
                System.out.println("build block rtree used " + (e - s) + "ms"
                        + "(minT,MaxT,minA,maxA): (" + block.getMinT() + "," + block.getMaxT() + "," + block.getMinA() + "," + block.getMaxA() + ")");
                MyHash.getIns().insert(blockInfo);
                // checkError(block, blockInfo);
            });

            // 写文件
            for (int i = 0; i < block.getMessageAmount(); ++i) {
                aBuffer.putLong(block.getMessages()[i].getA());
                tBuffer.putLong(block.getMessages()[i].getT());
                buffer.put(block.getMessages()[i].getBody());
            }

            buffer.flip();
            channelBody.write(buffer);
            buffer.clear();

            aBuffer.flip();
            channelA.write(aBuffer);
            aBuffer.clear();

            tBuffer.flip();
            channelT.write(tBuffer);
            tBuffer.clear();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
