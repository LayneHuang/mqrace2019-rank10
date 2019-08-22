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
    private FileChannel channelBody;

//    private ExecutorService executor = Executors.newFixedThreadPool(3);

    // 线程安全
    private List<MyBlock> blocks = new ArrayList<>();

    private BufferHolder() {
        try {
            Path path = GlobalParams.getPath(false);
            channelA = FileChannel.open(
                    path,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND
            );

            Path pathBody = GlobalParams.getPath(true);
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

//    private int inCount = 0;

    void commit(List<MyBlock> blocks) {
        try {
            for (MyBlock block : blocks) {
//                inCount++;
//                block.sortByA();
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
                for (int i = 0; i < GlobalParams.WRITE_COUNT_LIMIT; ++i) {
                    MyBlock block = blockQueue.poll(1, TimeUnit.SECONDS);
                    if (block != null) {
                        writeFileLock.lock();
                        blocks.add(block);
                        writeFileLock.unlock();
                    }
                }

                if (blocks.isEmpty() || isFinish) {
                    // 结束
                    isFinish = true;
                    System.out.println("BufferHolder write file 结束~");
                    break;
                } else {
                    solve();
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

    private ReentrantLock writeFileLock = new ReentrantLock();

    void flush() {
        System.out.println("BufferHolder flush");
        while (!blockQueue.isEmpty()) {
            MyBlock block = blockQueue.poll();
            if (block != null) {
                writeFileLock.lock();
                blocks.add(block);
                writeFileLock.unlock();
            }
        }
        if (!blocks.isEmpty()) {
            solve();
        }
    }


    /**
     * 写操作
     */

//    private int outCount = 0;
    private void solve() {
        writeFileLock.lock();

        if (blocks.isEmpty()) {
            writeFileLock.unlock();
            return;
        }

        ByteBuffer buffer = ByteBuffer.allocate(
                GlobalParams.getBodySize() * GlobalParams.getBlockMessageLimit() * blocks.size()
        );

        ByteBuffer aBuffer = ByteBuffer.allocate(
                8 * GlobalParams.getBlockMessageLimit() * blocks.size()
        );

//        outCount += blocks.size();
//        System.out.println(inCount + ", " + outCount);
//        System.out.println(blocks.size());

        try {
            long posBody = channelBody.position();
            long posA = channelA.position();
            // 第几块
            for (MyBlock block : blocks) {
                BlockInfo blockInfo = new BlockInfo();
                blockInfo.initBlockInfo(block, posBody, posA);
                MyHash.getIns().insert(blockInfo);
                // checkError(block, blockInfo);
                for (int i = 0; i < block.getMessageAmount(); ++i) {
                    aBuffer.putLong(block.getMessages()[i].getA());
                    buffer.put(block.getMessages()[i].getBody());
                }
                posBody += GlobalParams.getBodySize() * block.getMessageAmount();
                posA += 8 * block.getMessageAmount();
            }

            // 写文件
            buffer.flip();
            channelBody.write(buffer);
            buffer.clear();

            aBuffer.flip();
            channelA.write(aBuffer);
            aBuffer.clear();

            blocks.clear();

        } catch (IOException e) {
            e.printStackTrace();
        }
        writeFileLock.unlock();
    }

}
