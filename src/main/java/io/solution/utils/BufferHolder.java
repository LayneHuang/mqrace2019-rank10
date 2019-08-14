package io.solution.utils;

import io.solution.GlobalParams;
import io.solution.data.BlockInfo;
import io.solution.data.MyBlock;
import io.solution.data.MyPage;
import io.solution.map.MyHash;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.LinkedList;
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

    private FileChannel channel;

//    private ExecutorService executor = Executors.newFixedThreadPool(3);

    // 线程安全
    private List<MyBlock> blocks = Collections.synchronizedList(new LinkedList<>());

    private BufferHolder() {
        try {
            Path path = GlobalParams.getPath();
            channel = FileChannel.open(
                    path,
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
                blockQueue.put(block);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void writeFile() {
        System.out.println("BufferHolder write file 开始工作~");
        while (!isFinish) {
            try {
                for (int i = 0; i < GlobalParams.WRITE_COUNT_LIMIT; ++i) {
                    MyBlock block = blockQueue.poll(1, TimeUnit.SECONDS);
                    if (block != null) {
                        blocks.add(block);
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
            if (channel != null) {
                channel.close();
                channel = null;
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
                blocks.add(block);
            }
        }
        if (!blocks.isEmpty()) {
            solve();
        }
    }

    private ReentrantLock writeFileLock = new ReentrantLock();

    /**
     * 写操作
     */

//    private int outCount = 0;
    private void solve() {
        writeFileLock.lock();
        ByteBuffer buffer = ByteBuffer.allocate(
                GlobalParams.BLOCK_SIZE * blocks.size()
        );
//        outCount += blocks.size();
//        System.out.println(inCount + ", " + outCount);
//        System.out.println(blocks.size());
        try {
            long pos = channel.position();
            // 第几块
            for (MyBlock block : blocks) {
                long nPos = pos;

//                executor.execute(() -> {
                BlockInfo blockInfo = new BlockInfo();
                blockInfo.setPosition(nPos);
                blockInfo.initBlockInfo(block);
                MyHash.getIns().insert(blockInfo);
//                });

                for (int i = 0; i < block.getPageAmount(); ++i) {
                    MyPage page = block.getPages()[i];
                    for (int j = 0; j < page.getMessageAmount(); ++j) {
                        buffer.put(page.getMessages()[j].getBody());
                        pos += GlobalParams.getBodySize();
                    }
                }
            }

            // 写文件
            buffer.flip();
            channel.write(buffer);
            buffer.clear();
            blocks.clear();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            writeFileLock.unlock();
        }
    }

}
