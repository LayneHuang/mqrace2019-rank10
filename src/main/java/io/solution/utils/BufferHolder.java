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

    private ExecutorService executor = Executors.newFixedThreadPool(3);

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
        System.out.println("BufferHolder write file 开始工作~");
        while (!isFinish) {
            try {
                MyBlock block = blockQueue.poll(1, TimeUnit.SECONDS);
                if (block == null) {
                    // 结束
                    isFinish = true;
                    System.out.println("BufferHolder write file 结束~");
                    break;
                }
                solve(block);
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
            try {
                MyBlock block = blockQueue.take();
                solve(block);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private ReentrantLock writeFileLock = new ReentrantLock();

    /**
     * 写操作
     */
    private void solve(MyBlock block) {
        writeFileLock.lock();
        ByteBuffer buffer = ByteBuffer.allocate(
                GlobalParams.BLOCK_SIZE

        );

        try {

            long pos = channel.position();
            executor.execute(() -> {
                BlockInfo blockInfo = new BlockInfo();
                blockInfo.initBlockInfo(block);
                blockInfo.setPosition(pos);
                MyHash.getIns().insert(blockInfo);
            });

            for (int i = 0; i < block.getPageAmount(); ++i) {
                MyPage page = block.getPages()[i];
                for (int j = 0; j < page.getMessageAmount(); ++j) {
                    buffer.put(page.getMessages()[j].getBody());
                }
            }

            // 写文件
            buffer.flip();
            channel.write(buffer);
            buffer.clear();

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            writeFileLock.unlock();
        }
    }

}
