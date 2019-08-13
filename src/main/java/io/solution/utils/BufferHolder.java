package io.solution.utils;

import io.openmessaging.Message;
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
        blockQueue = new LinkedBlockingQueue<>((int) GlobalParams.getWriteCountLimit() * 4);
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
//                System.out.println("buffer holder提交块个数:" + inCount);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    //    private int writeCount = 0;
    private boolean isFirst = true;

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

//    private long totalWriteMessage = 0;

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
        ByteBuffer buffer = ByteBuffer.allocateDirect(GlobalParams.BLOCK_SIZE);
//        ByteBuffer buffer = ByteBuffer.allocateDirect(GlobalParams.getBodySize());
        try {
//            writeCount++;
//            System.out.println("写入块的个数:" + writeCount + ",块大小:" + block.getPageAmount());
            long pos = channel.position();
//            System.out.println("当前文件位置:" + pos);
            executor.execute(() -> {
                BlockInfo blockInfo = new BlockInfo();
                blockInfo.initBlockInfo(block);
                blockInfo.setPosition(pos);
                MyHash.getIns().insert(blockInfo);
//                if (isFirst) {
//                    isFirst = false;
//                    checkError(block, blockInfo);
//                }
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
//            totalWriteMessage += messageAmount;
//            System.out.println("写入Message个数: " + totalWriteMessage + "(总) " + messageAmount + "(本次)");
//            MyHash.getIns().insert(blockInfo);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            writeFileLock.unlock();
        }
    }


    /**
     * 对比写进的元数据及读出的数据是否一致
     */
    private void checkError(MyBlock block, BlockInfo blockInfo) {

        List<Message> originMessage = new ArrayList<>();
        long originSum = 0;
        for (int i = 0; i < block.getPageAmount(); ++i) {
            MyPage page = block.getPages()[i];
            for (int j = 0; j < page.getMessageAmount(); ++j) {
                Message message = page.getMessages()[j];
                originMessage.add(message);
                originSum += message.getA();
            }
        }

        byte[][] bodys = HelpUtil.readBody(blockInfo.getPosition(), blockInfo.getMessageAmount());

        long[] aList = blockInfo.readBlockA();
        long[] tList = blockInfo.readBlockT();

        for (int i = 0; i < blockInfo.getMessageAmount(); ++i) {
            if (
                    tList[i] != originMessage.get(i).getT()
                            || aList[i] != originMessage.get(i).getA()
                            || (!checkBody(bodys[i], originMessage.get(i).getBody()))

            ) {
                System.out.println(
                        "Message内容不一致:" + i + "(下标) " +
                                "原:" + originMessage.get(i).getT() + "(t) " + originMessage.get(i).getA() + "(a) " +
                                "读:" + tList[i] + "(t) " + aList[i] + "(a) "
                );
//                break;
            }
        }

        if (originSum != blockInfo.getSum()) {
            System.out.println("写后块内和不一致:" + originSum + " " + blockInfo.getSum());
        }
    }

    boolean checkBody(byte[] a, byte[] b) {
        for (int i = 0; i < GlobalParams.getBodySize(); ++i) {
            if (a[i] != b[i]) return false;
        }
        return true;
    }

}
