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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 缓冲
 *
 * @Author: laynehuang
 * @CreatedAt: 2019/8/5 0005
 */
public class BufferHolder {

//    private ReentrantReadWriteLock bufferRWLock;

    private boolean isFinish = false;

    private static BufferHolder ins = new BufferHolder();

    private LinkedBlockingQueue<MyBlock> blockQueue;

//    private ExecutorService executor = Executors.newFixedThreadPool(3);

    private FileChannel channel;


    private BufferHolder() {
//        bufferRWLock = new ReentrantReadWriteLock();
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
        // double check locking
//        if (ins != null) {
//            return ins;
//        } else {
//            synchronized (BufferHolder.class) {
//                if (ins == null) {
//                    ins = new BufferHolder();
//                }
//            }
//            return ins;
//        }
    }

    private int inCount = 0;

    void commit(List<MyBlock> blocks) {
        try {
            for (MyBlock block : blocks) {
                inCount++;
                blockQueue.put(block);
                System.out.println("buffer holder提交块个数:" + inCount);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private int writeCount = 0;

    private void writeFile() {
        System.out.println("BufferHolder write file 开始工作~");
        while (!isFinish) {
            try {
                MyBlock block = blockQueue.poll(1, TimeUnit.SECONDS);
                if (block == null) {
                    // 结束
                    isFinish = true;
                    System.out.println("BufferHolder write file 结束~");
//                    bufferRWLock.writeLock().unlock();
                    break;
                }
                solve(block);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
//            if (blockQueue.isEmpty() && GlobalParams.isStepOneFinished() && BlockHolder.getIns().isFinish()) {
//                System.out.println("BufferHolder write file 结束~");
//                break;
//            }
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

    private long totalWriteMessage = 0;

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
     *
     * @param block
     */
    private void solve(MyBlock block) {
        writeFileLock.lock();
        ByteBuffer buffer = ByteBuffer.allocateDirect(GlobalParams.PAGE_SIZE);
        try {
            writeCount++;
            System.out.println("写入块的个数:" + writeCount + ",块大小:" + block.getSize());
            BlockInfo blockInfo = new BlockInfo();
            long pos = channel.position();
            System.out.println("当前文件位置:" + pos);
            blockInfo.setPosition(pos);
            blockInfo.setSquare(block.getMinT(), block.getMaxT(), block.getMinA(), block.getMaxA());
            blockInfo.setSum(block.getSum());
            blockInfo.setAmount(block.getSize());
            // 写文件
            int messageAmount = 0;
            for (MyPage page : block.getPages()) {
                messageAmount += page.getSize();
                page.writeBuffer(buffer);
                channel.write(buffer);
                buffer.clear();
            }
            totalWriteMessage += messageAmount;
            System.out.println("写入Message个数: " + totalWriteMessage + "(总) " + messageAmount + "(本次)");
            blockInfo.setMessageAmount(messageAmount);
//            executor.execute(() -> MyHash.getIns().insert(blockInfo));
            MyHash.getIns().insert(blockInfo);
            errorCheck(block, blockInfo);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            writeFileLock.unlock();
        }
    }

    private void errorCheck(MyBlock block, BlockInfo blockInfo) {
        List<Message> messages =
                HelpUtil.readMessages(
                        blockInfo.getPosition(),
                        block.getSize() * GlobalParams.PAGE_SIZE
                );

        List<Message> originMessage = new ArrayList<>();
        long originSum = 0;
        for (MyPage page : block.getPages()) {
            originMessage.addAll(page.getMessages());
            for (Message message : page.getMessages()) {
                originSum += message.getA();
            }
        }

        if (messages.size() != originMessage.size()) {
            System.out.println("写后读不一致:" + messages.size() + "," + originMessage.size());
        }

        for (int i = 0; i < messages.size(); ++i) {
            if (
                    messages.get(i).getT() != originMessage.get(i).getT()
                            || messages.get(i).getA() != originMessage.get(i).getA()
            ) {
                System.out.println(
                        "Message内容不一致:" + i + "(下标) " +
                                "原:" + originMessage.get(i).getT() + "(t) " + originMessage.get(i).getA() + "(a) " +
                                "读:" + messages.get(i).getT() + "(t) " + messages.get(i).getA() + "(a) "
                );
                 break;
            }
        }

        if (originSum != blockInfo.getSum()) {
            System.out.println("写后块内和不一致:" + originSum + " " + blockInfo.getSum());
        }
    }

}
