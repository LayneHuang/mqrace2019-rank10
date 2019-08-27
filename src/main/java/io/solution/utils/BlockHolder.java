package io.solution.utils;

import io.openmessaging.Message;
import io.solution.GlobalParams;
import io.solution.data.MyBlock;
import io.solution.map.MyHash;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.*;

/**
 * @Author: laynehuang
 * @CreatedAt: 2019/8/2 0002
 */
public class BlockHolder {

    private boolean isFinish = false;

    private LinkedBlockingQueue<Integer> lockQueue;

    // 双缓冲队列
    private BlockingQueue<Message> msgQueue1;
    private BlockingQueue<Message> msgQueue2;

    private BlockingQueue<Message> readerQue;
    private BlockingQueue<Message> writerQue;

    private static BlockHolder ins = new BlockHolder();


    private BlockHolder() {
//        indexMap = new ConcurrentHashMap<>();
        lockQueue = new LinkedBlockingQueue<>(GlobalParams.MSG_BLOCK_QUEUE_LIMIT);

        readerQue = msgQueue1 = new PriorityBlockingQueue<>(
                GlobalParams.MSG_BLOCK_QUEUE_LIMIT,
                Comparator.comparingLong(t0 -> t0.getT())
        );
        writerQue = msgQueue2 = new PriorityBlockingQueue<>(
                GlobalParams.MSG_BLOCK_QUEUE_LIMIT,
                Comparator.comparingLong(t0 -> t0.getT())
        );

        Thread workThread = new Thread(this::work);
        workThread.setName("BLOCK-HOLDER-THREAD");
        workThread.start();

    }

    public static BlockHolder getIns() {
        return ins;
    }

    private void myPut(Message message) {
        try {
            lockQueue.put(0);
            writerQue.put(message);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private Message myPoll() {
        if (readerQue.isEmpty()) {
            BlockingQueue<Message> temp = readerQue;
            readerQue = writerQue;
            writerQue = temp;
            writerQue.clear();
        }
        Message message = null;
        try {
            message = readerQue.poll(5, TimeUnit.SECONDS);
            if (message != null) {
                lockQueue.poll();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return message;
    }

    private int queueSize() {
        return readerQue.size() + writerQue.size();
    }

    public long commitWaitTime = 0;

    public long waitTime = 0;

    private void work() {
        System.out.println("Block holder worker 开始工作~");
        while (!isFinish) {
            if (queueSize() > (GlobalParams.MSG_BLOCK_QUEUE_LIMIT / 2)) {
                MyBlock block = new MyBlock();
                for (int i = 0; i < GlobalParams.getBlockMessageLimit(); ++i) {
                    long s0 = System.nanoTime();
                    Message message = myPoll();
                    waitTime += System.nanoTime() - s0;
                    if (message != null) {
                        block.addMessage(message);
                    }
                }
                if (block.getMessageAmount() > 0) {
                    BufferHolder.getIns().commit(block);
                }
            }
        }
        System.out.println("Block holder worker 结束工作~");
    }

    public void commit(Message message) {
        long s0 = System.nanoTime();
        myPut(message);
        commitWaitTime += System.nanoTime() - s0;
    }

    public synchronized void flush() {
        if (GlobalParams.isStepOneFinished()) {
            return;
        }
        GlobalParams.setStepOneFinished();

        isFinish = true;
        Message[] messages = new Message[GlobalParams.getBlockMessageLimit()];
        int msgSize = 0;

        while (readerQue.size() > 0) {
            Message message = readerQue.poll();
            if (message != null) {
                messages[msgSize++] = message;
                if (msgSize == GlobalParams.getBlockMessageLimit()) {
                    MyBlock block = new MyBlock();
                    block.addMessages(messages, msgSize);
                    BufferHolder.getIns().commit(block);
                    msgSize = 0;
                }
            }
        }

        while (writerQue.size() > 0) {
            Message message = writerQue.poll();
            if (message != null) {
                messages[msgSize++] = message;
                if (msgSize == GlobalParams.getBlockMessageLimit()) {
                    MyBlock block = new MyBlock();
                    block.addMessages(messages, msgSize);
                    BufferHolder.getIns().commit(block);
                    msgSize = 0;
                }
            }
        }

        if (msgSize > 0) {
            MyBlock block = new MyBlock();
            block.addMessages(messages, msgSize);
            BufferHolder.getIns().commit(block);
        }

        BufferHolder.getIns().flush();

        MyHash.getIns().check();
        System.out.println("block info size:" + MyHash.getIns().size + " limit:" + GlobalParams.getBlockInfoLimit());
        System.out.println("块最大消息数:" + MyHash.getIns().maxMsgAmount);
        System.out.println("total msg:" + MyHash.getIns().totalMsg + " exchange count:" + MyHash.getIns().exchangeCount);
        System.out.println("Rest memory:" + Runtime.getRuntime().freeMemory() / (1024 * 1024) + "(M)");
    }

}
