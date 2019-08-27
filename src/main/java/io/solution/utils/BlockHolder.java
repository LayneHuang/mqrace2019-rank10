package io.solution.utils;

import io.openmessaging.Message;
import io.solution.GlobalParams;
import io.solution.data.MyBlock;
import io.solution.data.MyMsg;
import io.solution.map.MyHash;

import java.util.Comparator;
import java.util.concurrent.*;

/**
 * @Author: laynehuang
 * @CreatedAt: 2019/8/2 0002
 */
public class BlockHolder {

    private boolean isFinish = false;


    // 双缓冲队列
    private BlockingQueue<MyMsg> readerQue;
    private BlockingQueue<MyMsg> writerQue;

    // 线程信号量
    private Semaphore[] semaphores = new Semaphore[20];

    private static BlockHolder ins = new BlockHolder();

    private BlockHolder() {
        indexMap = new ConcurrentHashMap<>();

        readerQue = new PriorityBlockingQueue<>(
                GlobalParams.MSG_BLOCK_QUEUE_LIMIT,
                Comparator.comparingLong(t0 -> t0.msg.getT())
        );
        writerQue = new PriorityBlockingQueue<>(
                GlobalParams.MSG_BLOCK_QUEUE_LIMIT,
                Comparator.comparingLong(t0 -> t0.msg.getT())
        );

        Thread workThread = new Thread(this::work);
        workThread.setName("BLOCK-HOLDER-THREAD");
        workThread.start();

    }

    public static BlockHolder getIns() {
        return ins;
    }

    private Message myPoll() {
        if (readerQue.isEmpty()) {
            BlockingQueue<MyMsg> temp = readerQue;
            readerQue = writerQue;
            writerQue = temp;
        }
        MyMsg myMsg = null;
        try {
            myMsg = readerQue.poll(5, TimeUnit.SECONDS);
            if (myMsg != null) {
                semaphores[myMsg.idx].release();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return myMsg == null ? null : myMsg.msg;
    }

    private int queueSize() {
        return readerQue.size() + writerQue.size();
    }

    public long commitWaitTime = 0;

    public long waitTime = 0;

    private void work() {
        System.out.println("Block holder worker 开始工作~");
        while (!isFinish) {
            if (queueSize() > GlobalParams.MSG_BLOCK_QUEUE_LIMIT / 2) {
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

    public void commit(long threadId, Message message) {
        long s0 = System.nanoTime();
        try {
            int idx = getIndex(threadId);
            semaphores[idx].acquire();
            writerQue.put(new MyMsg(message, idx));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
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
            Message message = readerQue.poll().msg;
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
            Message message = writerQue.poll().msg;
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

//        long totalMsg = 0;
//        for (int i = 0; i < total; ++i) {
//            totalMsg += msgAmount[i];
//        }

        System.out.println("block info size:" + MyHash.getIns().size + " limit:" + GlobalParams.getBlockInfoLimit());
        System.out.println("BlockHolder提交等待时间:" + commitWaitTime);
        System.out.println("BlockHolder队列取等待时间:" + waitTime);
        System.out.println("BufferHolder写文件队列取等待时间:" + BufferHolder.getIns().waitTime);
        System.out.println("块最大消息数:" + MyHash.getIns().maxMsgAmount);
        System.out.println("块最大消息数:" + MyHash.getIns().maxMsgAmount);
        System.out.println("块合并次数:" + MyHash.getIns().exchangeCost);
//        System.out.println("接收消息数:" + totalMsg);
        System.out.println("插入消息数:" + MyHash.getIns().totalMsg + " exchange count:" + MyHash.getIns().exchangeCount);
        System.out.println("Rest memory:" + Runtime.getRuntime().freeMemory() / (1024 * 1024) + "(M)");

    }

    private int total = 0;

    private ConcurrentHashMap<Long, Integer> indexMap;

    private static final String HEAP_CREATE_LOCK = "HEAP_CREATE_LOCK";

//    public int[] msgAmount = new int[20];
//
//    public void addCount(long threadId) {
//        int id = getIndex(threadId);
//        msgAmount[id]++;
//    }

    private int getIndex(long threadId) {
        if (indexMap.containsKey(threadId)) {
            return indexMap.get(threadId);
        } else {
            synchronized (HEAP_CREATE_LOCK) {
                if (indexMap.containsKey(threadId)) {
                    return indexMap.get(threadId);
                }
                int index = total++;
//                msgAmount[index] = 0;
                semaphores[index] = new Semaphore(GlobalParams.MSG_BLOCK_QUEUE_LIMIT / 12);
                indexMap.put(threadId, index);
                return index;
            }
        }
    }
}
