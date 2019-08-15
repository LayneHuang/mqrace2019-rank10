package io.solution.utils;

import io.openmessaging.Message;
import io.solution.GlobalParams;
import io.solution.data.MyBlock;
import io.solution.map.MyHash;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author: laynehuang
 * @CreatedAt: 2019/8/1 0001
 */

// 单例
public class HeapHolder {

    private static final String HEAP_CREATE_LOCK = "HEAP_CREATE_LOCK";

    /**
     * 线程映射heaps idx
     */
    private ConcurrentHashMap<Long, Integer> indexMap;

    /**
     * 各自线程所使用的堆
     */
    private ArrayList<Queue<Message>> heaps;

    private static HeapHolder ins = new HeapHolder();

    private HeapHolder() {
        indexMap = new ConcurrentHashMap<>();
        heaps = new ArrayList<>();
    }

    public static HeapHolder getIns() {
        return ins;
    }

    private int getIndex(long threadId) {
        if (indexMap.containsKey(threadId)) {
            return indexMap.get(threadId);
        } else {
            synchronized (HEAP_CREATE_LOCK) {
                if (indexMap.containsKey(threadId)) {
                    return indexMap.get(threadId);
                }
                Queue<Message> queue = new LinkedList<>();
                int index = heaps.size();
                indexMap.put(threadId, index);
                heaps.add(queue);
                return index;
            }
        }
    }

    public void put(long threadId, Message message) {
        int index = getIndex(threadId);
        Queue<Message> queue = heaps.get(index);
        queue.add(message);
    }

    public void checkAndCommit(long threadId) {
        int index = getIndex(threadId);
        Queue<Message> queue = heaps.get(index);
        // 组合成页，然后进行提交到缓冲池当中
        if (queue.size() >= GlobalParams.getBlockMessageLimit()) {
            MyBlock block = new MyBlock();
            Message[] messages = new Message[GlobalParams.getBlockMessageLimit()];
            int messageAmount = 0;
            for (int i = 0; i < GlobalParams.getBlockMessageLimit(); ++i) {
                if (!queue.isEmpty()) {
                    Message message = queue.poll();
                    messages[messageAmount++] = message;
                }
            }
            block.addMessages(messages, messageAmount);
            BlockHolder.getIns().commit(block);
        }
    }

    /**
     * 第二阶段开始前处理未落盘数据
     */
    public void flush() {
        if (GlobalParams.isStepOneFinished()) {
            return;
        }
        synchronized (this) {
            if (!GlobalParams.isStepOneFinished()) {
                System.out.println("heap holder flush");
                Message[] messages = new Message[GlobalParams.getBlockMessageLimit()];
                int messageAmount = 0;
                for (Queue<Message> queue : heaps) {
                    while (!queue.isEmpty()) {
                        Message message = queue.poll();
                        messages[messageAmount++] = message;
                        if (messageAmount >= GlobalParams.getBlockMessageLimit()) {
                            MyBlock block = new MyBlock();
                            block.addMessages(messages, messageAmount);
                            BlockHolder.getIns().commit(block);
                            messageAmount = 0;
                        }
                    }
                }
                if (messageAmount > 0) {
                    MyBlock block = new MyBlock();
                    block.addMessages(messages, messageAmount);
                    BlockHolder.getIns().commit(block);
                }
                BlockHolder.getIns().flush();
                BufferHolder.getIns().flush();
                // 打印个块的信息
                // MyHash.getIns().showEachInfo();

                // 清空
                heaps = null;
                indexMap.clear();
                indexMap = null;
                System.gc();
                MyHash.getIns().showAllBlockInfo();
                System.out.println("BlockInfo的Size:" + MyHash.getIns().getSize() + ",");
                System.out.println("jvm GC~~ rest memory:" + Runtime.getRuntime().freeMemory() / (1024 * 1024) + "(M)");
                GlobalParams.setStepOneFinished();
            }
        }
    }

}
