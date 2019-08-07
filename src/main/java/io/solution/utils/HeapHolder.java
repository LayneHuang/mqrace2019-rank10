package io.solution.utils;

import io.openmessaging.Message;
import io.solution.GlobalParams;
import io.solution.data.MyBlock;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
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
    private ArrayList<PriorityQueue<Message>> heaps;

    private static HeapHolder ins;

    private HeapHolder() {
        indexMap = new ConcurrentHashMap<>();
        heaps = new ArrayList<>();
    }

    public static HeapHolder getIns() {
        // double check locking
        if (ins != null) {
            return ins;
        } else {
            synchronized (HeapHolder.class) {
                if (ins == null) {
                    ins = new HeapHolder();
                }
            }
            return ins;
        }
    }

    private int getIndex(long threadId) {
        if (indexMap.containsKey(threadId)) {
            return indexMap.get(threadId);
        } else {
            synchronized (HEAP_CREATE_LOCK) {
                if (indexMap.containsKey(threadId)) {
                    return indexMap.get(threadId);
                }
                PriorityQueue<Message> queue = new PriorityQueue<>((o1, o2) -> {
                    int res = -Long.compare(o1.getT(), o2.getT());
                    return res == 0 ? -Long.compare(o1.getT(), o2.getT()) : -res;
                });
                int index = heaps.size();
                indexMap.put(threadId, index);
                heaps.add(queue);
                return index;
            }
        }
    }

    public void put(long threadId, Message message) {
        int index = getIndex(threadId);
        PriorityQueue<Message> queue = heaps.get(index);
        queue.add(message);
    }

    public void checkAndCommit(long threadId) {
        int index = getIndex(threadId);
        PriorityQueue<Message> queue = heaps.get(index);
        // 组合成页，然后进行提交到缓冲池当中
        long addCount = GlobalParams.getQueueLimit();
        if (queue.size() >= addCount) {
            MyBlock block = new MyBlock();
            List<Message> messages = new ArrayList<>();
            for (int i = 0; i < addCount; ++i) {
                if (!queue.isEmpty()) {
                    messages.add(queue.poll());
                }
            }
            block.addMessages(messages);
            BlockHolder.getIns().commit(block);
            System.out.println("commit a block~");
        }
    }

    /**
     * 第二阶段开始前处理未落盘数据
     */
    public synchronized void flush() {
        if (GlobalParams.isStepOneFinished()) {
            return;
        }

        List<Message> messages = new ArrayList<>();
        for (PriorityQueue<Message> queue : heaps) {
            while (!queue.isEmpty()) {
                Message message = queue.poll();
                messages.add(message);
                if (messages.size() >= GlobalParams.getQueueLimit()) {
                    MyBlock block = new MyBlock();
                    block.addMessages(messages);
                    BlockHolder.getIns().commit(block);
                    messages.clear();
                }
            }
        }

        if (!messages.isEmpty()) {
            MyBlock block = new MyBlock();
            block.addMessages(messages);
            BlockHolder.getIns().commit(block);
        }
        GlobalParams.setStepOneFinished();
    }

}
