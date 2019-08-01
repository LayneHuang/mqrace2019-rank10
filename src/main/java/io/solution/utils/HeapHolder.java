package io.solution.utils;

import io.openmessaging.Message;
import io.solution.GlobalParams;

import java.util.ArrayList;
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
        return ins == null ? ins = new HeapHolder() : ins;
    }

    private int getIndex(long threadId) {
        if (indexMap.containsKey(threadId)) {
            return indexMap.get(threadId);
        } else {
            synchronized (HEAP_CREATE_LOCK) {
                PriorityQueue<Message> queue = new PriorityQueue<>((o1, o2) -> {
                    int res = -Long.compare(o1.getT(), o2.getT());
                    return res == 0 ? -Long.compare(o1.getT(), o2.getT()) : -res;
                });
                int index = heaps.size();
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
        if (queue.size() >= GlobalParams.getQueueLimit()) {

        }
    }

}
