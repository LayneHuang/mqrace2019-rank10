package io.solution.utils;

import io.solution.GlobalParams;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author: laynehuang
 * @CreatedAt: 2019/9/3 0003
 */
public class BufferHolderFactory {

    private static AtomicInteger total = new AtomicInteger(0);

    private static ConcurrentHashMap<Long, BufferHolder> mp = new ConcurrentHashMap<>();

    static double[] wLines = new double[GlobalParams.A_RANGE];

    public static BufferHolder getBufferHolder(long id) {
        if (mp.containsKey(id)) {
            return mp.get(id);
        } else {
            int idx = total.getAndAdd(1);
            BufferHolder b = new BufferHolder(idx);
            mp.put(id, b);
            return b;
        }
    }

    private static int getSize() {
        return total.get();
    }

    static int cnt = 0;

    public static synchronized void flush() {
        if (GlobalParams.isStepOneFinished()) {
            return;
        }
        cnt++;
        if (cnt > 1) {
            System.out.println("fuck in flush 2~~~  tid:" + Thread.currentThread().getId());
        }
        for (BufferHolder blockHolder : mp.values()) {
            blockHolder.flush();
            for (int i = 0; i < GlobalParams.A_RANGE; ++i) {
                wLines[i] += blockHolder.wLines[i];
            }
        }
        int threadAmount = getSize();
        if (threadAmount == 0) {
            System.out.println("GG~");
        } else {
            for (int i = 0; i < GlobalParams.A_RANGE; ++i) {
                wLines[i] /= threadAmount;
            }
//            System.out.print("[");
//            for (int i = 0; i < GlobalParams.A_RANGE; ++i) System.out.print(String.format("%.2f", wLines[i]) + ",");
//            System.out.println("]");
        }
        PretreatmentHolder.getIns().work();
        GlobalParams.setStepOneFinished();
    }

}
