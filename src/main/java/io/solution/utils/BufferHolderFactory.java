package io.solution.utils;

import io.solution.GlobalParams;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author: laynehuang
 * @CreatedAt: 2019/9/3 0003
 */
public class BufferHolderFactory {

    private static int total = 0;

    private static HashMap<Long, BufferHolder> mp = new HashMap<>();

    static double[] wLines = new double[GlobalParams.A_RANGE];

    private static final String HEAP_CREATE_LOCK = "HEAP_CREATE_LOCK";

    public static BufferHolder getBufferHolder(long id) {
        if (mp.containsKey(id)) {
            return mp.get(id);
        } else {
            synchronized (HEAP_CREATE_LOCK) {
                if (mp.containsKey(id)) {
                    return mp.get(id);
                }
                BufferHolder b = new BufferHolder(total);
                mp.put(id, b);
                total++;
                return b;
            }
        }
    }

    public static synchronized void flush() {
        if (GlobalParams.isStepOneFinished()) {
            return;
        }

        for (BufferHolder blockHolder : mp.values()) {
            blockHolder.flush();
            for (int i = 0; i < GlobalParams.A_RANGE; ++i) {
                wLines[i] += blockHolder.wLines[i];
            }
        }
        if (total == 0) {
            System.out.println("GG~");
        } else {
            for (int i = 0; i < GlobalParams.A_RANGE; ++i) {
                wLines[i] /= total;
            }
//            System.out.print("[");
//            for (int i = 0; i < GlobalParams.A_RANGE; ++i) System.out.print(String.format("%.2f", wLines[i]) + ",");
//            System.out.println("]");
        }
        PretreatmentHolder.getIns().work();
        GlobalParams.setStepOneFinished();
    }

}
