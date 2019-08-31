package io.openmessaging;

import io.solution.GlobalParams;
import io.solution.map.MyHash;
import io.solution.utils.AyscBufferHolder;

import java.util.List;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageStoreImpl extends MessageStore {


    @Override
    public void put(Message message) {
        long threadId = Thread.currentThread().getId();
        AyscBufferHolder.getIns().commit(threadId, message);
    }

    private long s0 = 0;
    private int[] cnt = new int[GlobalParams.MAX_THREAD_AMOUNT];
    private long cost = 0;

    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        long threadId = Thread.currentThread().getId();
        if (!GlobalParams.isStepOneFinished()) {
            s0 = System.currentTimeMillis();
            AyscBufferHolder.getIns().flush(threadId);
        }
//        long s0 = System.nanoTime();
//        List<Message> res = MyHash.getIns().find2(tMin, tMax, aMin, aMax);
//        long s1 = System.nanoTime();
//        int idx = MyHash.getIns().getIndex(threadId);
//        cnt[idx]++;
//        if (idx == 0) {
//            cost += (s1 - s0);
//            if (cnt[idx] % (GlobalParams.IS_DEBUG ? 50 : 500) == 0) {
//                System.out.println("单线程解决" + cnt + "个查询耗时:" + cost);
//            }
//        }
//
//        return res;
        return MyHash.getIns().find2(tMin, tMax, aMin, aMax);
    }

    private boolean f = false;

    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        if (!f) {
            f = true;
            System.out.println("第二阶段耗时:" + (System.currentTimeMillis() - s0));
        }
//        long s0 = System.currentTimeMillis();
//        long res = MyHash.getIns().easyFind3(tMin, tMax, aMin, aMax);
//        System.out.println("res :" + res + " " + (System.currentTimeMillis() - s0));
//        return res;
//        return MyHash.getIns().easyFind3(tMin, tMax, aMin, aMax);
        return MyHash.getIns().easyFind3Aysc(tMin, tMax, aMin, aMax);
    }

}
