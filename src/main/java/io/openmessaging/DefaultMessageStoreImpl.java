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


    private int cnt = 0;
    private long cost = 0;

    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        long threadId = Thread.currentThread().getId();
        if (!GlobalParams.isStepOneFinished()) {
            AyscBufferHolder.getIns().flush(threadId);
        }
        long s0 = System.nanoTime();
        List<Message> res = MyHash.getIns().find2(tMin, tMax, aMin, aMax);
        long s1 = System.nanoTime();

        if (MyHash.getIns().getIndex(threadId) == 0) {
            cnt++;
            cost += (s1 - s0);
            if (cnt % (GlobalParams.IS_DEBUG ? 50 : 500) == 0) {
                System.out.println("单线程解决" + cnt + "个查询耗时:" + cost);
            }
        }

        return res;
    }

    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
//        long s0 = System.currentTimeMillis();
//        long res = MyHash.getIns().easyFind3(tMin, tMax, aMin, aMax);
//        System.out.println("res :" + res + " " + (System.currentTimeMillis() - s0));
//        return res;
        return MyHash.getIns().easyFind3(tMin, tMax, aMin, aMax);
//        return MyHash.getIns().easyFind3Aysc(tMin, tMax, aMin, aMax);
    }

}
