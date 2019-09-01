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

    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        if (!GlobalParams.isStepOneFinished()) {
            s0 = System.currentTimeMillis();
            AyscBufferHolder.getIns().flush();
        }
        return MyHash.getIns().find2(tMin, tMax, aMin, aMax);
    }

    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        if (!GlobalParams.isStepTwoFinished()) {
            System.out.println("第二阶段耗时:" + (System.currentTimeMillis() - s0));
            MyHash.getIns().cleanStepTwoInfo();
        }
        return MyHash.getIns().find3(tMin, tMax, aMin, aMax);
    }

}
