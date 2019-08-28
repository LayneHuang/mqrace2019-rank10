package io.openmessaging;

import io.solution.GlobalParams;
import io.solution.map.MyHash;
import io.solution.utils.BlockHolder;

import java.util.List;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageStoreImpl extends MessageStore {


    @Override
    public void put(Message message) {
        long threadId = Thread.currentThread().getId();
        BlockHolder.getIns().commit(threadId , message);
    }

    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        long threadId = Thread.currentThread().getId();
        if (!GlobalParams.isStepOneFinished()) {
            BlockHolder.getIns().flush(threadId);
        }
        return MyHash.getIns().easyFind2(tMin, tMax, aMin, aMax);
    }

    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        return MyHash.getIns().find3(tMin, tMax, aMin, aMax);
    }

}
