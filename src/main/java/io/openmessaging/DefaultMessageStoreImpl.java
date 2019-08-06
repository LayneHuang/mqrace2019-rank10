package io.openmessaging;

import io.solution.GlobalParams;
import io.solution.utils.HeapHolder;

import java.util.ArrayList;
import java.util.List;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageStoreImpl extends MessageStore {


    @Override
    public void put(Message message) {
        long theadId = Thread.currentThread().getId();
        // 数据填入优先队列中
        HeapHolder.getIns().put(theadId, message);
        // 检查并提交
        HeapHolder.getIns().checkAndCommit(theadId);
    }

    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        if (!GlobalParams.isStepOneFinished()) {
            HeapHolder.getIns().flush();
        }
        ArrayList<Message> res = new ArrayList<Message>();

        return res;
    }


    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        return 0;
    }

}
