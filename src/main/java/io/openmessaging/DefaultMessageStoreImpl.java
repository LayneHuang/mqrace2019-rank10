package io.openmessaging;

import io.solution.map.MyHash;
import io.solution.utils.HeapHolder;

import java.util.List;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageStoreImpl extends MessageStore {


    @Override
    public void put(Message message) {
//        System.out.println(message.getBody().length);
        long theadId = Thread.currentThread().getId();
        // 数据填入优先队列中
        HeapHolder.getIns().put(theadId, message);
        // 检查并提交
        HeapHolder.getIns().checkAndCommit(theadId);
    }

    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        HeapHolder.getIns().flush();
        List<Message> res = MyHash.getIns().find2(tMin, tMax, aMin, aMax);
        System.out.println("step2: " + res.size());
        return res;
//        return MyHash.getIns().force2(tMin, tMax, aMin, aMax);
//        return MyHash.getIns().find2(tMin, tMax, aMin, aMax);
//        return null;
    }

    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        //        System.out.println("step3: " + res);
//        return MyHash.getIns().force3(tMin, tMax, aMin, aMax);
        return MyHash.getIns().find3(tMin, tMax, aMin, aMax);
    }

}
