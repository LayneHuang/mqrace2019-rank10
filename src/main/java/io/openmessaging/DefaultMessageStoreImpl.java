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
        // 数据填入优先队列中
        BlockHolder.getIns().commit(message);
    }

    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        if (!GlobalParams.isStepOneFinished()) {
            BlockHolder.getIns().flush();
        }
//        List<Message> res = MyHash.getIns().easyFind2(tMin, tMax, aMin, aMax);
//        System.out.println("step2");
//        System.out.println("step2: " + res.size());
//        return res;
//        return MyHash.getIns().force2(tMin, tMax, aMin, aMax);
//        return MyHash.getIns().find2(tMin, tMax, aMin, aMax);
        return MyHash.getIns().easyFind2(tMin, tMax, aMin, aMax);
    }

    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
//        long s1 = System.currentTimeMillis();
//        long res = MyHash.getIns().easyFind3(tMin, tMax, aMin, aMax);
//        long s2 = System.currentTimeMillis();
//        System.out.println("step3: " + res + " " + (s2 - s1));
//        return MyHash.getIns().force3(tMin, tMax, aMin, aMax);
//        return res;
        return MyHash.getIns().easyFind3(tMin, tMax, aMin, aMax);
    }

}
