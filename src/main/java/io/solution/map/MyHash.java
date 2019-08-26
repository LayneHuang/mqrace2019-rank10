package io.solution.map;

import io.openmessaging.Message;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: laynehuang
 * @CreatedAt: 2019/8/6 0006
 */
public class MyHash {

    private static MyHash ins = new MyHash();

    private MyHash() {

    }

    public static MyHash getIns() {
        return ins;
    }


    public List<Message> easyFind2(long minT, long maxT, long minA, long maxA) {
        List<Message> res = new ArrayList<>();
        return res;
    }

    public long easyFind3(long minT, long maxT, long minA, long maxA) {
        long res = 0;
        return res;
    }


}
