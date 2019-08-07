package io.solution.map;

import io.solution.data.BlockInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: laynehuang
 * @CreatedAt: 2019/8/6 0006
 */
public class MyHash {

    private static MyHash ins;

    List<BlockInfo> all;

    private MyHash() {
        all = new ArrayList<>();
    }

    static public MyHash getIns() {
        // double check locking
        if (ins != null) {
            return ins;
        } else {
            synchronized (MyHash.class) {
                if (ins == null) {
                    ins = new MyHash();
                }
            }
            return ins;
        }
    }

    public synchronized void insert(BlockInfo info) {
        all.add(info);
    }

}
