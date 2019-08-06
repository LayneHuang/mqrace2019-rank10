package io.solution.map;

import io.solution.data.BlockInfo;

/**
 * @Author: laynehuang
 * @CreatedAt: 2019/8/6 0006
 */
public class MyHash {

    private static MyHash ins;

    private MyHash() {
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

    }

}
