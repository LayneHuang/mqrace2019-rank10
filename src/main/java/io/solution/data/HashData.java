package io.solution.data;

import io.solution.GlobalParams;
import io.solution.utils.HashUtil;

import java.io.IOException;

/**
 * @Author: laynehuang
 * @CreatedAt: 2019/8/30 0030
 */
public class HashData {

    private long t;
    public byte[] dataT;
    public int dataSize;
    public int size;

    public HashData() {
        dataSize = GlobalParams.getBlockMessageLimit() + 30;
        dataT = new byte[dataSize];
    }

    public long[] readT() {
        long[] res = new long[GlobalParams.getBlockMessageLimit()];
        res[0] = t;
        MyCursor cursor = new MyCursor();
        for (int i = 1; i < size; ++i) {
            try {
                int diff = HashUtil.readT(this, cursor);
                res[i] = res[i - 1] + diff;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return res;
    }

    public void encode(long[] tList, int size) {
        this.size = size;
        t = tList[0];
        long pre = t;
        int pos = 0;
        for (int i = 1; i < size; ++i) {
            int diff = (int) (tList[i] - pre);
            pos += HashUtil.encodeInt(diff, this, pos);
            pre = tList[i];
        }
    }

}
