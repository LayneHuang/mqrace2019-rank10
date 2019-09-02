package io.solution.data;

import io.solution.GlobalParams;

/**
 * @Author: laynehuang
 * @CreatedAt: 2019/9/2 0002
 */
public class HashInfo {

    public LineInfo[] lineInfos;
    public long[] aList;

    public HashInfo() {
        lineInfos = new LineInfo[GlobalParams.A_RANGE];
        aList = new long[GlobalParams.getBlockMessageLimit()];
    }

}
