package io.solution.data;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: laynehuang
 * @CreatedAt: 2019/8/1 0001
 */
public class MyBlock {

    private long minA;
    private long maxA;
    private long minT;
    private long maxT;
    private List<MyPage> pages;

    public MyBlock() {
        pages = new ArrayList<>();
    }

    public List<MyPage> getPages() {
        return this.pages;
    }

    public int getSize() {
        return this.pages.size();
    }

    public long getMaxT() {
        return maxT;
    }

    public void setMaxT(long maxT) {
        this.maxT = maxT;
    }

    public long getMinA() {
        return minA;
    }

    public void setMinA(long minA) {
        this.minA = minA;
    }

    public long getMaxA() {
        return maxA;
    }

    public void setMaxA(long maxA) {
        this.maxA = maxA;
    }

    public long getMinT() {
        return minT;
    }

    public void setMinT(long minT) {
        this.minT = minT;
    }
}
