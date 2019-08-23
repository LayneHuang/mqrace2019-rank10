package io.solution.map.rtree;

public class Entry {
    public Rect rect;
    public long sum;
    public int count;
    public long posB;
    public long posT;
    public long posA;


    public Entry() {
    }

    public Entry(Rect r, long sum, int count, long posT, long postA, long posB) {
        this.rect = r;
        this.sum = sum;
        this.count = count;
        this.posA = postA;
        this.posB = posB;
        this.posT = posT;
    }


    public Rect getRect() {
        return rect;
    }

    public void setRect(Rect rect) {
        this.rect = rect;
    }

    public long getSum() {
        return sum;
    }

    public void setSum(long sum) {
        this.sum = sum;
    }

    public int getCount() {
        return count;
    }

}
