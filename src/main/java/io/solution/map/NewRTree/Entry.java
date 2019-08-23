package io.solution.map.NewRTree;

public class Entry {
    private Rect rect;
    private long sum;
    private int count;
    private int posB;
    private int posT;
    private int posA;


    public Entry() {}

    public Entry(Rect r, long sum, int count,  int posT, int postA,int posB) {
        this.rect = r;
        this.sum = sum;
        this.count = count;
        this.posA =postA;
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

    public void setCount(int count) {
        this.count = count;
    }

    public int getPosB() {
        return posB;
    }

    public void setPosB(int posB) {
        this.posB = posB;
    }

    public int getPosT() {
        return posT;
    }

    public void setPosT(int posT) {
        this.posT = posT;
    }

    public int getPosA() {
        return posA;
    }

    public void setPosA(int posA) {
        this.posA = posA;
    }
}
