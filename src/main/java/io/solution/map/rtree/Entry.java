package io.solution.map.rtree;

public class Entry {
    private Rect rect;
    private long sum;
    private long count;
    private Node child;
    private int idx;

    Entry(Rect rect, long sum, long count, int idx) {
        this.rect = rect;
        this.sum = sum;
        this.count = count;
        this.child = null;
        this.idx = idx;
    }

    Entry(Rect rect, long sum, long count, Node child) {
        this.rect = rect;
        this.sum = sum;
        this.count = count;
        this.child = child;
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

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public Node getChild() {
        return child;
    }

    public void setChild(Node child) {
        this.child = child;
    }

    public int getIdx() {
        return idx;
    }

    public void setIdx(int idx) {
        this.idx = idx;
    }

    public void show() {
        System.out.println(rect.x1 + " " + rect.x2 + " " + rect.y1 + " " + rect.y2);
    }
}
