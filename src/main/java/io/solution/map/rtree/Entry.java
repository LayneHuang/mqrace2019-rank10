package io.solution.map.rtree;

public class Entry {
    private Rect rect;
    private Node child;
    private int idx;

    Entry(Rect rect, int idx) {
        this.rect = rect;
        this.child = null;
        this.idx = idx;
    }

    Entry(Rect rect, Node child) {
        this.rect = rect;
        this.child = child;
    }

    public Rect getRect() {
        return rect;
    }

    public void setRect(Rect rect) {
        this.rect = rect;
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
