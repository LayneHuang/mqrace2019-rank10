package io.solution.map.rtree;

import java.util.ArrayList;

public abstract class Node {
    private long sum;
    private int count;
    private int level;
    private Rect rect;
    private boolean isLeaf;
    private Node parent;

    public Node() {}

    public Node(boolean isLeaf, int level, Node parent) {
        this.sum = 0;
        this.count = 0;
        this.rect = null;
        this.isLeaf = isLeaf;
        this.level = level;
        this.parent = parent;
    }

    public  Node(boolean isLeaf, int level, Node parent, long sum, int count, Rect rect) {
        this.sum = sum;
        this.count = count;
        this.rect = rect;
        this.isLeaf = isLeaf;
        this.level = level;
        this.parent = parent;
    }

    public abstract ArrayList<Node> getChildren();

    public abstract ArrayList<Entry> getEntries();

    public abstract void addEntry(Entry entry);

    public abstract void addNode(Node node);

    public abstract Node spiltNode(int maxChild);

    public abstract int getSize();

    public abstract void Reset();

    public int getLevel() {
        return level;
    }

    public void setLevel(int level) {
        this.level = level;
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

    public Rect getRect() {
        return rect;
    }

    public void setRect(Rect rect) {
        this.rect = rect;
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    public void setLeaf(boolean leaf) {
        isLeaf = leaf;
    }

    public Node getParent() {
        return parent;
    }

    public void setParent(Node parent) {
        this.parent = parent;
    }
}
