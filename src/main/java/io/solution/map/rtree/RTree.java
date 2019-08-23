package io.solution.map.rtree;

import java.util.ArrayList;

public class RTree {
    private Node root;
    private int height;
    private int maxChild;

    public RTree() {
        this.root = null;
        this.height = 1;
        this.maxChild = 4;
    }

    public RTree(int maxChild) {
        this.root = null;
        this.height = 1;
        this.maxChild = maxChild;
    }

    public AverageResult SearchAverage(Rect searchRange) {
        AverageResult result = new AverageResult();
        searchAverage(this.root, searchRange, result);
        return result;
    }

    private void searchAverage(Node n, Rect searchRange, AverageResult result) {
        if (searchRange.disjoint(n.getRect()))
            return;

        if (searchRange.contain(n.getRect())) {
            result.addSumAndCnt(n.getSum(), n.getCount());
            return;
        }

        if (n.isLeaf()) {
            ArrayList<Entry> entries = n.getEntries();
            for (Entry e : entries) {
                if (searchRange.disjoint(e.getRect())) {
                    continue;
                }

                if (searchRange.contain(e.getRect())) {
                    result.addSumAndCnt(e.getSum(), e.getCount());
                    continue;
                }

                result.addEntry(e);
            }
        } else {
            ArrayList<Node> nodes = n.getChildren();
            for (Node node : nodes) {
                searchAverage(node, searchRange, result);
            }
        }

    }

    public ArrayList<Entry> Search(Rect searchRange) {
        ArrayList<Entry> result = new ArrayList<Entry>();
        search(this.root, searchRange, result);
        return result;
    }

    private void search(Node n, Rect searchRange, ArrayList<Entry> result) {
        if (n.isLeaf()) {
            ArrayList<Entry> entries = n.getEntries();
            for (Entry e : entries) {
                if (searchRange.disjoint(e.getRect())) {
                    continue;
                }
                result.add(e);
            }
        } else {
            ArrayList<Node> nodes = n.getChildren();
            for (Node node : nodes) {
                if (searchRange.disjoint(node.getRect())) {
                    continue;
                }
                search(node, searchRange, result);
            }
        }
    }


    public void Insert(Rect r, long sum, int count, long posT, long posA, long posB) {
        Entry e = new Entry(r, sum, count, posT, posA, posB);
        if (root == null) {
            root = new LeafNode(null, new ArrayList<Entry>());
            root.addEntry(e);
            return;
        }
        insert(e);
    }

    public void Insert(Entry entry) {
        if (root == null) {
            root = new LeafNode(null, new ArrayList<Entry>());
            root.addEntry(entry);
            return;
        }
        insert(entry);
    }

    private void insert(Entry entry) {
        Node leaf = this.chooseNode(root, entry);
        leaf.addEntry(entry);

        Node split = null;
        if (leaf.getSize() > this.maxChild) {
            split = leaf.spiltNode(this.maxChild);
        }

        ArrayList<Node> res = adjustTree(leaf, split);

        if (res.size() == 2) {
            height++;
            Node l = res.get(0), r = res.get(1);
            root = new NonLeafNode(height, null, res, l.getRect().Add(r.getRect()), l.getSum() + r.getSum(), l.getCount() + r.getCount());
            res.get(0).setParent(root);
            res.get(1).setParent(root);
        }
    }

    private ArrayList<Node> adjustTree(Node n, Node nn) {

        if (n.getLevel() == this.height) {
            ArrayList<Node> result = new ArrayList<Node>();
            result.add(n);
            if (nn != null) {
                result.add(nn);
            }
            return result;
        }
        //没发生分裂
        n.getParent().Reset();
        if (nn == null) {
            return adjustTree(n.getParent(), null);
        }
        //发生分裂
        n.getParent().addNode(nn);
        Node parent = n.getParent();
        if (parent.getSize() > this.maxChild) {
            Node split = parent.spiltNode(maxChild);
            return adjustTree(parent, split);
        }
        return adjustTree(n.getParent(), null);
    }

    private Node chooseNode(Node n, Entry entry) {
        if (n.isLeaf()) {
            return n;
        }

        long minIncArea = Long.MAX_VALUE;
        Node chosen = n.getChildren().get(0);
        for (Node node : n.getChildren()) {
            Rect tmp = node.getRect().Add(entry.getRect());
            long d = tmp.getArea() - node.getRect().getArea();
            if (d < minIncArea || (d == minIncArea && node.getRect().getArea() < chosen.getRect().getArea())) {
                chosen = node;
                minIncArea = d;
            }
        }
        return chooseNode(chosen, entry);
    }

    public Node getRoot() {
        return root;
    }

    public void setRoot(Node root) {
        this.root = root;
    }

    public int getHeight() {
        return height;
    }

    public void setHeight(int height) {
        this.height = height;
    }

    public int getMaxChild() {
        return maxChild;
    }

    public void setMaxChild(int maxChild) {
        this.maxChild = maxChild;
    }
}
