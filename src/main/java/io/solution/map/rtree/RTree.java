package io.solution.map.rtree;

import java.util.ArrayList;
import java.util.Arrays;

public class RTree {
    private Node root;

    public Node getRoot() {
        return root;
    }

    public void setRoot(Node root) {
        this.root = root;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
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

    private int size;
    private int height;
    private int maxChild;

    public RTree(int maxChild) {
        this.maxChild = maxChild;
        this.root = new Node(true, 1, null, new ArrayList<Entry>());
        this.size = 0;
        this.height = 1;
    }

    public void Insert(Rect rect, long sum, long count, int idx) {
        Entry entry = new Entry(rect, sum, count, idx);
        this.insert(entry, 1);
        this.size++;
    }

    private void insert(Entry entry, int level) {
        //1.选择合适的叶子结点
        Node leaf = this.chooseNode(this.root, entry, level);
        leaf.getEntries().add(entry);

        if (entry.getChild() != null) {
            entry.getChild().setParent(leaf);
        }

        //2.若此叶子结点数据数量大于最大值，分裂此叶子结点
        Node split = null;
        if (leaf.getEntries().size() > this.maxChild) {
            split = leaf.spiltNode(maxChild);
        }

        //3.调整颗R树
        ArrayList<Node> res = adjustTree(leaf, split);

        //4.若根节点发生分裂，创建一个新的根节点
        if (res.size() == 2 && res.get(1) != null) {
            height++;
            ArrayList<Entry> list = new ArrayList<Entry>();
            list.add(new Entry(res.get(0).getRect(), res.get(0).getSum(), res.get(0).getCount(), res.get(0)));
            list.add(new Entry(res.get(1).getRect(), res.get(1).getSum(), res.get(1).getCount(), res.get(1)));
            root = new Node(false, height, null, list);
            res.get(0).setParent(root);
            res.get(1).setParent(root);
        }
    }

    private Node chooseNode(Node n, Entry entry, int level) {
        if (n.getLeaf() || level == n.getLevel()) {
            return n;
        }

        //选择插入的子树，遍历，选择扩张面积最小的，若扩张面积相等，选面积最小的
        long minIncreasedArea = Long.MAX_VALUE;
        Entry chosen = n.getEntries().get(0);
        for (Entry e : n.getEntries()) {
            Rect tmp = e.getRect().Add(entry.getRect());
            long d = tmp.getArea() - e.getRect().getArea();
            if ((d == minIncreasedArea && e.getRect().getArea() < chosen.getRect().getArea()) || d < minIncreasedArea) {
                chosen = e;
                minIncreasedArea = d;
            }
        }

        //递归寻找至叶子节点
        return chooseNode(chosen.getChild(), entry, level);
    }

    private ArrayList<Node> adjustTree(Node n, Node nn) {
        //调整到根节点，结束递归
        if (n.getLevel() == this.height) {
            ArrayList<Node> result = new ArrayList<Node>();
            result.add(n);
            result.add(nn);
            return result;
        }

        //调整该节点的数据
        n.getEntry().setRect(n.getRect());
        n.getEntry().setSum(n.getSum());
        n.getEntry().setCount(n.getCount());

        //没发生分裂
        if (nn == null) {
            return adjustTree(n.getParent(), null);
        }


        //发生分裂
        Entry entryNN = new Entry(nn.getRect(), nn.getSum(), nn.getCount(), nn);
        n.getParent().getEntries().add(entryNN);
        Node parent = n.getParent();
        if (parent.getEntries().size() > maxChild) {
            Node split = parent.spiltNode(maxChild);
            return adjustTree(parent, split);
        }
        return adjustTree(n.getParent(), null);
    }

    public AverageResult SearchAverage(Rect searchRange) {
        AverageResult result = new AverageResult();
        searchAverage(this.root, searchRange, result);
        return result;
    }

    private void searchAverage(Node n, Rect searchRange, AverageResult result) {
        for (int i = 0; i < n.getEntries().size(); i++) {
            Entry e = n.getEntries().get(i);
            if (searchRange.disjoint(e.getRect())) {
                continue;
            }

            if (searchRange.contain(e.getRect())) {
                result.addSumAndCnt(e.getSum(), e.getCount());
                continue;
            }

            if (!n.getLeaf()) {
                searchAverage(e.getChild(), searchRange, result);
                continue;
            }
            result.addEntry(e);
        }
    }

    public ArrayList<Entry> Search(Rect searchRange) {
        ArrayList<Entry> result = new ArrayList<Entry>();
        search(this.root, searchRange, result);
        return result;
    }

    private void search(Node n, Rect searchRange, ArrayList<Entry> result) {
        for (int i = 0; i < n.getEntries().size(); i++) {
            Entry e = n.getEntries().get(i);
            if (searchRange.disjoint(e.getRect())) {
                continue;
            }

            if (!n.getLeaf()) {
                search(e.getChild(), searchRange, result);
                continue;
            }

            result.add(e);
        }
    }

    public void PrintTree() {
        root.Print();
    }
}
