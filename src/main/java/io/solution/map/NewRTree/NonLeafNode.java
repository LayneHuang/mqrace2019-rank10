package io.solution.map.NewRTree;

import java.util.ArrayList;

public class NonLeafNode extends Node{
    private ArrayList<Node> children;

    public NonLeafNode(){}

    public NonLeafNode(int level, ArrayList<Node> children) {
        super(false, level, null);
        this.children = children;
    }

    public NonLeafNode(int level, Node parent, ArrayList<Node> children) {
        super(false, level, parent);
        this.children = children;
    }

    public NonLeafNode(int level, Node parent, ArrayList<Node> children, Rect r, long sum, int count) {
        super(false, level, parent,sum,count,r);
        this.children = children;
    }

    public ArrayList<Node> getChildren() {
        return children;
    }

    public void setChildren(ArrayList<Node> children) {
        this.children = children;
    }

    public ArrayList<Entry> getEntries() {
        return null;
    }

    public  void addEntry(Entry entry){
    }

    public  void addNode(Node node){
        if(this.getRect() != null) {
            this.setRect(this.getRect().Add(node.getRect()));
        }else {
            this.setRect(node.getRect());
        }
        this.setSum(this.getSum() + node.getSum());
        this.setCount(this.getCount() + node.getCount());
        this.children.add(node);
        node.setParent(this);
    }

    public  Node spiltNode(int maxChild){
        int l = 0,r = 1;
        long maxWasted = -1;
        for(int i = 0; i < children.size(); i++) {
            for(int j = i + 1; j < children.size(); j++){
                Rect rect = children.get(i).getRect().Add(children.get(j).getRect());
                long d = rect.getArea() - children.get(i).getRect().getArea() - children.get(j).getRect().getArea();
                if(d > maxWasted) {
                    maxWasted = d;
                    l = i;
                    r = j;
                }
            }
        }

        Node lSeed  = children.get(l), rSeed = children.get(r);

        ArrayList<Node> remain = new ArrayList<Node>();
        for(int i = 0; i < children.size(); i++) {
            if(i != l && i != r) {
                remain.add(children.get(i));
            }
        }

        this.children.clear();
        this.children.add(lSeed);
        this.setRect(lSeed.getRect());
        this.setSum(lSeed.getSum());
        this.setCount(lSeed.getCount());

        NonLeafNode right = new NonLeafNode(this.getLevel() ,this.getParent(), new ArrayList<Node>());
        right.addNode(rSeed);

        while(remain.size() > 0) {
            long maxDiff = -1;
            Rect lRect = this.getRect();
            Rect rRect = right.getRect();
            int chosen = 0;

            for (int i = 0;i < remain.size(); i++) {
                long d1 = lRect.Add(remain.get(i).getRect()).getArea() - lRect.getArea();
                long d2 = rRect.Add(remain.get(i).getRect()).getArea() - rRect.getArea();
                long d = Math.abs(d1 - d2);
                if(d > maxDiff) {
                    maxDiff = d;
                    chosen = i;
                }
            }

            Node tmp = remain.get(chosen);
            if(remain.size() + this.getSize() <= maxChild /2) {
                remain.remove(chosen);
                this.addNode(tmp);
            }
            else if(remain.size() + right.getSize() <= maxChild /2){
                remain.remove(chosen);
                right.addNode(tmp);
            }
            else {
                remain.remove(chosen);
                Rect lEnlarged = lRect.Add(tmp.getRect());
                Rect rEnlarged = rRect.Add(tmp.getRect());
                long d1 = lEnlarged.getArea() - lRect.getArea();
                long d2 = rEnlarged.getArea() - rRect.getArea();

                if(d1 < d2) {
                    this.addNode(tmp);
                    continue;
                } else if(d1 > d2) {
                    right.addNode(tmp);
                    continue;
                }

                if (lRect.getArea() < rRect.getArea()) {
                    this.addNode(tmp);
                    continue;
                } else if(lRect.getArea() > rRect.getArea())  {
                    right.addNode(tmp);
                    continue;
                }

                if(this.getSize() <= right.getSize()) {
                    this.addNode(tmp);
                }else{
                    right.addNode(tmp);
                }
            }
        }

        return right;
    }

    public int getSize() {
        return children.size();
    }

    public void Reset() {
        Rect r = null;
        long sum = 0;
        int count = 0;

        for(Node e: children) {
            r = e.getRect().Add(r);
            sum += e.getSum();
            count += e.getCount();
        }
        this.setRect(r);
        this.setCount(count);
        this.setSum(sum);
    }
}
