package io.solution.map.rtree;

import java.util.ArrayList;

public class Node {
    private boolean leaf;
    private int level;
    private Node parent;
    private ArrayList<Entry> entries;

    public Node() {}
    public Node(boolean isLeaf, int level, Node parent, ArrayList<Entry> entries) {
        this.leaf = isLeaf;
        this.level = level;
        this.parent = parent;
        this.entries = entries;
    }

    public boolean getLeaf() {
        return leaf;
    }

    public void setLeaf(boolean leaf) {
        this.leaf = leaf;
    }

    public int getLevel() {
        return level;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    public Node getParent() {
        return parent;
    }

    public void setParent(Node parent) {
        this.parent = parent;
    }

    public ArrayList<Entry> getEntries() {
        return entries;
    }

    public void setEntries(ArrayList<Entry> entries) {
        this.entries = entries;
    }

    public Entry getEntry() {
        Entry entry = null;
        for(int i = 0; i < this.parent.getEntries().size(); i ++){
            if(this.parent.getEntries().get(i).getChild() == this) {
                return this.parent.getEntries().get(i);
            }
        }
        return  entry;
    }

    public Rect getRect() {
        Rect rect = entries.get(0).getRect();
        for(int i = 1; i < entries.size(); i++) {
            rect = rect.Add(entries.get(i).getRect());
        }
        return rect;
    }

    public Node spiltNode(int maxChild) {
        int l = 0,r = 1;
        long maxWasted = -1;
        for(int i = 0; i < entries.size(); i++) {
            for(int j = i + 1; j < entries.size(); j++){
                Rect rect = entries.get(i).getRect().Add(entries.get(j).getRect());
                long d = rect.getArea() - entries.get(i).getRect().getArea() - entries.get(j).getRect().getArea();
                if(d > maxWasted) {
                    maxWasted = d;
                    l = i;
                    r = j;
                }
            }
        }

        Entry lSeed  = entries.get(l), rSeed = entries.get(r);

        ArrayList<Entry> remain = new ArrayList<Entry>();
        for(int i = 0; i < entries.size(); i++) {
            if(i != l && i != r) {
                remain.add(entries.get(i));
            }
        }

        this.entries.clear();
        this.entries.add(lSeed);

        Node right = new Node(this.leaf, this.level, this.parent, new ArrayList<Entry>());
        right.entries.add(rSeed);

        if(rSeed.getChild() != null) {
            rSeed.getChild().setParent(right);
        }
        if(lSeed.getChild() != null) {
            lSeed.getChild().setParent(this);
        }


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

            Entry tmp = remain.get(chosen);
            if(remain.size() + this.entries.size() <= maxChild /2) {
                remain.remove(chosen);
                if(tmp.getChild() != null) {
                    tmp.getChild().setParent(this);
                }
                this.entries.add(tmp);
            }
            else if(remain.size() + right.entries.size() <= maxChild /2){
                remain.remove(chosen);
                if(tmp.getChild() != null) {
                    tmp.getChild().setParent(right);
                }
                right.entries.add(tmp);
            }
            else {
                remain.remove(chosen);
                Rect lEnlarged = lRect.Add(tmp.getRect());
                Rect rEnlarged = rRect.Add(tmp.getRect());
                long d1 = lEnlarged.getArea() - lRect.getArea();
                long d2 = rEnlarged.getArea() - rRect.getArea();

                if(d1 < d2) {
                    if(tmp.getChild() != null) {
                        tmp.getChild().setParent(this);
                    }
                    this.entries.add(tmp);
                    continue;
                } else if(d1 > d2) {
                    if(tmp.getChild() != null) {
                        tmp.getChild().setParent(right);
                    }
                    right.entries.add(tmp);
                    continue;
                }

                if (lRect.getArea() < rRect.getArea()) {
                    if(tmp.getChild() != null) {
                        tmp.getChild().setParent(this);
                    }
                    this.entries.add(tmp);
                    continue;
                } else if(lRect.getArea() > rRect.getArea())  {
                    if(tmp.getChild() != null) {
                        tmp.getChild().setParent(right);
                    }
                    right.entries.add(tmp);
                    continue;
                }

                if(this.entries.size() <= right.entries.size()) {
                    if(tmp.getChild() != null) {
                        tmp.getChild().setParent(this);
                    }
                    this.entries.add(tmp);
                }else{
                    if(tmp.getChild() != null) {
                        tmp.getChild().setParent(right);
                    }
                    right.entries.add(tmp);
                }
            }
        }

        return right;
    }


    public void  Print(){
        System.out.println(getRect().toString()+ "level = " + level + " leaf = " +leaf);
        for(int i =0;i <entries.size();i++){
            System.out.printf("%s, ",entries.get(i).getRect().toString());
        }
        System.out.println();
        for(int i =0;i <entries.size();i++){
            if(entries.get(i).getChild() != null)
            entries.get(i).getChild().Print();
        }
    }
}
