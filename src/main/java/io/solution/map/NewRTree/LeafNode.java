package io.solution.map.NewRTree;

import java.util.ArrayList;

public class LeafNode extends  Node{
    private ArrayList<Entry> entries;

    public LeafNode() {}

    public LeafNode(Node parent, ArrayList<Entry> entries) {
        super(true, 1, parent);
        this.entries = entries;
    }


    public  void addEntry(Entry entry){
        if(this.getRect() != null) {
            this.setRect(this.getRect().Add(entry.getRect()));
        } else {
            this.setRect(entry.getRect());
        }
        this.setSum(this.getSum() + entry.getSum());
        this.setCount(this.getCount() + entry.getCount());
        entries.add(entry);
    }

    public void addNode(Node node){

    }

    public Node spiltNode(int maxChild){
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
        this.setRect(lSeed.getRect());
        this.setSum(lSeed.getSum());
        this.setCount(lSeed.getCount());

        LeafNode right = new LeafNode(this.getParent(), new ArrayList<Entry>());
        right.addEntry(rSeed);


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
                this.addEntry(tmp);
            }
            else if(remain.size() + right.entries.size() <= maxChild /2){
                remain.remove(chosen);
                right.addEntry(tmp);
            }
            else {
                remain.remove(chosen);
                Rect lEnlarged = lRect.Add(tmp.getRect());
                Rect rEnlarged = rRect.Add(tmp.getRect());
                long d1 = lEnlarged.getArea() - lRect.getArea();
                long d2 = rEnlarged.getArea() - rRect.getArea();

                if(d1 < d2) {
                    this.addEntry(tmp);
                    continue;
                } else if(d1 > d2) {
                    right.addEntry(tmp);
                    continue;
                }

                if (lRect.getArea() < rRect.getArea()) {
                    this.addEntry(tmp);
                    continue;
                } else if(lRect.getArea() > rRect.getArea())  {
                    right.addEntry(tmp);
                    continue;
                }

                if(this.entries.size() <= right.entries.size()) {
                    this.addEntry(tmp);
                }else{
                    right.addEntry(tmp);
                }
            }
        }
        return right;
    }

    public ArrayList<Node> getChildren() {
        return null;
    }

    public ArrayList<Entry> getEntries() {
        return entries;
    }

    public void setEntries(ArrayList<Entry> entries) {
        this.entries = entries;
    }

    public int getSize() {
        return entries.size();
    }

    public void Reset() {
        Rect r = null;
        long sum = 0;
        int count = 0;

        for(Entry e: entries) {
            r = e.getRect().Add(r);
            sum += e.getSum();
            count += e.getCount();
        }
        this.setRect(r);
        this.setCount(count);
        this.setSum(sum);
    }
}
