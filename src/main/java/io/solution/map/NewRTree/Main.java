package io.solution.map.NewRTree;

public class Main {

    public static void main(String[] args) {
        RTree rTree = new RTree(4);
        long s = System.currentTimeMillis();
        for(int i = 1; i <= 10_000_000; i ++) {
            Rect r = new Rect(i,i+1,i,i+1);
            rTree.Insert(r, i,i,i,i,i);
        }
        long e = System.currentTimeMillis();
        System.out.println(e-s + "ms");
    }
}
