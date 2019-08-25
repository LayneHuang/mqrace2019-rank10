package io.solution.data;

public class SortMessage {
    // public int inB;     // 所在块
    public int idx;     // 下标
    public long a;      // a值
    public SortMessage( int idx , long a) {
       // this.inB = inB;
        this.idx = idx;
        this.a = a;
    }
}
