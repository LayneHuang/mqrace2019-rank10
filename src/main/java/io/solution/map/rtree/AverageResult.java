package io.solution.map.rtree;

import java.util.ArrayList;

public class AverageResult {
    private long sum;
    private long cnt;
    private ArrayList<Entry> result;

    public AverageResult() {
        sum = 0;
        cnt = 0;
        result = new ArrayList<Entry>();
    }

    public long getSum() {
        return sum;
    }

    public void setSum(long sum) {
        this.sum = sum;
    }

    public long getCnt() {
        return cnt;
    }

    public void setCnt(long cnt) {
        this.cnt = cnt;
    }

    public ArrayList<Entry> getResult() {
        return result;
    }

    public void setResult(ArrayList<Entry> result) {
        this.result = result;
    }

    public void addSumAndCnt(long sum, long cnt) {
        this.sum += sum;
        this.cnt += cnt;
    }

    public void addEntry(Entry e) {
        this.result.add(e);
    }
}