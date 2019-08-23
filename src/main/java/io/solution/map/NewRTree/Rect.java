package io.solution.map.NewRTree;

public class Rect {
    public long minT, maxT, minA, maxA;

    public Rect(long minT, long maxT, long minA, long maxA) {
        this.minT = minT;
        this.maxT = maxT;
        this.minA = minA;
        this.maxA = maxA;
    }

    public long getArea() {
        return (maxT - minT) * (maxA - minA);
    }

    public Rect Add(Rect r) {
        if (r == null) {
            return new Rect(minT, maxT, minA, maxA);
        }
        return new Rect(Math.min(minT, r.minT), Math.max(maxT, r.maxT), Math.min(minA, r.minA), Math.max(maxA, r.maxA));
    }

    public void Combine(Rect r) {
        this.minT = Math.min(minT, r.minT);
        this.maxT = Math.max(maxT, r.maxT);
        this.minA = Math.min(minA, r.minA);
        this.maxA = Math.max(maxA, r.maxA);
    }

    public Boolean contain(Rect r) {
        return minT <= r.minT && r.maxT <= maxT && minA <= r.minA && r.maxA <= maxA;
    }

    public Boolean disjoint(Rect r) {
        return minT > r.maxT || maxT < r.minT || minA > r.maxA || maxA < r.minA;
    }

    public Boolean intersect(Rect r) {
        return !disjoint(r);
    }

    @Override
    public String toString() {
        return "Rect{" +
                "minT=" + minT +
                ", minA=" + minA +
                ", maxT=" + maxT +
                ", maxA=" + maxA +
                '}';
    }
}
