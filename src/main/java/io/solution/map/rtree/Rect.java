package io.solution.map.rtree;

public class Rect {
    public long x1,x2,y1,y2;

    public Rect(long x1, long x2, long y1, long y2) {
        this.x1 = x1;
        this.x2 = x2;
        this.y1 = y1;
        this.y2 = y2;
    }

    public long getArea() {
        return (x2 - x1) * (y2 - y1);
    }

    public Rect Add(Rect r) {
        return new Rect(Math.min(x1,r.x1), Math.max(x2,r.x2), Math.min(y1,r.y1), Math.max(y2,r.y2));
    }

    public Boolean contain(Rect r) {
        return x1<=r.x1 && r.x2 <= x2 && y1<=r.y1 && r.y2<=y2;
    }

    public Boolean disjoint(Rect r) {
        return x1>r.x2 || x2<r.x1 || y1>r.y2 || y2<r.y1;
    }

    public Boolean intersect(Rect r) {
        return !disjoint(r);
    }

    @Override
    public String toString() {
        return "Rect{" +
                "minT=" + x1 +
                ", minA=" + y1 +
                ", maxT=" + x2 +
                ", maxA=" + y2 +
                '}';
    }
}
