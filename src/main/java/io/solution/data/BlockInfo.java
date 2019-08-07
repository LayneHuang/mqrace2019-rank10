package io.solution.data;

/**
 * @Author: laynehuang
 * @CreatedAt: 2019/8/6 0006
 */
public class BlockInfo {

    private long maxT;
    private long minT;
    private long maxA;
    private long minA;
    private long amount;
    private long avg;
    private long position;

    public long getPosition() {
        return position;
    }

    public void setPosition(long position) {
        this.position = position;
    }

    public void setSquare(long maxT, long minT, long maxA, long minA) {
        this.maxA = maxA;
        this.maxT = maxT;
        this.minA = minA;
        this.minT = minT;
    }


    public void setAmount(long amount) {
        this.amount = amount;
    }

    public void setAvg(long avg) {
        this.avg = avg;
    }
}
