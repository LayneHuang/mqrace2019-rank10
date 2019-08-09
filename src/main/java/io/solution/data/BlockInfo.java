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
    private int amount;
    private long sum;
    private long position;
    private long messageAmount;

    public long getSum() {
        return sum;
    }

    public int getAmount() {
        return amount;
    }

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

    public long getMaxT() {
        return maxT;
    }

    public long getMinA() {
        return minA;
    }

    public long getMaxA() {
        return maxA;
    }

    public long getMinT() {
        return minT;
    }

    public void setSum(long sum) {
        this.sum = sum;
    }

    public void show() {
        System.out.println("aL:" + minA + ",aR:" + maxA + ",tL:" + minT + ",tR:" + maxT + ",amount:" + amount + ",sum:" + sum + ",pos:" + position);
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public long getMessageAmount() {
        return messageAmount;
    }

    public void setMessageAmount(long messageAmount) {
        this.messageAmount = messageAmount;
    }
}
