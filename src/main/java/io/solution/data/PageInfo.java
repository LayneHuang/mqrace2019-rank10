package io.solution.data;

import io.openmessaging.Message;

/**
 * @Author: laynehuang
 * @CreatedAt: 2019/8/21 0021
 */
public class PageInfo {

    private long sum;
    private long maxT;
    private long minT;
    private long maxA;
    private long minA;

    private int messageAmount;

    // body 在文件中的偏移位置
    private long positionB;

    // a 在文件中的偏移位置
    private long positionA;

    // t 在文件中的偏移位置
    private long positionT;

    void addMessages(
            Message[] messages,
            int size,
            long positionT,
            long positionA,
            long positionB
    ) {
        this.messageAmount = size;
        this.positionT = positionT;
        this.positionA = positionA;
        this.positionB = positionB;

        for (int i = 0; i < size; ++i) {
            long nowA = messages[i].getA();
            long nowT = messages[i].getT();
            sum += nowA;
            if (i == 0) {
                minT = maxT = nowT;
                maxA = minA = nowA;
            } else {
                minT = Math.min(minT, nowT);
                maxT = Math.max(maxT, nowT);
                minA = Math.min(minA, nowA);
                maxA = Math.max(maxA, nowA);
            }
        }
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

    public long getSum() {
        return sum;
    }

    public void setSum(long sum) {
        this.sum = sum;
    }

    public int getMessageAmount() {
        return messageAmount;
    }

    public void setMessageAmount(int messageAmount) {
        this.messageAmount = messageAmount;
    }

    public long getPositionB() {
        return positionB;
    }

    public void setPositionB(long positionB) {
        this.positionB = positionB;
    }

    public long getPositionA() {
        return positionA;
    }

    public void setPositionA(long positionA) {
        this.positionA = positionA;
    }

    public long getPositionT() {
        return positionT;
    }

    public void setPositionT(long positionT) {
        this.positionT = positionT;
    }

}
