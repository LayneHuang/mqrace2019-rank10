package io.solution.data;

import io.openmessaging.Message;
import io.solution.GlobalParams;
import io.solution.utils.HashUtil;

import java.io.IOException;

/**
 * @Author: laynehuang
 * @CreatedAt: 2019/8/21 0021
 */
public class PageInfo {

    private int limitT = GlobalParams.getPageMessageCount() + 100;

    private long sum;
    private long maxT;
    private long minT;
    private long maxA;
    private long minA;

    private int messageAmount;

    // body 在文件中的偏移位置
    private long position;

    // a 在文件中的偏移位置
    private long positionA;

    private long beginT;        // 第一个t值
    private byte[] dataT;       // t的压缩数据
    private int sizeT;          // t的压缩数据占用的位
    private int tPosition;      // t在buffer中的偏移位置

    PageInfo() {
        dataT = new byte[limitT];
    }

    void addMessages(
            Message[] messages,
            int size,
            long position,
            long positionA
    ) {
        this.messageAmount = size;
        this.position = position;
        this.positionA = positionA;
        long lastT = 0;
        int posT = 0;
        for (int i = 0; i < size; ++i) {
            long nowA = messages[i].getA();
            long nowT = messages[i].getT();
            sum += nowA;
            if (i == 0) {
                lastT = beginT = minT = maxT = nowT;
                maxA = minA = nowA;
            } else {
                minT = Math.min(minT, nowT);
                maxT = Math.max(maxT, nowT);
                minA = Math.min(minA, nowA);
                maxA = Math.max(maxA, nowA);

                int tDiff = (int) (nowT - lastT);
                posT += HashUtil.encodeInt(tDiff, this, posT);
                lastT = nowT;
            }
        }
    }

    public long[] readPageT() {
        long[] res = new long[messageAmount];
        long pre = beginT;
        res[0] = pre;
        MyCursor cursor = new MyCursor();
        cursor.setPos(tPosition);
        for (int i = 1; i < messageAmount; ++i) {
            try {
                int tDiff = HashUtil.readIntT(cursor);
                pre += tDiff;
                res[i] = pre;
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ArrayIndexOutOfBoundsException e) {
                System.out.println("当前i : " + i + "," + "sizeT:" + sizeT + " limitT:" + limitT);
                e.printStackTrace();
            }
        }
        return res;
    }

    public int getLimitT() {
        return limitT;
    }

    public void setLimitT(int limitT) {
        this.limitT = limitT;
    }

    public byte[] getDataT() {
        return dataT;
    }

    public void setDataT(byte[] dataT) {
        this.dataT = dataT;
    }

    public int getSizeT() {
        return sizeT;
    }

    public void setSizeT(int sizeT) {
        this.sizeT = sizeT;
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

    public long getPosition() {
        return position;
    }

    public void setPosition(long position) {
        this.position = position;
    }

    public long getPositionA() {
        return positionA;
    }

    public void setPositionA(long positionA) {
        this.positionA = positionA;
    }

    public void settPosition(int tPosition) {
        this.tPosition = tPosition;
    }
}
