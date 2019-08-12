package io.solution.data;

import io.openmessaging.Message;
import io.solution.GlobalParams;
import io.solution.utils.HashUtil;

import java.io.IOException;
import java.util.List;

/**
 * @Author: laynehuang
 * @CreatedAt: 2019/8/6 0006
 */
public class BlockInfo {

    private int limitA = GlobalParams.getBlockMessageLimit() + 200;
    private int limitT = GlobalParams.getBlockMessageLimit() + 200;

    private long maxT;
    private long minT;
    private long maxA;
    private long minA;
    private int amount;

    // 偏移值的和
    private long sum;

    private long position;
    private int messageAmount;

    // 块内A是递增的
    private long beginA;        // 第一个a值
    private byte[] dataA;       // a的压缩数据
    // private int posA;           // a的压缩数据游标位置   Todo:注意线程安全
    private int sizeA;          // a的压缩数据占用的位

    private long beginT;        // 第一个t值
    private byte[] dataT;       // t的压缩数据
    // private int posT;           // t的压缩数据游标位置
    private int sizeT;          // t的压缩数据占用的位

    public BlockInfo() {
        dataA = new byte[limitA];
        dataT = new byte[limitT];
    }

    /**
     * block info 赋值
     * 调用前必须先设置 消息数量
     */
    public void initBlockInfo(MyBlock block) {

        setSquare(block.getMinT(), block.getMaxT(), block.getMinA(), block.getMaxA());
        messageAmount = block.getMessageAmount();
        sum = block.getSum();
        amount = block.getPageAmount();

        // 初始化
        sizeA = sizeT = 0;
        // flip();
        boolean isFirst = true;
        long lastA = 0;
        long lastT = 0;
        int posA = 0;
        int posT = 0;

        for (int i = 0; i < block.getPageAmount(); ++i) {
            MyPage page = block.getPages()[i];
            for (int j = 0; j < page.getMessageAmount(); ++j) {
                Message message = page.getMessages()[j];
                if (isFirst) {
                    lastA = beginA = message.getA();
                    lastT = beginT = message.getT();
                    isFirst = false;
                } else {
                    long nowA = message.getA();
                    long nowT = message.getT();
                    int aDiff = (int) (nowA - lastA);
                    // a
                    posA += HashUtil.encodeInt(aDiff, this, false, posA);
                    // t
                    int tDiff = (int) (nowT - lastT);
                    posT += HashUtil.encodeInt(tDiff, this, true, posT);
                    lastA = nowA;
                    lastT = nowT;
                }
            }
        }
    }

    /**
     * 获取所有Message a的值
     */
    public long[] readBlockA() {
        long[] res = new long[messageAmount];
        long pre = beginA;
        res[0] = pre;
        MyCursor cursor = new MyCursor();
        for (int i = 1; i < messageAmount; ++i) {
            try {
                long aDiff = HashUtil.readInt(this, false, cursor);
                pre += aDiff;
                res[i] = pre;
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ArrayIndexOutOfBoundsException e) {
                System.out.println("当前i : " + i + "," + "sizeA:" + sizeA + " limitA:" + limitA);
                e.printStackTrace();
            }
        }
        return res;
    }

    /**
     * 获取所有Message t的值
     */
    public long[] readBlockT() {
        long[] res = new long[messageAmount];
        long pre = beginT;
        res[0] = pre;
//        posT = 0;
        MyCursor cursor = new MyCursor();
        for (int i = 1; i < messageAmount; ++i) {
            try {
                int tDiff = HashUtil.readInt(this, true, cursor);
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

    // Todo : 获取block的所有内容
    public List<Message> readBlock() {
        return null;
    }

//    public void flip() {
//        posA = 0;
//        posT = 0;
//    }
//
//    public void flipA() {
//        posA = 0;
//    }
//
//    public void flipT() {
//        posA = 0;
//    }

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

    private void setSquare(long minT, long maxT, long minA, long maxA) {
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

    private void setSum(long sum) {
        this.sum = sum;
    }

    public void show() {
        System.out.println(
                "aL:" + minA +
                        ",aR:" + maxA +
                        ",tL:" + minT +
                        ",tR:" + maxT +
                        ",amount:" + amount +
                        ",sum:" + sum +
                        ",pos:" + position +
                        ",aDiff: " + (maxA - minA) +
                        ",tDiff: " + (maxT - minT)
        );
    }

    private void setAmount(int amount) {
        this.amount = amount;
    }

    public int getMessageAmount() {
        return messageAmount;
    }

    public void setMessageAmount(int messageAmount) {
        this.messageAmount = messageAmount;
    }

    public byte[] getDataA() {
        return dataA;
    }

    public void setDataA(byte[] dataA) {
        this.dataA = dataA;
    }

    public byte[] getDataT() {
        return dataT;
    }

    public void setDataT(byte[] dataT) {
        this.dataT = dataT;
    }

    public int getSizeA() {
        return sizeA;
    }

    public void setSizeA(int sizeA) {
        this.sizeA = sizeA;
    }

    public int getSizeT() {
        return sizeT;
    }

    public void setSizeT(int sizeT) {
        this.sizeT = sizeT;
    }

    public int getLimitT() {
        return limitT;
    }

    public void setLimitT(int limit) {
        this.limitT = limit;
    }

    public int getLimitA() {
        return limitA;
    }

    public void setLimitA(int limit) {
        this.limitA = limit;
    }


}
