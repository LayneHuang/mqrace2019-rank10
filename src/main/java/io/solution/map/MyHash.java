package io.solution.map;

import io.openmessaging.Message;
import io.solution.GlobalParams;
import io.solution.data.BlockInfo;
import io.solution.utils.HelpUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * @Author: laynehuang
 * @CreatedAt: 2019/8/6 0006
 */
public class MyHash {

    private int limit = 2000;

    private static MyHash ins = new MyHash();

    private BlockInfo[] all;

    private int size = 0;

    public int getSize() {
        return size;
    }

    private MyHash() {
        all = new BlockInfo[limit];
    }

    public static MyHash getIns() {
        return ins;
    }

    //private long maxADiff = 0;
    //    private long maxTDiff = 0;
    private double useByteSum = 0;

    public synchronized void insert(BlockInfo info) {
//        System.out.println("插入块的信息:");
//        maxADiff = Math.max(info.getMaxA() - info.getMinA(), maxADiff);
//        maxTDiff = Math.max(info.getMaxT() - info.getMinT(), maxTDiff);
//        System.out.println("最大差值: " + maxADiff + "(a) " + maxTDiff + "(t)");
//        info.show();

        useByteSum += (info.getSizeA() + info.getSizeT());
        System.out.println(
                "插入" + (size + 1) + "块 " +
                        "使用内存:" + String.format("%.6f", useByteSum / 1024 / 1024 / 1024) + "(GB) "
                        + "message数量:" + info.getMessageAmount() + " "
                        + " size:" + info.getSizeA() + "," + info.getSizeT() + " (byte) "
//                        + " limit:" + info.getLimitA() + "," + info.getLimitT() + " (byte) "
//                        " mem:" + Runtime.getRuntime().freeMemory() + " (byte) "
        );

        all[size] = info;
        size++;
        if (size == limit) {
            limit += (size >> 1);
            all = Arrays.copyOf(all, limit);
        }
    }

    public List<Message> find2(long minT, long maxT, long minA, long maxA) {
//        System.out.println("hash list size: " + size);
        List<Message> res = new ArrayList<>();
//        long[] aList = new long[GlobalParams.getBlockMessageLimit()];
//        long[] tList = new long[GlobalParams.getBlockMessageLimit()];
//        byte[][] bodys = new byte[GlobalParams.getBlockMessageLimit()][GlobalParams.getBodySize()];
        for (int i = 0; i < size; ++i) {
            BlockInfo info = all[i];
            if (HelpUtil.intersect(
                    minT, maxT, minA, maxA,
                    info.getMinT(), info.getMaxT(), info.getMinA(), info.getMaxA()
            )) {
//                List<Message> messages = HelpUtil.readMessages(
//                        info.getPosition(),
//                        info.getAmount() * GlobalParams.PAGE_SIZE
//                );
                byte[][] bodys = HelpUtil.readBody(info.getPosition(), info.getMessageAmount());
                long[] aList = info.readBlockA();
                long[] tList = info.readBlockT();
                for (int j = 0; j < info.getMessageAmount(); ++j) {
                    if (
                            HelpUtil.inSide(
                                    tList[j], aList[j],
                                    minT, maxT, minA, maxA
                            )
                    ) {
                        Message message = new Message(aList[j], tList[j], bodys[j]);
                        res.add(message);
                    }
                }
            }
        }

        res.sort(Comparator.comparingLong(Message::getT));
        return res;
    }

    public long find3(long minT, long maxT, long minA, long maxA) {
        long res = 0;
        long messageAmount = 0;
//        long[] aList = new long[GlobalParams.getBlockMessageLimit()];
//        long[] tList = new long[GlobalParams.getBlockMessageLimit()];
        for (int i = 0; i < size; ++i) {
            BlockInfo info = all[i];
            if (            // 完全包含
                    HelpUtil.matrixInside(
                            minT, maxT, minA, maxA,
                            info.getMinT(), info.getMaxT(), info.getMinA(), info.getMaxA()
                    )
            ) {
                res += info.getSum();
                messageAmount += info.getMessageAmount();
            } else if (    // 部分包含
                    HelpUtil.intersect(
                            minT, maxT, minA, maxA,
                            info.getMinT(), info.getMaxT(), info.getMinA(), info.getMaxA()
                    )
            ) {
//                tList = info.readBlockT(tList);
                long[] tList = info.readBlockT();
//                aList = info.readBlockA(aList);
                long[] aList = info.readBlockA();
                for (int j = 0; j < info.getMessageAmount(); ++j) {
                    long a = aList[j];
                    long t = tList[j];
                    if (HelpUtil.inSide(t, a, minT, maxT, minA, maxA)) {
                        res += aList[j];
                        messageAmount++;
                    }
                }
            }
        }
        return messageAmount == 0 ? 0 : Math.floorDiv(res, messageAmount);
    }

    /**
     * 输出各块的情况
     *
     * @return
     */
    public void showEachInfo() {
        for (int i = 0; i < size; ++i) {
            BlockInfo info = all[i];
            System.out.println(
                    "第" + i + "块信息个数: " + info.getMessageAmount() +
                            " 占用内存:" + (info.getSizeT() + info.getSizeA()) + "(byte), " +
                            ((double) (info.getSizeT() + info.getSizeA()) / GlobalParams.ONE_MB) + "(MB) " +
                            " a 字节: " + (info.getSizeA()) +
                            " t 字节: " + (info.getSizeT())
            );

        }
    }

}
