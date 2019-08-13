package io.solution.map;

import io.openmessaging.Message;
import io.solution.GlobalParams;
import io.solution.data.BlockInfo;
import io.solution.map.rtree.Entry;
import io.solution.map.rtree.RTree;
import io.solution.map.rtree.Rect;
import io.solution.utils.HelpUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * @Author: laynehuang
 * @CreatedAt: 2019/8/6 0006
 */
public class MyHash {

    private int limit = GlobalParams.getBlockInfoLimit();

    private static MyHash ins = new MyHash();

    private BlockInfo[] all = new BlockInfo[limit];

    private int size = 0;

    private RTree rTree = new RTree(GlobalParams.MAX_R_TREE_CHILDREN_AMOUNT);

    private ByteBuffer aBuffer = ByteBuffer.allocateDirect(GlobalParams.DIRECT_MEMORY_SIZE);

    private int aBufferSize = 0;

    public int getSize() {
        return size;
    }

    private MyHash() {

    }

    public static MyHash getIns() {
        return ins;
    }

    public synchronized void insert(BlockInfo info) {
//        System.out.println("插入块的信息:");
//        maxADiff = Math.max(info.getMaxA() - info.getMinA(), maxADiff);
//        maxTDiff = Math.max(info.getMaxT() - info.getMinT(), maxTDiff);
//        System.out.println("最大差值: " + maxADiff + "(a) " + maxTDiff + "(t)");
//        info.show();

//        useByteSum += (info.getSizeA() + info.getSizeT());
//        System.out.println(
//                "插入" + (size + 1) + "块 " +
//                        "使用内存:" + String.format("%.6f", useByteSum / 1024 / 1024 / 1024) + "(GB) "
//                        + "message数量:" + info.getMessageAmount() + " "
//                        + " size:" + info.getSizeA() + "," + info.getSizeT() + " (byte) "
//                        + " limit:" + info.getLimitA() + "," + info.getLimitT() + " (byte) "
//                        + " mem:" + Runtime.getRuntime().freeMemory() + " (byte) "
//        );

        // RTree 插入
        Rect rect = new Rect(info.getMinT(), info.getMaxT(), info.getMinA(), info.getMaxA());
        rTree.Insert(rect, info.getSum(), info.getMessageAmount(), size);
//        System.out.println("rtree节点数:" + rTree.getSize());

        // a放到buffer中
        info.setIdx(size);
        info.setaPosition(aBufferSize);
        for (int i = 0; i < info.getSizeA(); ++i) {
            aBuffer.put(aBufferSize + i, info.getDataA()[i]);
        }
        aBufferSize += info.getSizeA();
        info.setDataA(null);

        // 放到列表中
        all[size] = info;
        size++;
        if (size == limit) {
            limit += (limit >> 1);
            all = Arrays.copyOf(all, limit);
        }
    }

    public List<Message> find2(long minT, long maxT, long minA, long maxA) {

//        List<Message> res = force2(minT, maxT, minA, maxA);
        ArrayList<Entry> nodes = rTree.Search(new Rect(minT, maxT, minA, maxA));
        List<Message> res = new ArrayList<>();
        for (Entry node : nodes) {
//            Entry entry = nodes.get(i);
            int idx = node.getIdx();
            BlockInfo info = all[idx];
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
        res.sort(Comparator.comparingLong(Message::getT));
        return res;
    }

    public long find3(long minT, long maxT, long minA, long maxA) {
//        System.out.println("查询区间: " + minT + " " + maxA + " " + minA + " " + maxA);
//        force3(minT, maxT, minA, maxA);

        long res = 0;
        long messageAmount = 0;

        ArrayList<Entry> nodes = rTree.Search(new Rect(minT, maxT, minA, maxA));

        for (Entry entry : nodes) {
//            System.out.println("idx : " + entry.getIdx() + " sum:" + entry.getSum() + " cnt:" + entry.getCount());
//            entry.show();
            if (
                    HelpUtil.matrixInside(
                            minT, maxT, minA, maxA,
                            entry.getRect().x1, entry.getRect().x2, entry.getRect().y1, entry.getRect().y2
                    )
            ) {
                res += entry.getSum();
                messageAmount += entry.getCount();
            } else {
                BlockInfo info = all[entry.getIdx()];
                long[] tList = info.readBlockT();
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

//        System.out.println("RTree求和总和:" + res + " 个数:" + messageAmount);
        return messageAmount == 0 ? 0 : Math.floorDiv(res, messageAmount);
    }

    public List<Message> force2(long minT, long maxT, long minA, long maxA) {
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


    public long force3(long minT, long maxT, long minA, long maxA) {
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

//                System.out.println("完全包含: " + i + "(idx)");
                info.show();
                res += info.getSum();
                messageAmount += info.getMessageAmount();
            } else if (    // 部分包含
                    HelpUtil.intersect(
                            minT, maxT, minA, maxA,
                            info.getMinT(), info.getMaxT(), info.getMinA(), info.getMaxA()
                    )
            ) {
//                System.out.println("矩阵相交: " + i + "(idx)");
//                info.show();
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
//        System.out.println("暴力求和总和:" + res + " 个数:" + messageAmount);
        return messageAmount == 0 ? 0 : Math.floorDiv(res, messageAmount);
    }


    public ByteBuffer getaBuffer() {
        return aBuffer;
    }

}
