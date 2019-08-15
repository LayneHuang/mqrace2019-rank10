package io.solution.map;

import io.openmessaging.Message;
import io.solution.GlobalParams;
import io.solution.data.BlockInfo;
import io.solution.map.rtree.AverageResult;
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

//    private long minA = Long.MAX_VALUE;
//    private long minT = Long.MAX_VALUE;
//    private long maxA = 0;
//    private long maxT = 0;
//    private long maxDiffA = 0;
//    private long maxDiffT = 0;
//    private long minDiffA = Long.MAX_VALUE;
//    private long minDiffT = Long.MAX_VALUE;
//    private long maxArea = 0;
//    private long aTotalDiff = 0;
//    private long tTotalDiff = 0;
//    private double areaSum = 0;

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

//        info.show();
//        long diffA = info.getMaxA() - info.getMinA();
//        long diffT = info.getMaxT() - info.getMinT();
//        aTotalDiff += diffA;
//        tTotalDiff += diffT;
//        areaSum += diffA * diffT;
//        maxDiffA = Math.max(maxDiffA, diffA);
//        maxDiffT = Math.max(maxDiffT, diffT);
//        minDiffA = Math.min(minDiffA, diffA);
//        minDiffT = Math.min(minDiffT, diffT);
//
//        maxArea = Math.max(maxArea, diffA * diffT);
//
//        maxA = Math.max(maxA, info.getMaxA());
//        minA = Math.min(minA, info.getMinA());
//        maxT = Math.max(maxT, info.getMaxT());
//        minT = Math.min(minT, info.getMinT());

        // 放到列表中
        all[size] = info;
        size++;
        if (size == limit) {
            limit += (limit >> 1);
            all = Arrays.copyOf(all, limit);
        }
    }

    public List<Message> find2(long minT, long maxT, long minA, long maxA) {

        ArrayList<Entry> nodes = rTree.Search(new Rect(minT, maxT, minA, maxA));
        List<Message> res = new ArrayList<>();
        for (Entry node : nodes) {
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

    private int find3Count = 0;

    public long find3(long minT, long maxT, long minA, long maxA) {

        find3Count++;
        if (find3Count > 10000) {
            return 0;
        }

//        System.out.println("查询区间: " + minT + " " + maxA + " " + minA + " " + maxA);
        long res = 0;
        long messageAmount = 0;
//        long s1 = System.nanoTime();
        AverageResult result = rTree.SearchAverage(new Rect(minT, maxT, minA, maxA));
//        long s2 = System.nanoTime();
//        ArrayList<Entry> nodes = rTree.Search(new Rect(minT, maxT, minA, maxA));
        res += result.getSum();
        messageAmount += result.getCnt();
//        System.out.println(res + "  cnt = "+ messageAmount);
        long s3 = System.nanoTime();
        for (Entry entry : result.getResult()) {
//            System.out.println("idx : " + entry.getIdx() + " sum:" + entry.getSum() + " cnt:" + entry.getCount());
//            entry.show();
//            if (
//                    HelpUtil.matrixInside(
//                            minT, maxT, minA, maxA,
//                            entry.getRect().x1, entry.getRect().x2, entry.getRect().y1, entry.getRect().y2
//                    )
//            ) {
//                BlockInfo info = all[entry.getIdx()];
//                res += info.getSum();
//                messageAmount += info.getMessageAmount();
//                insideCount++;
//            } else {
            BlockInfo info = all[entry.getIdx()];
            long[] tList = info.readBlockT();
            long[] aList = info.readBlockA();
            for (int j = 0; j < info.getMessageAmount(); ++j) {
                if (HelpUtil.inSide(tList[j], aList[j], minT, maxT, minA, maxA)) {
                    res += aList[j];
                    messageAmount++;
                }
            }
        }
//        long s4 = System.nanoTime();
//        if (res % 5 == 0) {
//
//            System.out.println(
//                    "RTree搜索结点个数:" + result.getCheckNode()
//                            + " 消息个数:" + messageAmount
//                            + " 查询包含块的消息数:" + result.getCnt()
//                            + " 相交块数:" + result.getResult().size()
//                            + " 查询区间跨段（tDiff,aDiff): (" + (maxT - minT) + "," + (maxA - minA) + ")"
//                            + " r树查询时间：" + (s2 - s1)
//                            + " 求相交块平均值时间: " + (s4 - s3)
//            );
//        }
        return messageAmount == 0 ? 0 : Math.floorDiv(res, messageAmount);
    }

    public ByteBuffer getaBuffer() {
        return aBuffer;
    }

//    public void showAllBlockInfo() {
//        System.out.println(
//                " minA: " + minA
//                        + " maxA: " + maxA
//                        + " minT: " + minT
//                        + " maxT: " + maxT
//                        + " aTotalDiff:" + aTotalDiff
//                        + " tTotalDiff:" + tTotalDiff
//                        + " areaSum: " + areaSum
//                        + " minDiffA: " + minDiffA
//                        + " maxDiffA: " + maxDiffA
//                        + " minDiffT: " + minDiffT
//                        + " maxDiffT: " + maxDiffT
//                        + " maxArea: " + maxArea
//        );
//    }
}
