package io.solution.map;

import io.openmessaging.Message;
import io.solution.GlobalParams;
import io.solution.data.BlockInfo;
import io.solution.data.MyResult;
import io.solution.data.PageInfo;
import io.solution.map.rtree.AverageResult;
import io.solution.map.rtree.Entry;
import io.solution.map.rtree.RTree;
import io.solution.map.rtree.Rect;

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

//    private ByteBuffer tBuffer = ByteBuffer.allocateDirect(GlobalParams.DIRECT_MEMORY_SIZE);

//    private int tBufferSize = 0;

//    public int gettBufferSize() {
//        return tBufferSize;
//    }

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
        // RTree 插入
        Rect rect = new Rect(info.getMinT(), info.getMaxT(), info.getMinA(), info.getMaxA());
        rTree.Insert(rect, info.getSum(), info.getMessageAmount(), size);
        info.setIdx(size);

        // t放到buffer中
//        for (int i = 0; i < info.getPageInfoSize(); ++i) {
//            PageInfo pageInfo = info.getPageInfos()[i];
//            pageInfo.settPosition(tBufferSize);
//            for (int j = 0; j < pageInfo.getSizeT(); ++j) {
//                tBuffer.put(tBufferSize + j, pageInfo.getDataT()[j]);
//            }
//            tBufferSize += pageInfo.getSizeT();
//            pageInfo.setDataT(null);
//        }

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
            res.addAll(info.find2(minT, maxT, minA, maxA));
        }
        res.sort(Comparator.comparingLong(Message::getT));
        return res;
    }

//    private int find3Count = 0;

    public long find3(long minT, long maxT, long minA, long maxA) {

//        find3Count++;
//        if (find3Count > 10000) {
//            return 0;
//        }

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
//        long s3 = System.nanoTime();
        MyResult myResult = new MyResult();
        for (Entry entry : result.getResult()) {
            BlockInfo info = all[entry.getIdx()];
            info.find3(minT, maxT, minA, maxA, myResult);
            res += myResult.getSum();
            messageAmount += myResult.getCnt();
        }
//        long s4 = System.nanoTime();
//        if (res % 100 == 0) {
//
//            System.out.println(
//                    "外层RTree搜索结点个数:" + result.getCheckNode()
//                            + " 消息个数:" + messageAmount
//                            + " 查询包含块的个数:" + result.getCnt()
//                            + " 相交块数:" + result.getResult().size()
//                            + " 查询区间跨段（tDiff,aDiff): (" + (maxT - minT) + "," + (maxA - minA) + ")"
//                            + " r树查询时间：" + (s2 - s1)
//                            + " 求相交块平均值时间: " + (s4 - s3)
//            );
//        }
        return messageAmount == 0 ? 0 : Math.floorDiv(res, messageAmount);
    }

    public BlockInfo[] getAll() {
        return all;
    }
//
//    public ByteBuffer gettBuffer() {
//        return tBuffer;
//    }

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
