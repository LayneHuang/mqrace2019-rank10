package io.solution.map;

import io.openmessaging.Message;
import io.solution.GlobalParams;
import io.solution.data.MyBlock;
import io.solution.map.NewRTree.AverageResult;
import io.solution.map.NewRTree.Entry;
import io.solution.map.NewRTree.RTree;
import io.solution.map.NewRTree.Rect;
import io.solution.utils.HelpUtil;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @Author: laynehuang
 * @CreatedAt: 2019/8/6 0006
 */
public class MyHash {

//    private int limit = GlobalParams.getBlockInfoLimit();

    private static MyHash ins = new MyHash();

//    private BlockInfo[] all = new BlockInfo[limit];

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

    private long totalMsgAmount = 0;


    public synchronized void easyInsert(MyBlock block, long posT, long posA, long posB) {
        // RTree 插入
        Rect rect = new Rect(block.getMinT(), block.getMaxT(), block.getMinA(), block.getMaxA());

        size++;
        long s1 = System.nanoTime();
        rTree.Insert(rect, block.getSum(), block.getMessageAmount(), posT, posA, posB);
        long s2 = System.nanoTime();
//        info.setIdx(size);
        totalMsgAmount += block.getMessageAmount();
//        long s = System.currentTimeMillis();
        if (s2 - s1 > 50 * 1000 || size % 10000 == 0) {
            System.out.println("now block size:" + size + ", inserted msg amount:" + totalMsgAmount + ", insert cost time:" + (s2 - s1));
        }
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
//        all[size] = info;
//        size++;
//        if (size == limit) {
//            limit += (limit >> 1);
//            all = Arrays.copyOf(all, limit);
//        }
    }

//    public synchronized void insert(BlockInfo info) {
//        // RTree 插入
//        Rect rect = new Rect(info.getMinT(), info.getMaxT(), info.getMinA(), info.getMaxA());
//        rTree.Insert(rect, info.getSum(), info.getMessageAmount(), size);
////        info.setIdx(size);
//        totalMsgAmount += info.getMessageAmount();
////        long s = System.currentTimeMillis();
//        System.out.println("now block size:" + size + ", inserted msg amount:" + totalMsgAmount);
//        // t放到buffer中
////        for (int i = 0; i < info.getPageInfoSize(); ++i) {
////            PageInfo pageInfo = info.getPageInfos()[i];
////            pageInfo.settPosition(tBufferSize);
////            for (int j = 0; j < pageInfo.getSizeT(); ++j) {
////                tBuffer.put(tBufferSize + j, pageInfo.getDataT()[j]);
////            }
////            tBufferSize += pageInfo.getSizeT();
////            pageInfo.setDataT(null);
////        }
//
//        // 放到列表中
//        all[size] = info;
//        size++;
//        if (size == limit) {
//            limit += (limit >> 1);
//            all = Arrays.copyOf(all, limit);
//        }
//    }

//    public List<Message> find2(long minT, long maxT, long minA, long maxA) {
//        ArrayList<Entry> nodes = rTree.Search(new Rect(minT, maxT, minA, maxA));
//        List<Message> res = new ArrayList<>();
//        for (Entry node : nodes) {
//            int idx = node.getIdx();
//            BlockInfo info = all[idx];
//            res.addAll(info.find2(minT, maxT, minA, maxA));
//        }
//        res.sort(Comparator.comparingLong(Message::getT));
//        return res;
//    }
//
////    private int find3Count = 0;
//
//    public long find3(long minT, long maxT, long minA, long maxA) {
//
////        find3Count++;
////        if (find3Count > 10000) {
////            return 0;
////        }
//
////        System.out.println("查询区间: " + minT + " " + maxA + " " + minA + " " + maxA);
//        long res = 0;
//        long messageAmount = 0;
////        long s1 = System.nanoTime();
//        AverageResult result = rTree.SearchAverage(new Rect(minT, maxT, minA, maxA));
////        long s2 = System.nanoTime();
//
//        //        ArrayList<Entry> nodes = rTree.Search(new Rect(minT, maxT, minA, maxA));
//        res += result.getSum();
//        messageAmount += result.getCnt();
////        System.out.println(res + "  cnt = "+ messageAmount);
////        long s3 = System.nanoTime();
//        MyResult myResult = new MyResult();
//        for (Entry entry : result.getResult()) {
//            BlockInfo info = all[entry.getIdx()];
//            info.find3(minT, maxT, minA, maxA, myResult);
//            res += myResult.getSum();
//            messageAmount += myResult.getCnt();
//        }
////        long s4 = System.nanoTime();
////        if (res % 100 == 0) {
////
////            System.out.println(
////                    "外层RTree搜索结点个数:" + result.getCheckNode()
////                            + " 消息个数:" + messageAmount
////                            + " 查询包含块的个数:" + result.getCnt()
////                            + " 相交块数:" + result.getResult().size()
////                            + " 查询区间跨段（tDiff,aDiff): (" + (maxT - minT) + "," + (maxA - minA) + ")"
////                            + " r树查询时间：" + (s2 - s1)
////                            + " 求相交块平均值时间: " + (s4 - s3)
////            );
////        }
//        return messageAmount == 0 ? 0 : Math.floorDiv(res, messageAmount);
//    }

    public List<Message> easyFind2(long minT, long maxT, long minA, long maxA) {
        ArrayList<Entry> nodes = rTree.Search(new Rect(minT, maxT, minA, maxA));
        List<Message> res = new ArrayList<>();
        for (Entry node : nodes) {
            long[] tList = HelpUtil.readT(node.posT, node.count);
            long[] aList = HelpUtil.readA(node.posA, node.count);
            byte[][] bodyList = HelpUtil.readBody(node.posB, node.count);
            for (int j = 0; j < node.count && tList[j] <= maxT; ++j) {
//                System.out.println("f2:" + tList[j] + "," + aList[j]);
                if (HelpUtil.inSide(tList[j], aList[j], minT, maxT, minA, maxA)) {
                    Message message = new Message(aList[j], tList[j], bodyList[j]);
                    res.add(message);
                }
            }
        }
        res.sort(Comparator.comparingLong(Message::getT));
        return res;
    }

    public long easyFind3(long minT, long maxT, long minA, long maxA) {
        long res = 0;
        long cnt = 0;
        AverageResult result = rTree.SearchAverage(new Rect(minT, maxT, minA, maxA));
        res += result.getSum();
        cnt += result.getCnt();
        for (Entry node : result.getResult()) {
            long[] aList = HelpUtil.readA(node.posA, node.count);
            if (minT <= node.rect.minT && node.rect.maxT <= maxT) {
                for (int i = 0; i < node.count; ++i) {
                    if (minA <= aList[i] && aList[i] <= maxA) {
                        res += aList[i];
                        cnt++;
                    }
                }
            } else {
                long[] tList = HelpUtil.readT(node.posT, node.count);
                for (int i = 0; i < node.count && tList[i] <= maxT; ++i) {
                    if (HelpUtil.inSide(tList[i], aList[i], minT, maxT, minA, maxA)) {
                        res += aList[i];
                        cnt++;
                    }
                }
            }
        }
        return cnt == 0 ? 0 : Math.floorDiv(res, cnt);
    }

}
