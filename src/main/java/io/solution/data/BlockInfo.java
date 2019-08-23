package io.solution.data;

import io.openmessaging.Message;
import io.solution.GlobalParams;
import io.solution.map.rtree.AverageResult;
import io.solution.map.rtree.Entry;
import io.solution.map.rtree.RTree;
import io.solution.map.rtree.Rect;
import io.solution.utils.HelpUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: laynehuang
 * @CreatedAt: 2019/8/6 0006
 */
public class BlockInfo {

    private long maxT;
    private long minT;
    private long maxA;
    private long minA;

    private int messageAmount;

    // a的和
    private long sum;

    private RTree rTree = new RTree(GlobalParams.MAX_R_TREE_CHILDREN_AMOUNT);

    private PageInfo[] pageInfos = new PageInfo[GlobalParams.getBlockPageLimit()];

    private int pageInfoSize;

    /**
     * block info 赋值
     * 调用前必须先设置 消息数量
     */
    public void initBlockInfo(MyBlock block, long positionT, long positionA, long positionB) {

        pageInfoSize = 0;
        setSquare(block.getMinT(), block.getMaxT(), block.getMinA(), block.getMaxA());
        sum = block.getSum();
        messageAmount = block.getMessageAmount();
        int tempSize = 0;
        Message[] messages = new Message[GlobalParams.getPageMessageCount()];
        for (int i = 0; i < block.getMessageAmount(); ++i) {
            messages[tempSize++] = block.getMessages()[i];
            if (tempSize == GlobalParams.getPageMessageCount()) {
                addPageInfo(messages, tempSize, positionT, positionA, positionB);
                positionT += 8 * tempSize;
                positionA += 8 * tempSize;
                positionB += GlobalParams.getBodySize() * tempSize;
                tempSize = 0;
            }
        }

        if (tempSize > 0) {
            addPageInfo(messages, tempSize, positionT, positionA, positionB);
        }
        for(int i = 0; i < pageInfoSize; i ++) {
            rTree.Insert(
                    new Rect(
                            pageInfos[i].getMinT(),
                            pageInfos[i].getMaxT(),
                            pageInfos[i].getMinA(),
                            pageInfos[i].getMaxA()
                    ),
                    pageInfos[i].getSum(),
                    pageInfos[i].getMessageAmount(),
                    i
            );
        }
    }

    private void addPageInfo(
            Message[] messages,
            int messageAmount,
            long positionT,            // 块偏移起始
            long positionA,
            long positionB
    ) {
        PageInfo pageInfo = new PageInfo();
        pageInfo.addMessages(messages, messageAmount, positionT, positionA, positionB);
//        rTree.Insert(
//                new Rect(
//                        pageInfo.getMinT(),
//                        pageInfo.getMaxT(),
//                        pageInfo.getMinA(),
//                        pageInfo.getMaxA()
//                ),
//                pageInfo.getSum(),
//                pageInfo.getMessageAmount(),
//                pageInfoSize
//        );

        pageInfos[pageInfoSize++] = pageInfo;
    }

    public List<Message> find2(long minT, long maxT, long minA, long maxA) {
        List<Message> res = new ArrayList<>();

        ArrayList<Entry> nodes = rTree.Search(new Rect(minT, maxT, minA, maxA));
        for (Entry node : nodes) {
            PageInfo info = pageInfos[node.getIdx()];
            long[] tList = HelpUtil.readT(info.getPositionT(), info.getMessageAmount());
            long[] aList = HelpUtil.readA(info.getPositionA(), info.getMessageAmount());
            byte[][] bodyList = HelpUtil.readBody(info.getPositionB(), info.getMessageAmount());
            for (int j = 0; j < info.getMessageAmount() && tList[j] <= maxT; ++j) {
//                System.out.println("f2:" + tList[j] + "," + aList[j]);
                if (HelpUtil.inSide(tList[j], aList[j], minT, maxT, minA, maxA)) {
                    Message message = new Message(aList[j], tList[j], bodyList[j]);
                    res.add(message);
                }
            }
        }
        return res;
    }

    public void find3(long minT, long maxT, long minA, long maxA, MyResult myResult) {

        long res = 0;
        long cnt = 0;

//        long s1 = System.nanoTime();
        AverageResult result = rTree.SearchAverage(new Rect(minT, maxT, minA, maxA));
        res += result.getSum();
        cnt += result.getCnt();
//        long s2 = System.nanoTime();

        for (Entry entry : result.getResult()) {
            PageInfo info = pageInfos[entry.getIdx()];
            long[] aList = HelpUtil.readA(info.getPositionA(), info.getMessageAmount());
            if (minT <= info.getMinT() && info.getMaxT() <= maxT) {
                for (int i = 0; i < info.getMessageAmount(); ++i) {
                    if (minA <= aList[i] && aList[i] <= maxA) {
                        res += aList[i];
                        cnt++;
                    }
                }
            } else {
                long[] tList = HelpUtil.readT(info.getPositionT(), info.getMessageAmount());
                for (int i = 0; i < info.getMessageAmount() && tList[i] <= maxT; ++i) {
                    if (HelpUtil.inSide(tList[i], aList[i], minT, maxT, minA, maxA)) {
                        res += aList[i];
                        cnt++;
                    }
                }
            }
        }
        myResult.setSum(res);
        myResult.setCnt(cnt);
//        long s3 = System.nanoTime();
//        if (res == 0) {
//            System.out.println(
//                    "内层RTree搜索结点个数:" + result.getCheckNode()
//                            + " 消息总和:" + res
//                            + " 消息个数:" + cnt
//                            + " 查询包含块的个数:" + result.getCnt()
//                            + " 相交块数:" + result.getResult().size()
//                            + " 查询区间跨段（tDiff,aDiff): (" + (maxT - minT) + "," + (maxA - minA) + ")"
//                            + " r树查询时间：" + (s2 - s1)
//                            + " 求相交块平均值时间: " + (s3 - s2)
//            );
//            System.out.println("query:" + minT + "," + maxT + "," + minA + "," + maxA);
//            for (Entry entry : result.getResult()) {
//                PageInfo info = pageInfos[entry.getIdx()];
//                System.out.println("page:" + info.getMinT() + "," + info.getMaxT() + "," + info.getMinA() + "," + info.getMaxA());
//                long[] aList = HelpUtil.readA(info.getPositionA(), info.getMessageAmount());
//                long nowAMax = Long.MIN_VALUE;
//                long nowAMin = Long.MAX_VALUE;
//                for (int i = 0; i < info.getMessageAmount(); ++i) {
//                    nowAMax = Math.max(nowAMax, aList[i]);
//                    nowAMin = Math.min(nowAMin, aList[i]);
//                }
//                System.out.println("now: " + nowAMin + "," + nowAMax);
//            }
//
//        }
    }

    public long getSum() {
        return sum;
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
                "tL:" + minT +
                        ",tR:" + maxT +
                        ",aL:" + minA +
                        ",aR:" + maxA +
                        ",sum:" + sum +
                        ",aDiff: " + (maxA - minA) +
                        ",tDiff: " + (maxT - minT) +
                        ",area: " + ((maxA - minA) * (maxT - minT))
        );
    }

    public void showRect() {
        System.out.println(
                "aL:" + minA +
                        ",aR:" + maxA +
                        ",tL:" + minT +
                        ",tR:" + maxT +
                        ",sum:" + sum +
                        ",aDiff: " + (maxA - minA) +
                        ",tDiff: " + (maxT - minT)
        );
    }


//    public int getLimitT() {
//        return limitT;
//    }
//
//    public void setLimitT(int limit) {
//        this.limitT = limit;
//    }
//
//    public int getLimitA() {
//        return limitA;
//    }
//
//    public void setLimitA(int limit) {
//        this.limitA = limit;
//    }

//    public int getIdx() {
//        return idx;
//    }
//
//    public void setIdx(int idx) {
//        this.idx = idx;
//    }

    public RTree getrTree() {
        return rTree;
    }

    public void setrTree(RTree rTree) {
        this.rTree = rTree;
    }

    public int getMessageAmount() {
        return messageAmount;
    }

    public void setMessageAmount(int messageAmount) {
        this.messageAmount = messageAmount;
    }

    public PageInfo[] getPageInfos() {
        return this.pageInfos;
    }

    public int getPageInfoSize() {
        return pageInfoSize;
    }
}
