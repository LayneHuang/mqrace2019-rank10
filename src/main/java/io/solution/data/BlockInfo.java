package io.solution.data;

import io.openmessaging.Message;
import io.solution.GlobalParams;
import io.solution.map.NewRTree.AverageResult;
import io.solution.map.NewRTree.Entry;
import io.solution.map.NewRTree.RTree;
import io.solution.map.NewRTree.Rect;
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

    /**
     * block info 赋值
     * 调用前必须先设置 消息数量
     */
    public void initBlockInfo(MyBlock block, long positionT, long positionA, long positionB) {

        setSquare(block.getMinT(), block.getMaxT(), block.getMinA(), block.getMaxA());
        sum = block.getSum();
        messageAmount = block.getMessageAmount();
        int tempSize = 0;

        long sum = 0;
        long minA = Long.MAX_VALUE;
        long minT = Long.MAX_VALUE;
        long maxA = Long.MIN_VALUE;
        long maxT = Long.MIN_VALUE;

        for (int i = 0; i < block.getMessageAmount(); ++i) {

            tempSize++;

            long nowA = block.getMessages()[i].getA();
            long nowT = block.getMessages()[i].getT();

            sum += block.getMessages()[i].getA();

            minT = Math.min(minT, nowT);
            maxT = Math.max(maxT, nowT);
            minA = Math.min(minA, nowA);
            maxA = Math.max(maxA, nowA);

            if (tempSize == GlobalParams.getPageMessageCount()) {
                // 插入到树内
                rTree.Insert(
                        new Rect(minT, maxT, minA, maxA),
                        sum,
                        tempSize,
                        (int) positionT,
                        (int) positionA,
                        (int) positionB
                );
                // 初始化页数据
                positionT += 8 * tempSize;
                positionA += 8 * tempSize;
                positionB += GlobalParams.getBodySize() * tempSize;
                minA = Long.MAX_VALUE;
                minT = Long.MAX_VALUE;
                maxA = Long.MIN_VALUE;
                maxT = Long.MIN_VALUE;
                sum = 0;
                tempSize = 0;
            }

        }

        if (tempSize > 0) {
            rTree.Insert(
                    new Rect(minT, maxT, minA, maxA),
                    sum,
                    tempSize,
                    (int) positionT,
                    (int) positionA,
                    (int) positionB
            );
        }
    }

    public List<Message> find2(long minT, long maxT, long minA, long maxA) {
        List<Message> res = new ArrayList<>();
        ArrayList<Entry> nodes = rTree.Search(new Rect(minT, maxT, minA, maxA));
        for (Entry entry : nodes) {
            long[] tList = HelpUtil.readT(entry.getPosT(), entry.getCount());
            long[] aList = HelpUtil.readA(entry.getPosA(), entry.getCount());
            byte[][] bodyList = HelpUtil.readBody(entry.getPosB(), entry.getCount());
            for (int j = 0; j < entry.getCount(); ++j) {
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
//            PageInfo info = pageInfos[entry.getIdx()];
            long[] aList = HelpUtil.readA(entry.getPosA(), entry.getCount());
            if (minT <= entry.getRect().minT && entry.getRect().maxT <= maxT) {
                for (int i = 0; i < entry.getCount(); ++i) {
                    if (minA <= aList[i] && aList[i] <= maxA) {
                        res += aList[i];
                        cnt++;
                    }
                }
            } else {
                long[] tList = HelpUtil.readT(entry.getPosT(), entry.getCount());
                for (int i = 0; i < entry.getCount() && tList[i] <= maxT; ++i) {
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

}
