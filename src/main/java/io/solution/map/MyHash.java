package io.solution.map;

import io.openmessaging.Message;
import io.solution.GlobalParams;
import io.solution.data.MyBlock;
import io.solution.map.rtree.AverageResult;
import io.solution.map.rtree.Entry;
import io.solution.map.rtree.RTree;
import io.solution.map.rtree.Rect;
import io.solution.utils.HelpUtil;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @Author: laynehuang
 * @CreatedAt: 2019/8/6 0006
 */
public class MyHash {

    private static MyHash ins = new MyHash();

    private int size = GlobalParams.FATHER_TREE_BLOCK_SIZE;

    private int curIndex = 0;

    private RTree rTree = new RTree(GlobalParams.MAX_R_TREE_CHILDREN_AMOUNT);

    private RTree[] subRTree = new RTree[GlobalParams.getRTreeSubTreeSize()];

    public int getCurIndex() {
        return curIndex;
    }

    public int getSize() {
        return size;
    }

    private MyHash() {

    }

    public static MyHash getIns() {
        return ins;
    }

    private long totalMsgAmount = 0;

    public void flush() {
        if (size != 0) {
            RTree curSubRTree = subRTree[curIndex - 1];
            rTree.Insert(curSubRTree.getRoot().getRect(), curSubRTree.getRoot().getSum(), curSubRTree.getRoot().getCount(), curIndex - 1, 0, 0);
            size = 0;
        }
    }

    public synchronized void easyInsert(MyBlock block, long posT, long posA, long posB) {
        // RTree 插入
        if (size == GlobalParams.FATHER_TREE_BLOCK_SIZE) {
            if (curIndex > 0) {
                RTree curSubRTree = subRTree[curIndex - 1];
                rTree.Insert(curSubRTree.getRoot().getRect(), curSubRTree.getRoot().getSum(), curSubRTree.getRoot().getCount(), curIndex - 1, 0, 0);
            }
            subRTree[curIndex++] = new RTree(GlobalParams.MAX_R_TREE_CHILDREN_AMOUNT);
            size = 0;
        }
        Rect rect = new Rect(block.getMinT(), block.getMaxT(), block.getMinA(), block.getMaxA());
        size++;
//        long s1 = System.nanoTime();
        subRTree[curIndex - 1].Insert(rect, block.getSum(), block.getMessageAmount(), posT, posA, posB);
//        long s2 = System.nanoTime();
        totalMsgAmount += block.getMessageAmount();
//        if (s2 - s1 > 50 * 1000 || getBlockCount() % 10000 == 0) {
//            System.out.println("now block size:" + getBlockCount() + ", inserted msg amount:" + totalMsgAmount + ", insert cost time:" + (s2 - s1));
//        }
    }

    public List<Message> easyFind2(long minT, long maxT, long minA, long maxA) {
        Rect range = new Rect(minT, maxT, minA, maxA);
        ArrayList<Entry> nodes = rTree.Search(range);
        List<Message> res = new ArrayList<>();

        for (Entry node : nodes) {
            ArrayList<Entry> entries = subRTree[(int) node.posT].Search(range);
            for (Entry entry : entries) {
                long[] tList = HelpUtil.readT(entry.posT, entry.count);
                long[] aList = HelpUtil.readA(entry.posA, entry.count);
                byte[][] bodyList = HelpUtil.readBody(entry.posB, entry.count);
                for (int j = 0; j < entry.count && aList[j] <= maxA; ++j) {
                    if (HelpUtil.inSide(tList[j], aList[j], minT, maxT, minA, maxA)) {
                        Message message = new Message(aList[j], tList[j], bodyList[j]);
                        res.add(message);
                    }
                }
            }
        }
        res.sort(Comparator.comparingLong(Message::getT));
        return res;
    }

    private boolean isFirst = true;
    // 单次查询最大块相交数
    public int maxIntersect = 0;

    public long easyFind3(long minT, long maxT, long minA, long maxA) {
        Rect range = new Rect(minT, maxT, minA, maxA);
        long res = 0;
        long cnt = 0;
        long s1 = System.currentTimeMillis();
        AverageResult result = rTree.SearchAverage(range);
        long s2 = System.currentTimeMillis();
        res += result.getSum();
        cnt += result.getCnt();

        int total = 0;
        long s3 = System.currentTimeMillis();
        for (Entry entry : result.getResult()) {
            AverageResult subRes = subRTree[(int) entry.posT].SearchAverage(range);
            res += subRes.getSum();
            cnt += subRes.getCnt();
            total += subRes.getResult().size();
            for (Entry node : subRes.getResult()) {
                long[] aList = HelpUtil.readA(node.posA, node.count);
                if (minT <= node.rect.minT && node.rect.maxT <= maxT) {
                    for (int i = 0; i < node.count && aList[i] <= maxA; ++i) {
                        if (minA <= aList[i]) {
                            res += aList[i];
                            cnt++;
                        }
                    }
                } else {
                    long[] tList = HelpUtil.readT(node.posT, node.count);
                    for (int i = 0; i < node.count && aList[i] <= maxA; ++i) {
                        if (HelpUtil.inSide(tList[i], aList[i], minT, maxT, minA, maxA)) {
                            res += aList[i];
                            cnt++;
                        }
                    }
                }
            }
        }
        long s4 = System.currentTimeMillis();
        maxIntersect = Math.max(maxIntersect, total);
        if (res % 1000 == 0 && isFirst) {
            isFirst = false;
            System.out.println("Step3 外层相交块:" + result.getResult().size() + ",内层相交块个数:" + total + ", cost time:" + (s4 - s3) + " rtree search time:" + (s2 - s1));
            System.out.println("最大相交数:" + maxIntersect);
            System.out.println("Query: " + minT + " " + maxT + " " + minA + " " + maxA);
            for (Entry entry : result.getResult()) {
                System.out.println("Fat Block: " + (entry.rect.minT - minT) + " " + (entry.rect.maxT - maxT) + " " + (entry.rect.minA - minA) + " " + (entry.rect.maxA - maxA));
                AverageResult subRes = subRTree[(int) entry.posT].SearchAverage(range);
                for (Entry node : subRes.getResult()) {
                    System.out.println("Son Block: " + (node.rect.minT - minT) + " " + (node.rect.maxT - maxT) + " " + (node.rect.minA - minA) + " " + (node.rect.maxA - maxA));
                }
            }
        }
        return cnt == 0 ? 0 : Math.floorDiv(res, cnt);
    }

    public int getBlockCount() {
        return (curIndex * GlobalParams.FATHER_TREE_BLOCK_SIZE + size);
    }

}
