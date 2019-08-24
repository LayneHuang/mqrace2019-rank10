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

    private int size = 0;

    private RTree rTree = new RTree(GlobalParams.MAX_R_TREE_CHILDREN_AMOUNT);

    public int getSize() {
        return size;
    }

    private MyHash() {

    }

    public static MyHash getIns() {
        return ins;
    }

    private long totalMsgAmount = 0;

    public synchronized void easyInsert(MyBlock block, long posT, long posA, long posB) {
        // RTree 插入
        Rect rect = new Rect(block.getMinT(), block.getMaxT(), block.getMinA(), block.getMaxA());
        size++;
        long s1 = System.nanoTime();
        rTree.Insert(rect, block.getSum(), block.getMessageAmount(), posT, posA, posB);
        long s2 = System.nanoTime();
        totalMsgAmount += block.getMessageAmount();
//        if (s2 - s1 > 50 * 1000 || size % 10000 == 0) {
//            System.out.println("now block size:" + size + ", inserted msg amount:" + totalMsgAmount + ", insert cost time:" + (s2 - s1));
//        }
    }

    public List<Message> easyFind2(long minT, long maxT, long minA, long maxA) {
        long s3 = System.currentTimeMillis();
        ArrayList<Entry> nodes = rTree.Search(new Rect(minT, maxT, minA, maxA));
        long s4 = System.currentTimeMillis();
        List<Message> res = new ArrayList<>();
        long s1 = System.currentTimeMillis();
        for (Entry node : nodes) {
            long[] tList = HelpUtil.readT(node.posT, node.count);
            long[] aList = HelpUtil.readA(node.posA, node.count);
            byte[][] bodyList = HelpUtil.readBody(node.posB, node.count);

            int l= 0, r = node.count;

            while(l<r) {
                int m = (l+r)>>1;
                if(aList[m] < minA) {
                    l= m +1;
                } else {
                    r = m;
                }
            }
            for (int j = r; j < node.count && aList[j] <= maxA; ++j) {
                if (HelpUtil.inSide(tList[j], aList[j], minT, maxT, minA, maxA)) {
                    Message message = new Message(aList[j], tList[j], bodyList[j]);
                    res.add(message);
                }
            }
        }
        long s2 = System.currentTimeMillis();
        if (s2 - s1 > 200) {
            System.out.println("Step2 node size:" + nodes.size() + ",Res size:" + res.size() + ", cost time:" + (s2 - s1)+ " rtree search time:" + (s4-s3));
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
        return cnt == 0 ? 0 : Math.floorDiv(res, cnt);
    }

}
