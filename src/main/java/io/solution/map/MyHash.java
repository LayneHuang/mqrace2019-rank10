package io.solution.map;

import io.openmessaging.Message;
import io.solution.GlobalParams;
import io.solution.data.MyBlock;
import io.solution.utils.BlockHolder;
import io.solution.utils.BufferHolder;
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

    public int size = 0;
    private long[] minTs = new long[GlobalParams.getBlockInfoLimit()];
    private long[] maxTs = new long[GlobalParams.getBlockInfoLimit()];
    private long[] minAs = new long[GlobalParams.getBlockInfoLimit()];
    private long[] maxAs = new long[GlobalParams.getBlockInfoLimit()];
    private long[] sums = new long[GlobalParams.getBlockInfoLimit()];

    // a,t文件位移
    private long[] posATs = new long[GlobalParams.getBlockInfoLimit()];
    private long[] posBs = new long[GlobalParams.getBlockInfoLimit()];
    private int[] msgAmount = new int[GlobalParams.getBlockInfoLimit()];

    private int lastBlockMsgAmount = 0;
    private int lastBlockIdx = 0;

    public int totalMsg = 0;
    public int exchangeCount = 0;
    public int exchangeCost = 0;
    public int maxMsgAmount = 0;

    public synchronized void insert(MyBlock block, long posAT, long posB) {

        totalMsg += block.getMessageAmount();

        minTs[size] = block.minT;
        maxTs[size] = block.maxT;
        minAs[size] = block.minA;
        maxAs[size] = block.maxA;
        sums[size] = block.sum;
        posATs[size] = posAT;
        posBs[size] = posB;
        msgAmount[size] = block.getMessageAmount();

        long s0 = System.currentTimeMillis();

        for (int i = size; i >= 1 && (minTs[i] < minTs[i - 1] || maxTs[i - 1] > maxTs[i]); --i) {
            minTs[i - 1] = Math.min(minTs[i - 1], minTs[i]);
            maxTs[i - 1] = Math.max(maxTs[i - 1], maxTs[i]);
            minAs[i - 1] = Math.min(minAs[i - 1], minAs[i]);
            maxAs[i - 1] = Math.max(maxAs[i - 1], maxAs[i]);
            sums[i - 1] += sums[i];
            if (posATs[i - 1] + 16 * msgAmount[i - 1] != posATs[i]) {
                System.out.println("fuck!!!~" + posATs[i - 1] + " " + posATs[i]);
            }
            msgAmount[i - 1] += msgAmount[i];
            exchangeCount++;
            size--;
        }

        maxMsgAmount = Math.max(maxMsgAmount, msgAmount[size]);

        exchangeCost += (System.currentTimeMillis() - s0);
        size++;

//        if (size % 10000 == 0) {
//            System.out.println(
//                    "插入消息:" + totalMsg
//                            + " block等待时间:" + BlockHolder.getIns().waitTime
//                            + " buffer等待时间:" + BufferHolder.getIns().waitTime
//                            + " exchange次数:" + exchangeCount
//                            + " exchange耗时:" + exchangeCost
//            );
//        }
    }

    public static MyHash getIns() {
        return ins;
    }

    public List<Message> easyFind2(long minT, long maxT, long minA, long maxA) {
//        int cont = 0;
        List<Message> res = new ArrayList<>();
        int l = findLeft(minT);
        int r = findRight(maxT);
        if (l == -1 || r == -1) {
            return res;
        }

        for (int i = l; i <= r; ++i) {
            long[] atList = HelpUtil.readAT(posATs[i], msgAmount[i]);
            byte[][] bodyList = HelpUtil.readBody(posBs[i], msgAmount[i]);
            for (int j = 0; j < msgAmount[i]; ++j) {
                if (HelpUtil.inSide(atList[j * 2 + 1], atList[j * 2], minT, maxT, minA, maxA)) {
                    res.add(new Message(atList[j * 2], atList[j * 2 + 1], bodyList[j]));
                }
            }
        }

//        int tMsgAmount = 0;
//        int sIdx = -1;

//        for (int i = l; i <= r; ++i) {
//            tMsgAmount += msgAmount[i];
//            if (i < r && posATs[i] + msgAmount[i] * 16 == posATs[i + 1]) {
//                if (sIdx == -1) {
//                    sIdx = i;
//                }
////                cont++;
//                continue;
//            } else if (sIdx == -1) {
//                sIdx = i;
//            }
//            long[] atList = HelpUtil.readAT(posATs[sIdx], tMsgAmount);
//            byte[][] bodyList = HelpUtil.readBody(posBs[sIdx], tMsgAmount);
//            for (int j = 0; j < tMsgAmount; ++j) {
//                if (HelpUtil.inSide(atList[j * 2 + 1], atList[j * 2], minT, maxT, minA, maxA)) {
//                    res.add(new Message(atList[j * 2], atList[j * 2 + 1], bodyList[j]));
//                }
//            }
//            tMsgAmount = 0;
//            sIdx = -1;
//        }

//        if (res.size() % 100 == 0) {
//            System.out.println("continue count:" + cont);
//        }

        res.sort(Comparator.comparingLong(Message::getT));
        return res;
    }

    public long easyFind3(long minT, long maxT, long minA, long maxA) {

        long res = 0;
        int cnt = 0;

        int l = findLeft(minT);
        int r = findRight(maxT);
        if (l == -1 || r == -1) {
            return res;
        }

        for (int i = l; i <= r; ++i) {

            long[] atList = HelpUtil.readAT(posATs[i], msgAmount[i]);
            for (int j = 0; j < msgAmount[i]; ++j) {
                if (HelpUtil.inSide(atList[j * 2 + 1], atList[j * 2], minT, maxT, minA, maxA)) {
                    res += atList[j * 2 + 1];
                    cnt++;
                }
            }
        }


//        int tMsgAmount = 0;
//        int sIdx = -1;

//        for (int i = 1; i <= size - 1; ++i) {
//
//
//
//
//            tMsgAmount += msgAmount[i];
//            if (i < r && posATs[i] + msgAmount[i] * 16 == posATs[i + 1]) {
//                if (sIdx == -1) {
//                    sIdx = i;
//                }
//                continue;
//            } else if (sIdx == -1) {
//                sIdx = i;
//            }
//
//            long[] atList = HelpUtil.readAT(posATs[sIdx], tMsgAmount);
//            for (int j = 0; j < tMsgAmount; ++j) {
//                if (HelpUtil.inSide(atList[j * 2 + 1], atList[j * 2], minT, maxT, minA, maxA)) {
//                    res += atList[j * 2 + 1];
//                    cnt++;
//                }
//            }
//            tMsgAmount = 0;
//            sIdx = -1;
//        }

        return cnt == 0 ? 0 : Math.floorDiv(res, (long) cnt);
    }


    private int findLeft(long value) {
        int l = 0, r = size - 1;
        if (maxTs[r] < value) {
            return -1;
        }
        if (value <= maxTs[l]) {
            return l;
        }

        while (l <= r) {
//            System.out.println("l:" + l + " r:" + r);
            int mid = (l + r) >> 1;
            if (minTs[mid] >= value) {
                r = mid - 1;
            } else {
                l = mid + 1;
            }
        }
        return r - 1;
    }

    private int findRight(long value) {
        int l = 0, r = size - 1;
        if (value < minTs[l]) {
            return -1;
        }
        if (value >= minTs[r]) {
            return r;
        }

        while (l <= r) {
//            System.out.println("l:" + l + " r:" + r);
            int mid = (l + r) >> 1;
            if (maxTs[mid] >= value) {
                r = mid - 1;
            } else {
                l = mid + 1;
            }
        }
        return l + 1;
    }


    public void check() {
        int cnt1 = 0;
        int cnt2 = 0;
        boolean cont = true;
        for (int i = 0; i < size; ++i) {
//            System.out.println(i + " " + minTs[i] + " " + maxTs[i]);
            if (i > 1 && (minTs[i] < minTs[i - 1] || maxTs[i - 1] > maxTs[i])) {
//                System.out.println("fuck!!!! " + i + " " + minTs[i - 1] + " " + minTs[i] + " " + maxTs[i - 1] + " " + maxTs[i]);
//                break;
                cnt1++;
            } else {
                cnt2++;
            }

            if (i + 1 < size && posATs[i] + 16 * msgAmount[i] != posATs[i + 1]) {
                cont = false;
            }

        }
        System.out.println("cnt1:" + cnt1 + ",cnt2:" + cnt2 + (cont ? " 偏移连续" : " 偏移出问题!!"));
    }

}
