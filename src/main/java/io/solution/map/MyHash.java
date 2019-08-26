package io.solution.map;

import io.openmessaging.Message;
import io.solution.GlobalParams;
import io.solution.data.MyBlock;
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

    private int lastBlockMsgAmount = 0;
    private int lastBlockIdx = 0;

    public int totalMsg = 0;

    public synchronized void insert(MyBlock block, long posAT, long posB) {

        totalMsg += block.getMessageAmount();

        minTs[size] = block.minT;
        maxTs[size] = block.maxT;
        minAs[size] = block.minA;
        maxAs[size] = block.maxA;
        sums[size] = block.sum;
        posATs[size] = posAT;
        posBs[size] = posB;

        if (block.getMessageAmount() < GlobalParams.getBlockMessageLimit()) {
            lastBlockIdx = size;
            lastBlockMsgAmount = block.getMessageAmount();
        }

        for (int i = size; i >= 1 && minTs[i] < minTs[i - 1]; --i) {
            long tMinTs = minTs[i];
            long tMaxTs = maxTs[i];
            long tMinAs = minAs[i];
            long tMaxAs = maxAs[i];
            long tSum = sums[i];
            long tPosAT = posATs[i];
            long tPosB = posBs[i];

            minTs[i] = minTs[i - 1];
            maxTs[i] = maxTs[i - 1];
            minAs[i] = minAs[i - 1];
            maxAs[i] = maxAs[i - 1];
            sums[i] = sums[i - 1];
            posATs[i] = posATs[i - 1];
            posBs[i] = posBs[i - 1];


            minTs[i - 1] = tMinTs;
            maxTs[i - 1] = tMaxTs;
            minAs[i - 1] = tMinAs;
            maxAs[i - 1] = tMaxAs;
            sums[i - 1] = tSum;
            posATs[i - 1] = tPosAT;
            posBs[i - 1] = tPosB;
            if (lastBlockIdx == i) {
                lastBlockIdx--;
            }
        }
        size++;
    }

    public static MyHash getIns() {
        return ins;
    }


    public List<Message> easyFind2(long minT, long maxT, long minA, long maxA) {
        int cont = 0;
        List<Message> res = new ArrayList<>();
        int l = findLeft(minT);
        int r = findRight(maxT);
        if (l == -1 || r == -1) {
            return res;
        }

        int tMsgAmount = 0;
        int sIdx = -1;

        for (int i = l; i <= r; ++i) {

            int msgAmount = GlobalParams.getBlockMessageLimit();
            if (i == lastBlockIdx) msgAmount = lastBlockMsgAmount;

            tMsgAmount += msgAmount;
            if (i < r && posATs[i] + msgAmount * 16 == posATs[i + 1]) {
                if (sIdx == -1) {
                    sIdx = i;
                }
                cont++;
                continue;
            } else if (sIdx == -1) {
                sIdx = i;
            }

            long[] atList = HelpUtil.readAT(posATs[sIdx], tMsgAmount);
            byte[][] bodyList = HelpUtil.readBody(posBs[sIdx], tMsgAmount);
            for (int j = 0; j < tMsgAmount; ++j) {
                if (HelpUtil.inSide(atList[j * 2 + 1], atList[j * 2], minT, maxT, minA, maxA)) {
                    res.add(new Message(atList[j * 2], atList[j * 2 + 1], bodyList[j]));
                }
            }
            tMsgAmount = 0;
            sIdx = -1;
        }

        if (res.size() % 100 == 0) {
            System.out.println("continue count:" + cont);
        }

        res.sort(Comparator.comparingLong(Message::getT));
        return res;
    }

    public long easyFind3(long minT, long maxT, long minA, long maxA) {

//        int insCount = 0;
        long res = 0;
        int cnt = 0;

        int l = findLeft(minT);
        int r = findRight(maxT);
        if (l == -1 || r == -1) {
            return res;
        }

//        long s1 = System.currentTimeMillis();
        int tMsgAmount = 0;
        int sIdx = -1;

        for (int i = l; i <= r; ++i) {
            int msgAmount = GlobalParams.getBlockMessageLimit();
            if (i == lastBlockIdx) msgAmount = lastBlockMsgAmount;
            tMsgAmount += msgAmount;
            if (i < r && posATs[i] + msgAmount * 16 == posATs[i + 1]) {
                if (sIdx == -1) {
                    sIdx = i;
                }
                continue;
            } else if (sIdx == -1) {
                if (HelpUtil.intersect(minT, maxT, minA, maxA, minTs[i], maxTs[i], minAs[i], maxAs[i])) {
                    res += sums[i];
                    cnt += msgAmount;
//                    insCount++;
                } else {
                    sIdx = i;
                }
            }
            if (sIdx != -1) {
                long[] atList = HelpUtil.readAT(posATs[sIdx], tMsgAmount);
                for (int j = 0; j < tMsgAmount; ++j) {
                    if (HelpUtil.inSide(atList[j * 2 + 1], atList[j * 2], minT, maxT, minA, maxA)) {
                        res += atList[j * 2 + 1];
                        cnt++;
                    }
                }
            }
            tMsgAmount = 0;
            sIdx = -1;
        }

//        long s2 = System.currentTimeMillis();
//        if (res % 100 == 0) {
//            System.out.println("包含块:" + insCount + " 相交块:" + (r - l + 1 - insCount) + " 耗时:" + (s2 - s1));
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
        for (int i = 0; i < size; ++i) {
            System.out.println(i + " " + minTs[i] + " " + maxTs[i]);
            if (i > 1 && (minTs[i] < minTs[i - 1] || maxTs[i - 1] > maxTs[i])) {
                System.out.println("fuck!!!! " + i + " " + minTs[i - 1] + " " + minTs[i] + " " + maxTs[i - 1] + " " + maxTs[i]);
//                break;
                cnt1++;
            } else {
                cnt2++;
            }
        }
        System.out.println("cnt1:" + cnt1 + ",cnt2:" + cnt2);
    }

}
