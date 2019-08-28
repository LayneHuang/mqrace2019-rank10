package io.solution.map;

import io.openmessaging.Message;
import io.solution.GlobalParams;
import io.solution.data.LineInfo;
import io.solution.data.MyBlock;
import io.solution.utils.HelpUtil;
import io.solution.utils.StepTwoBufferHolder;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @Author: laynehuang
 * @CreatedAt: 2019/8/6 0006
 */
public class MyHash {

    public long aNowMaxValue = Long.MIN_VALUE;
//    public long aNowMinValue = Long.MAX_VALUE;

    private static MyHash ins = new MyHash();

    public int size = 0;
    private long[] minTs = new long[GlobalParams.getBlockInfoLimit()];
    private long[] maxTs = new long[GlobalParams.getBlockInfoLimit()];
    private long[] minAs = new long[GlobalParams.getBlockInfoLimit()];
    private long[] maxAs = new long[GlobalParams.getBlockInfoLimit()];
    private long[] sums = new long[GlobalParams.getBlockInfoLimit()];

    // a,t文件位移
    public long[] posATs = new long[GlobalParams.getBlockInfoLimit()];
    private long[] posBs = new long[GlobalParams.getBlockInfoLimit()];
    public int[] msgAmount = new int[GlobalParams.getBlockInfoLimit()];

    public long[] infoPos = new long[GlobalParams.getBlockInfoLimit()];

    //    public int totalMsg = 0;
    public int exchangeCount = 0;
    public int exchangeCost = 0;
    public int maxMsgAmount = 0;

    public synchronized void insert(MyBlock block, long posAT, long posB) {

//        totalMsg += block.getMessageAmount();

        minTs[size] = block.minT;
        maxTs[size] = block.maxT;
        minAs[size] = block.minA;
        maxAs[size] = block.maxA;
        sums[size] = block.sum;
        posATs[size] = posAT;
        posBs[size] = posB;
        msgAmount[size] = block.getMessageAmount();
        long s0 = System.nanoTime();
        for (int i = size; i >= 1 && maxTs[i - 1] > minTs[i]; --i) {
            minTs[i - 1] = Math.min(minTs[i - 1], minTs[i]);
            maxTs[i - 1] = Math.max(maxTs[i - 1], maxTs[i]);
            minAs[i - 1] = Math.min(minAs[i - 1], minAs[i]);
            maxAs[i - 1] = Math.max(maxAs[i - 1], maxAs[i]);
            sums[i - 1] += sums[i];
            msgAmount[i - 1] += msgAmount[i];
            exchangeCount++;
            size--;
        }
        exchangeCost += (System.nanoTime() - s0);

        // 维护值域
        aNowMaxValue = Math.max(aNowMaxValue, maxAs[size]);
//        aNowMinValue = Math.max(aNowMinValue, minAs[size]);
        maxMsgAmount = Math.max(maxMsgAmount, msgAmount[size]);
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

        List<Message> res = new ArrayList<>();
        int l = findLeft(minT);
        int r = findRight(maxT);

        if (l == -1 || r == -1) {
            return res;
        }

        int tMsgAmount = 0;
        int sIdx = -1;

        for (int i = l; i <= r; ++i) {
            tMsgAmount += msgAmount[i];
            if (i < r && posATs[i] + msgAmount[i] * 16 == posATs[i + 1] && tMsgAmount < 1024) {
                if (sIdx == -1) {
                    sIdx = i;
                }
                continue;
            } else if (sIdx == -1) {
                sIdx = i;
            }
            long[] atList = HelpUtil.readAT(posATs[sIdx], tMsgAmount);
            byte[][] bodyList = HelpUtil.readBody(posBs[sIdx], tMsgAmount);
            for (int j = 0; j < tMsgAmount; ++j) {
                if (HelpUtil.inSide(atList[j * 2], atList[j * 2 + 1], minT, maxT, minA, maxA)) {
                    res.add(new Message(atList[j * 2 + 1], atList[j * 2], bodyList[j]));
                }
            }
            tMsgAmount = 0;
            sIdx = -1;
        }

        res.sort(Comparator.comparingLong(Message::getT));
        return res;
    }


    public long easyFind3(long minT, long maxT, long minA, long maxA) {
        int l = findLeft(minT);
        int r = findRight(maxT);
        if (l == -1 || r == -1) {
            return 0;
        }
        return easyFind3(minT, maxT, minA, maxA, l, r);
    }

    private long easyFind3(long minT, long maxT, long minA, long maxA, int l, int r) {
        long res = 0;
        int cnt = 0;
        int tMsgAmount = 0;
        int sIdx = -1;

        for (int i = l; i <= r; ++i) {
            tMsgAmount += msgAmount[i];
            if (i < r && posATs[i] + msgAmount[i] * 16 == posATs[i + 1]
                    && !HelpUtil.matrixInside(minT, maxT, minA, maxA, minTs[i], maxTs[i], minAs[i], maxAs[i])
                    && tMsgAmount < 1024) {
                if (sIdx == -1) {
                    sIdx = i;
                }
                continue;
            } else if (HelpUtil.matrixInside(minT, maxT, minA, maxA, minTs[i], maxTs[i], minAs[i], maxAs[i])) {
                res += sums[i];
                cnt += msgAmount[i];
                tMsgAmount -= msgAmount[i];
            } else if (sIdx == -1) {
                sIdx = i;
            }
            if (sIdx != -1) {
                long[] atList = HelpUtil.readAT(posATs[sIdx], tMsgAmount);
                for (int j = 0; j < tMsgAmount; ++j) {
                    if (HelpUtil.inSide(atList[j * 2], atList[j * 2 + 1], minT, maxT, minA, maxA)) {
                        res += atList[j * 2 + 1];
                        cnt++;
                    }
                }
            }
            tMsgAmount = 0;
            sIdx = -1;
        }
        return cnt == 0 ? 0 : res / cnt;
    }

    public long find3(long minT, long maxT, long minA, long maxA) {
        int l = findLeft(minT);
        int r = findRight(maxT);
        if (l == -1 || r == -1) {
            return 0;
        }
        if (r - l + 1 < 6) {
            return easyFind3(minT, maxT, minA, maxA, l, r);
        }
        long res = 0;
        int cnt = 0;
        long distance = StepTwoBufferHolder.getIns().distance;
        int btPos = (int) Math.floorDiv(minA, distance);
        int tpPos = (int) Math.floorDiv(maxA, distance);
        tpPos = Math.min(tpPos, GlobalParams.A_RANGE - 1);
        LineInfo[] leftLineInfos = HelpUtil.readLineInfo(infoPos[l + 1]);
        LineInfo[] rightLineInfos = HelpUtil.readLineInfo(infoPos[r]);

        // 中间
        for (int i = btPos + 1; i <= tpPos - 1; ++i) {
            long sum = rightLineInfos[i].bs - leftLineInfos[i].bs;
            if (sum < 0) {
                sum = Long.MAX_VALUE + sum;
            }
            int dCnt = rightLineInfos[i].cntSum - leftLineInfos[i].cntSum;
            res += sum;
            cnt += dCnt;
        }

        // 上下边界
        if (btPos < tpPos) {
            int bAmount = (int) ((rightLineInfos[btPos].aPos - leftLineInfos[btPos].aPos) / 8);
            if (bAmount > 0) {
                long[] baList = HelpUtil.readA(btPos, leftLineInfos[btPos].aPos, bAmount);
                for (int i = 0; i < bAmount; ++i) {
                    if (minA <= baList[i] && baList[i] <= maxA) {
                        res += baList[i];
                        cnt++;
                    }
                }
            }
        }

        int tAmount = (int) ((rightLineInfos[tpPos].aPos - leftLineInfos[tpPos].aPos) / 8);
        if (tAmount > 0) {
            long[] taList = HelpUtil.readA(tpPos, leftLineInfos[tpPos].aPos, tAmount);
            for (int i = 0; i < tAmount; ++i) {
                if (minA <= taList[i] && taList[i] <= maxA) {
                    res += taList[i];
                    cnt++;
                }
            }
        }

        // 左右边界
        long[] latList = HelpUtil.readAT(posATs[l], msgAmount[l]);
        for (int j = 0; j < msgAmount[l]; ++j) {
            if (HelpUtil.inSide(latList[j * 2], latList[j * 2 + 1], minT, maxT, minA, maxA)) {
                res += latList[j * 2 + 1];
                cnt++;
            }
        }

        long[] ratList = HelpUtil.readAT(posATs[r], msgAmount[r]);
        for (int j = 0; j < msgAmount[r]; ++j) {
            if (HelpUtil.inSide(ratList[j * 2], ratList[j * 2 + 1], minT, maxT, minA, maxA)) {
                res += ratList[j * 2 + 1];
                cnt++;
            }
        }
        return cnt == 0 ? 0 : res / cnt;
    }

    private int findLeft(long value) {
        int l = 0, r = size - 1;
        if (maxTs[r] < value) {
            return -1;
        }
        if (value <= maxTs[l]) {
            return l;
        }

        while (l + 1 < r) {
            int mid = (l + r) >> 1;
            if (maxTs[mid] < value) {
                l = mid;
            } else {
                r = mid;
            }
        }
        return l + 1;
    }

    private int findRight(long value) {
        int l = 0, r = size - 1;
        if (value < minTs[l]) {
            return -1;
        }
        if (value >= minTs[r]) {
            return r;
        }

        while (l + 1 < r) {
            int mid = (l + r) >> 1;
            if (minTs[mid] > value) {
                r = mid;
            } else {
                l = mid;
            }
        }
        return r - 1;
    }

    public void check() {
        int cnt1 = 0;
        int cnt2 = 0;
        boolean cont = true;
        for (int i = 0; i < size; ++i) {
            if (i > 1 && (minTs[i] < minTs[i - 1] || maxTs[i - 1] > maxTs[i])) {
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
