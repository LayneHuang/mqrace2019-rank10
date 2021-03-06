package io.solution.map;

import io.openmessaging.Message;
import io.solution.GlobalParams;
import io.solution.data.HashData;
import io.solution.data.HashInfo;
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

    // 第二阶段
    public ArrayList<ArrayList<Long>> minTs2 = new ArrayList<>();
    public ArrayList<ArrayList<Long>> maxTs2 = new ArrayList<>();
    private ArrayList<ArrayList<Long>> minAs2 = new ArrayList<>();
    private ArrayList<ArrayList<Long>> maxAs2 = new ArrayList<>();
    private ArrayList<ArrayList<Long>> sums2 = new ArrayList<>();
    public int[] lastMsgAmount = new int[GlobalParams.MAX_THREAD_AMOUNT];
    public int threadAmount = 0;

    // 第3阶段
    public int size3 = 0;
    private long[] minTs3 = new long[GlobalParams.getBlockInfoLimit()];
    private long[] maxTs3 = new long[GlobalParams.getBlockInfoLimit()];
    private long[] minAs3 = new long[GlobalParams.getBlockInfoLimit()];
    private long[] maxAs3 = new long[GlobalParams.getBlockInfoLimit()];
    private long[] sums3 = new long[GlobalParams.getBlockInfoLimit()];

    public int lastMsgAmount3 = GlobalParams.getBlockMessageLimit();
    private HashData[] hashDatas = new HashData[GlobalParams.getBlockInfoLimit()];

    private MyHash() {
        for (int i = 0; i < GlobalParams.MAX_THREAD_AMOUNT; ++i) {
            minTs2.add(new ArrayList<>());
            maxTs2.add(new ArrayList<>());
            minAs2.add(new ArrayList<>());
            maxAs2.add(new ArrayList<>());
            sums2.add(new ArrayList<>());
        }
    }

    public static MyHash getIns() {
        return ins;
    }

    public void insert2(int idx, long minT, long maxT, long minA, long maxA, long sum) {
        threadAmount = Math.max(threadAmount, idx + 1);
        minTs2.get(idx).add(minT);
        maxTs2.get(idx).add(maxT);
        minAs2.get(idx).add(minA);
        maxAs2.get(idx).add(maxA);
        sums2.get(idx).add(sum);
    }

    public void insert3(long minT, long maxT, long minA, long maxA, long sum, HashData hashData) {
        minTs3[size3] = minT;
        maxTs3[size3] = maxT;
        minAs3[size3] = minA;
        maxAs3[size3] = maxA;
        sums3[size3] = sum;
        hashDatas[size3] = hashData;
        size3++;
    }

//    public synchronized void cleanStepTwoInfo() {
//        if (GlobalParams.isStepTwoFinished()) {
//            return;
//        }
//        long s0 = System.nanoTime();
//        minTs2.clear();
//        maxTs2.clear();
//        minAs2.clear();
//        maxAs2.clear();
//        sums2.clear();
//        lastMsgAmount = null;
//        System.out.println("清掉第二阶段记录耗时:" + (System.nanoTime() - s0) + "(ns)");
//        System.out.println("Rest memory:" + (Runtime.getRuntime().maxMemory() - Runtime.getRuntime().totalMemory() + Runtime.getRuntime().freeMemory()) / (1024 * 1024) + "(M)");
//        GlobalParams.setStepTwoFinished();
//    }

    public List<Message> find2(long minT, long maxT, long minA, long maxA) {
        List<Message> res = new ArrayList<>();

        for (int idx = 0; idx < threadAmount; ++idx) {
            int l = findLeft2(idx, minT);
            int r = l;
            if (l == -1) {
                continue;
            }
            long nowMaxA = Long.MIN_VALUE;
            long nowMinA = Long.MAX_VALUE;
            int blockSize = minTs2.get(idx).size();
            for (int i = l; i < blockSize && minTs2.get(idx).get(i) <= maxT; ++i) {
                nowMaxA = Math.max(nowMaxA, maxAs2.get(idx).get(i));
                nowMinA = Math.min(nowMinA, minAs2.get(idx).get(i));
                r = i;
            }
            if (minA > nowMaxA || maxA < nowMinA) {
                continue;
            }

            int tMsgAmount = 0;
            int sIdx = -1;
            for (int i = l; i <= r; ++i) {
                int amount = GlobalParams.getBlockMessageLimit();
                if (i == blockSize - 1) amount = lastMsgAmount[idx];
                tMsgAmount += amount;
                // 是否相交
                boolean isIntersect = HelpUtil.intersect(
                        minT, maxT, minA, maxA,
                        minTs2.get(idx).get(i), maxTs2.get(idx).get(i), minAs2.get(idx).get(i), maxAs2.get(idx).get(i)
                );

                if (i < r && isIntersect && tMsgAmount < 1024 * 16) {
                    if (sIdx == -1) {
                        sIdx = i;
                    }
                    continue;
                } else if (sIdx == -1 && !isIntersect) {
                    tMsgAmount = 0;
                    continue;
                } else if (sIdx == -1) {
                    sIdx = i;
                }
                // 读出该块 a & t & body
                long taPos = 16L * sIdx * GlobalParams.getBlockMessageLimit();
                long bPos = (long) sIdx * GlobalParams.getBlockMessageLimit() * GlobalParams.getBodySize();
                long[] taList = HelpUtil.readTA(idx, taPos, tMsgAmount);
                byte[][] bodyList = HelpUtil.readBody(idx, bPos, tMsgAmount);
                for (int j = 0; j < tMsgAmount && taList[j << 1] <= maxT; ++j) {
                    if (HelpUtil.inSide(taList[j << 1], taList[j << 1 | 1], minT, maxT, minA, maxA)) {
                        res.add(new Message(taList[j << 1 | 1], taList[j << 1], bodyList[j]));
                    }
                }

                tMsgAmount = 0;
                sIdx = -1;
            }
        }
        res.sort(Comparator.comparingLong(Message::getT));
        return res;
    }

    public long find3(long minT, long maxT, long minA, long maxA) {

        int l = findLeft3(minT);
        int r = findRight3(maxT);
        if (l == -1 || r == -1) {
            return 0;
        }

        long res = 0;
        int cnt = 0;

        // 两边优化
        for (; l <= r; ++l) {
            int amount = (l == size3 - 1 ? lastMsgAmount3 : GlobalParams.getBlockMessageLimit());
            if (HelpUtil.matrixInside(minT, maxT, minA, maxA, minTs3[l], maxTs3[l], minAs3[l], maxAs3[l])) {
                res += sums3[l];
                cnt += amount;
            } else if (HelpUtil.intersect(minT, maxT, minA, maxA, minTs3[l], maxTs3[l], minAs3[l], maxAs3[l])) {
                break;
            }
        }

        for (; l <= r; --r) {
            int amount = (r == size3 - 1 ? lastMsgAmount3 : GlobalParams.getBlockMessageLimit());
            if (HelpUtil.matrixInside(minT, maxT, minA, maxA, minTs3[r], maxTs3[r], minAs3[r], maxAs3[r])) {
                res += sums3[r];
                cnt += amount;
            } else if (HelpUtil.intersect(minT, maxT, minA, maxA, minTs3[r], maxTs3[r], minAs3[r], maxAs3[r])) {
                break;
            }
        }

        if (l <= r) {
            // 打印小查询的个数 8K * SMALL_REGION
            if (l + GlobalParams.SMALL_REGION >= r) {
                if (l == r) {
                    int amount = (r == size3 - 1 ? lastMsgAmount3 : GlobalParams.getBlockMessageLimit());
                    long[] aList = HelpUtil.readA(8L * GlobalParams.getBlockMessageLimit() * l, amount);
                    long[] tList = hashDatas[l].readT();
                    for (int i = 0; i < amount; ++i) {
                        if (HelpUtil.inSide(tList[i], aList[i], minT, maxT, minA, maxA)) {
                            res += aList[i];
                            cnt++;
                        }
                    }
                } else {
                    int totalAmount = 0;
                    int lAmount = GlobalParams.getBlockMessageLimit();
                    int rAmount = (r == size3 - 1 ? lastMsgAmount3 : GlobalParams.getBlockMessageLimit());
                    for (int i = l; i <= r; ++i) {
                        int amount = (i == r ? rAmount : GlobalParams.getBlockMessageLimit());
                        totalAmount += amount;
                    }
                    long[] aList = HelpUtil.readA(8L * GlobalParams.getBlockMessageLimit() * l, totalAmount);
                    long[] lTList = hashDatas[l].readT();
                    long[] rTList = hashDatas[r].readT();

                    for (int i = 0; i < totalAmount; ++i) {
                        if (
                                (i < lAmount && HelpUtil.inSide(lTList[i], aList[i], minT, maxT, minA, maxA))
                                        || (i + rAmount - totalAmount >= 0 && HelpUtil.inSide(rTList[rAmount + i - totalAmount], aList[i], minT, maxT, minA, maxA))
                                        || (i >= lAmount && i + rAmount - totalAmount < 0 && minA <= aList[i] && aList[i] <= maxA)
                        ) {
                            res += aList[i];
                            cnt++;
                        }
                    }
                }
            } else {
                int leftAmount = GlobalParams.getBlockMessageLimit();
                int rightAmount = (r == size3 - 1 ? lastMsgAmount3 : GlobalParams.getBlockMessageLimit());

                // 右边界
                HashInfo rHashInfo = HelpUtil.readLineInfoAndA((8L * GlobalParams.getBlockMessageLimit() + GlobalParams.INFO_SIZE * GlobalParams.A_RANGE) * r, rightAmount);
                long[] rTList = hashDatas[r].readT();
                for (int j = 0; j < rightAmount && rTList[j] <= maxT; ++j) {
                    if (HelpUtil.inSide(rTList[j], rHashInfo.aList[j], minT, maxT, minA, maxA)) {
                        res += rHashInfo.aList[j];
                        cnt++;
                    }
                }

                // 左
                HashInfo lHashInfo = HelpUtil.readAAndLineInfo((8L * GlobalParams.getBlockMessageLimit() + GlobalParams.INFO_SIZE * GlobalParams.A_RANGE) * l + GlobalParams.INFO_SIZE * GlobalParams.A_RANGE, leftAmount);
                long[] lTList = hashDatas[l].readT();
                for (int j = 0; j < leftAmount && lTList[j] <= maxT; ++j) {
                    if (HelpUtil.inSide(lTList[j], lHashInfo.aList[j], minT, maxT, minA, maxA)) {
                        res += lHashInfo.aList[j];
                        cnt++;
                    }
                }

                // 中间
                int btPos = HelpUtil.getPosition(minA);
                int tpPos = HelpUtil.getPosition(maxA);
                for (int i = btPos + 1; i <= tpPos - 1; ++i) {
                    long sum = rHashInfo.lineInfos[i].bs - lHashInfo.lineInfos[i].bs;
                    if (sum < 0) {
                        sum = Long.MAX_VALUE + sum;
                    }
                    int dCnt = rHashInfo.lineInfos[i].cntSum - lHashInfo.lineInfos[i].cntSum;
                    res += sum;
                    cnt += dCnt;
                }

                // 上下边界
                if (btPos < tpPos) {
                    int bAmount = (int) ((rHashInfo.lineInfos[btPos].aPos - lHashInfo.lineInfos[btPos].aPos) / 8);
                    if (bAmount > 0) {
                        long[] baList = HelpUtil.readA(true, btPos, lHashInfo.lineInfos[btPos].aPos, bAmount);
                        for (int i = 0; i < bAmount; ++i) {
                            if (minA <= baList[i] && baList[i] <= maxA) {
                                res += baList[i];
                                cnt++;
                            }
                        }
                    }
                }

                int tAmount = (int) ((rHashInfo.lineInfos[tpPos].aPos - lHashInfo.lineInfos[tpPos].aPos) / 8);
                if (tAmount > 0) {
                    long[] taList = HelpUtil.readA(true, tpPos, lHashInfo.lineInfos[tpPos].aPos, tAmount);
                    for (int i = 0; i < tAmount; ++i) {
                        if (minA <= taList[i] && taList[i] <= maxA) {
                            res += taList[i];
                            cnt++;
                        }
                    }
                }
            }
        }
        return cnt == 0 ? 0 : res / cnt;
    }

    private int findLeft2(int idx, long value) {
        ArrayList<Long> maxTList = maxTs2.get(idx);
        int l = 0, r = maxTList.size() - 1;
        if (maxTList.get(r) < value) {
            return -1;
        }
        if (value <= maxTList.get(l)) {
            return l;
        }
        while (l + 1 < r) {
            int mid = (l + r) >> 1;
            if (maxTList.get(mid) < value) {
                l = mid;
            } else {
                r = mid;
            }
        }
        return l + 1;
    }

    private int findLeft3(long value) {
        int l = 0, r = size3 - 1;
        if (maxTs3[r] < value) {
            return -1;
        }
        if (value <= maxTs3[l]) {
            return l;
        }

        while (l + 1 < r) {
            int mid = (l + r) >> 1;
            if (maxTs3[mid] < value) {
                l = mid;
            } else {
                r = mid;
            }
        }
        return l + 1;
    }

    private int findRight3(long value) {
        int l = 0, r = size3 - 1;
        if (value < minTs3[l]) {
            return -1;
        }
        if (value >= minTs3[r]) {
            return r;
        }

        while (l + 1 < r) {
            int mid = (l + r) >> 1;
            if (minTs3[mid] > value) {
                r = mid;
            } else {
                l = mid;
            }
        }
        return r - 1;
    }
}
