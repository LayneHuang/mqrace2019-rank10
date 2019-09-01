package io.solution.map;

import io.openmessaging.Message;
import io.solution.GlobalParams;
import io.solution.data.LineInfo;
import io.solution.utils.AyscBufferHolder;
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
    public int lastMsgAmount3 = GlobalParams.getBlockMessageLimit();

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

    public void insert(int idx, long minT, long maxT, long minA, long maxA, long sum) {
        threadAmount = Math.max(threadAmount, idx + 1);
        minTs2.get(idx).add(minT);
        maxTs2.get(idx).add(maxT);
        minAs2.get(idx).add(minA);
        maxAs2.get(idx).add(maxA);
        sums2.get(idx).add(sum);
    }

    public void insert3(long minT, long maxT) {
        minTs3[size3] = minT;
        maxTs3[size3] = maxT;
        size3++;
    }

    public synchronized void cleanStepTwoInfo() {
        if (GlobalParams.isStepTwoFinished()) {
            return;
        }
        minTs2.clear();
        maxTs2.clear();
        minAs2.clear();
        maxAs2.clear();
        lastMsgAmount = null;
        GlobalParams.setStepTwoFinished();
    }

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
//
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
                long aPos = (long) sIdx * GlobalParams.getBlockMessageLimit() * 8;
                long bPos = (long) sIdx * GlobalParams.getBlockMessageLimit() * GlobalParams.getBodySize();

                long[] tList = readT(idx, sIdx, i, tMsgAmount);
                long[] aList = HelpUtil.readA(false, idx, aPos, tMsgAmount);
                byte[][] bodyList = HelpUtil.readBody(idx, bPos, tMsgAmount);
                for (int j = 0; j < tMsgAmount && tList[j] <= maxT; ++j) {
                    if (HelpUtil.inSide(tList[j], aList[j], minT, maxT, minA, maxA)) {
                        res.add(new Message(aList[j], tList[j], bodyList[j]));
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

        if (l + 1 < r) {
            int btPos = HelpUtil.getPosition(minA);
            int tpPos = HelpUtil.getPosition(maxA);

            tpPos = Math.min(tpPos, GlobalParams.A_RANGE - 1);
            LineInfo[] leftLineInfos = HelpUtil.readLineInfo((long) (l + 1) * GlobalParams.INFO_SIZE * GlobalParams.A_RANGE);
            LineInfo[] rightLineInfos = HelpUtil.readLineInfo((long) r * GlobalParams.INFO_SIZE * GlobalParams.A_RANGE);

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
                    long[] baList = HelpUtil.readA(true, btPos, leftLineInfos[btPos].aPos, bAmount);
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
                long[] taList = HelpUtil.readA(true, tpPos, leftLineInfos[tpPos].aPos, tAmount);
                for (int i = 0; i < tAmount; ++i) {
                    if (minA <= taList[i] && taList[i] <= maxA) {
                        res += taList[i];
                        cnt++;
                    }
                }
            }
        }

        // 左右边界
        if (l < r) {
            int leftAmount = GlobalParams.getBlockMessageLimit();
            long[] latList = HelpUtil.readAT(16L * l * GlobalParams.getBlockMessageLimit(), leftAmount);
            for (int j = 0; j < leftAmount; ++j) {
                if (HelpUtil.inSide(latList[j * 2], latList[j * 2 + 1], minT, maxT, minA, maxA)) {
                    res += latList[j * 2 + 1];
                    cnt++;
                }
            }
        }

        int rightAmount = (r == size3 - 1 ? lastMsgAmount3 : GlobalParams.getBlockMessageLimit());
        long[] ratList = HelpUtil.readAT(16L * r * GlobalParams.getBlockMessageLimit(), rightAmount);
        for (int j = 0; j < rightAmount; ++j) {
            if (HelpUtil.inSide(ratList[j * 2], ratList[j * 2 + 1], minT, maxT, minA, maxA)) {
                res += ratList[j * 2 + 1];
                cnt++;
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

    public static long[] readT(int idx, int l, int r, int size) {
        long[] tList = new long[size];
        int tListSize = 0;
        for (int i = l; i <= r; ++i) {
            int amount = AyscBufferHolder.getIns().hashInfos.get(idx).get(i).size;
            long[] tListSub = AyscBufferHolder.getIns().hashInfos.get(idx).get(i).readT();
            for (int j = 0; j < amount && tListSize < size; ++j) {
                tList[tListSize++] = tListSub[j];
            }
        }
        return tList;
    }

}
