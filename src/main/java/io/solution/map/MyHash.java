package io.solution.map;

import io.openmessaging.Message;
import io.solution.GlobalParams;
import io.solution.data.LineInfo;
import io.solution.utils.AyscBufferHolder;
import io.solution.utils.HelpUtil;
import io.solution.utils.PretreatmentHolder;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

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
    public int size2 = 0;

    // 第3阶段
    public int size3 = 0;
    public long[] minTs3 = new long[GlobalParams.getBlockInfoLimit()];
    private long[] maxTs3 = new long[GlobalParams.getBlockInfoLimit()];
    private long[] minAs3 = new long[GlobalParams.getBlockInfoLimit()];
    private long[] maxAs3 = new long[GlobalParams.getBlockInfoLimit()];
    private long[] sums3 = new long[GlobalParams.getBlockInfoLimit()];
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

    public void insert(int idx, long minT, long maxT, long minA, long maxA) {
        size2 = Math.max(size2, idx + 1);
        minTs2.get(idx).add(minT);
        maxTs2.get(idx).add(maxT);
        minAs2.get(idx).add(minA);
        maxAs2.get(idx).add(maxA);
    }

    public void insert(int idx, long minT, long maxT, long minA, long maxA, long sum) {
        size2 = Math.max(size2, idx + 1);
        minTs2.get(idx).add(minT);
        maxTs2.get(idx).add(maxT);
        minAs2.get(idx).add(minA);
        maxAs2.get(idx).add(maxA);
        sums2.get(idx).add(sum);
    }

    public void insert3(long minT, long maxT, long minA, long maxA, long sum) {
        minTs3[size3] = minT;
        maxTs3[size3] = maxT;
        minAs3[size3] = minA;
        maxAs3[size3] = maxA;
        sums3[size3] = sum;
        size3++;
//        System.out.println("cnt:" + ((size3 - 1) * GlobalParams.getBlockMessageLimit() + lastMsgAmount3));
    }


    private ConcurrentHashMap<Long, Integer> indexMap = new ConcurrentHashMap<>();

    private static final String HEAP_CREATE_LOCK = "MYHASH_IDX_CREATE_LOCK";

    private int total = 0;

    public int getIndex(long threadId) {
        if (indexMap.containsKey(threadId)) {
            return indexMap.get(threadId);
        } else {
            synchronized (HEAP_CREATE_LOCK) {
                if (indexMap.containsKey(threadId)) {
                    return indexMap.get(threadId);
                }
                int index = total++;
                indexMap.put(threadId, index);
                return index;
            }
        }
    }

    public List<Message> find2(long minT, long maxT, long minA, long maxA) {

        List<Message> res = new ArrayList<>();
        int readCount = 0;
        long s = System.nanoTime();
        boolean isLargeQuery = false;
        for (int idx = 0; idx < size2; ++idx) {
            int l = findLeft2(idx, minT);
            int r = findRight2(idx, maxT);
            if (l == -1 || r == -1) {
                continue;
            }
            if (r - l + 1 > 1000) {
                System.out.println("第二阶段跨越块:" + (r - l + 1));
                isLargeQuery = true;
            }
            long nowMaxA = Long.MIN_VALUE;
            long nowMinA = Long.MAX_VALUE;
            for (int i = l; i <= r; ++i) {
                nowMaxA = Math.max(nowMaxA, maxAs2.get(idx).get(i));
                nowMinA = Math.min(nowMinA, minAs2.get(idx).get(i));
            }
            if (minA > nowMaxA || maxA < nowMinA) {
                continue;
            }
            int infoSize = maxTs2.get(idx).size();
            int tMsgAmount = 0;
            int sIdx = -1;

            for (int i = l; i <= r; ++i) {
                int amount = GlobalParams.getBlockMessageLimit();
                if (i == infoSize - 1) amount = lastMsgAmount[idx];
                tMsgAmount += amount;
                if (i < r && tMsgAmount < 1024 * 16 && HelpUtil.intersect(minT, maxT, minA, maxA, minTs2.get(idx).get(i), maxTs2.get(idx).get(i), minAs2.get(idx).get(i), maxAs2.get(idx).get(i))) {
                    if (sIdx == -1) {
                        sIdx = i;
                    }
                    continue;
                } else if (sIdx == -1 && !HelpUtil.intersect(minT, maxT, minA, maxA, minTs2.get(idx).get(i), maxTs2.get(idx).get(i), minAs2.get(idx).get(i), maxAs2.get(idx).get(i))) {
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

                readCount++;
                for (int j = 0; j < tMsgAmount; ++j) {
                    if (HelpUtil.inSide(tList[j], aList[j], minT, maxT, minA, maxA)) {
                        res.add(new Message(aList[j], tList[j], bodyList[j]));
                    }
                }
                tMsgAmount = 0;
                sIdx = -1;
            }
        }
        if (isLargeQuery) {
            System.out.println("读盘次数:" + readCount + " 耗时:" + (System.nanoTime() - s));
        }
        res.sort(Comparator.comparingLong(Message::getT));
        return res;
    }

    private boolean isOutput = false;

    public long easyFind3Aysc(long minT, long maxT, long minA, long maxA) {
        if (!isOutput) {
            isOutput = true;
            System.out.println("aysc pre deal is not finish. now size:" + size3 + " msg amount:" + (size3 * GlobalParams.getBlockMessageLimit()));
        }
        if ((size3 > GlobalParams.WRITE_COMMIT_COUNT_LIMIT && maxTs3[size3 - GlobalParams.WRITE_COMMIT_COUNT_LIMIT] > maxT)) {
            return find3(minT, maxT, minA, maxA);
        }
        long res = 0;
        int cnt = 0;
        for (int idx = 0; idx < size2; ++idx) {
            int l = findLeft2(idx, minT);
            int r = findRight2(idx, maxT);
            if (l == -1 || r == -1) {
                continue;
            }
            long nowMaxA = Long.MIN_VALUE;
            long nowMinA = Long.MAX_VALUE;
            for (int i = l; i <= r; ++i) {
                nowMaxA = Math.max(nowMaxA, maxAs2.get(idx).get(i));
                nowMinA = Math.min(nowMinA, minAs2.get(idx).get(i));
            }
            if (minA > nowMaxA || maxA < nowMinA) {
                continue;
            }

            int infoSize = maxTs2.get(idx).size();
            int tMsgAmount = 0;
            int sIdx = -1;

            for (int i = l; i <= r; ++i) {
                int amount = GlobalParams.getBlockMessageLimit();
                if (i == infoSize - 1) amount = lastMsgAmount[idx];
                tMsgAmount += amount;
                if (i < r && !HelpUtil.intersect(minT, maxT, minA, maxA, minTs2.get(idx).get(i), maxTs2.get(idx).get(i), minAs2.get(idx).get(i), maxAs2.get(idx).get(i))
                        && tMsgAmount < 1024 * 16) {
                    if (sIdx == -1) {
                        sIdx = i;
                    }
                    continue;
                } else if (HelpUtil.matrixInside(minT, maxT, minA, maxA, minTs2.get(idx).get(i), maxTs2.get(idx).get(i), minAs2.get(idx).get(i), maxAs2.get(idx).get(i))) {
                    res += sums2.get(idx).get(i);
                    cnt += amount;
                    tMsgAmount -= amount;
                } else if (sIdx == -1) {
                    sIdx = i;
                }
                if (sIdx != -1) {
                    long aPos = (long) sIdx * GlobalParams.getBlockMessageLimit() * 8;
                    long[] tList = readT(idx, sIdx, i, tMsgAmount);
                    long[] aList = HelpUtil.readA(false, idx, aPos, tMsgAmount);
                    for (int j = 0; j < tMsgAmount; ++j) {
                        if (HelpUtil.inSide(tList[j], aList[j], minT, maxT, minA, maxA)) {
                            res += aList[j];
                            cnt++;
                        }
                    }
                }
                tMsgAmount = 0;
                sIdx = -1;
            }

        }

        return cnt == 0 ? 0 : res / cnt;
    }

    public long easyFind3(long minT, long maxT, long minA, long maxA) {
        if (!PretreatmentHolder.getIns().isFinish) {
            System.out.println("pre deal is not finish. now size:" + size3);
            return 0;
        }
        int l = findLeft3(minT);
        int r = findRight3(maxT);
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
            int amount = (i == size3 - 1 ? lastMsgAmount3 : GlobalParams.getBlockMessageLimit());
            tMsgAmount += amount;
            if (i < r && !HelpUtil.matrixInside(minT, maxT, minA, maxA, minTs3[i], maxTs3[i], minAs3[i], maxAs3[i])
                    && tMsgAmount < 1024 * 16) {
                if (sIdx == -1) {
                    sIdx = i;
                }
                continue;
            } else if (HelpUtil.matrixInside(minT, maxT, minA, maxA, minTs3[i], maxTs3[i], minAs3[i], maxAs3[i])) {
                res += sums3[i];
                cnt += amount;
                tMsgAmount -= amount;
            } else if (sIdx == -1) {
                sIdx = i;
            }
            if (sIdx != -1) {
                long[] atList = HelpUtil.readAT(16L * sIdx * GlobalParams.getBlockMessageLimit(), tMsgAmount);
                for (int j = 0; j < tMsgAmount; ++j) {
                    if (HelpUtil.inSide(atList[j << 1], atList[j << 1 | 1], minT, maxT, minA, maxA)) {
                        res += atList[j << 1 | 1];
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
//        if (!PretreatmentHolder.getIns().isFinish) {
        System.out.println("find3 pre deal is not finish. now size:" + size3 + " msg amount:" + (size3 * GlobalParams.getBlockMessageLimit()));
//            return 0;
//        }
        int l = findLeft3(minT);
        int r = findRight3(maxT);
        if (l == -1 || r == -1) {
            return 0;
        }
        if (r - l + 1 < 3) {
            return easyFind3(minT, maxT, minA, maxA, l, r);
        }
        long res = 0;
        int cnt = 0;

//        long distance = PretreatmentHolder.getIns().distance;
        int btPos = HelpUtil.getPosition(minA);
        int tpPos = HelpUtil.getPosition(maxA);

        tpPos = Math.min(tpPos, GlobalParams.A_RANGE - 1);
        LineInfo[] leftLineInfos = HelpUtil.readLineInfo((l + 1) * GlobalParams.INFO_SIZE * GlobalParams.A_RANGE);
        LineInfo[] rightLineInfos = HelpUtil.readLineInfo(r * GlobalParams.INFO_SIZE * GlobalParams.A_RANGE);

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

        // 左右边界
        int leftAmount = GlobalParams.getBlockMessageLimit();
        long[] latList = HelpUtil.readAT(16L * l * GlobalParams.getBlockMessageLimit(), leftAmount);
        for (int j = 0; j < leftAmount; ++j) {
            if (HelpUtil.inSide(latList[j * 2], latList[j * 2 + 1], minT, maxT, minA, maxA)) {
                res += latList[j * 2 + 1];
                cnt++;
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

    private int findRight2(int idx, long value) {
        ArrayList<Long> minTList = minTs2.get(idx);
        int l = 0, r = minTList.size() - 1;
        if (value < minTList.get(l)) {
            return -1;
        }
        if (value >= minTList.get(r)) {
            return r;
        }
        while (l + 1 < r) {
            int mid = (l + r) >> 1;
            if (minTList.get(mid) > value) {
                r = mid;
            } else {
                l = mid;
            }
        }
        return r - 1;
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
        for (int j = l; j <= r; ++j) {
            int amount = AyscBufferHolder.getIns().hashInfos.get(idx).get(j).size;
            long[] tListSub = AyscBufferHolder.getIns().hashInfos.get(idx).get(j).readT();
            for (int k = 0; k < amount; ++k) {
                tList[tListSize++] = tListSub[k];
            }
        }
        return tList;
    }
}
