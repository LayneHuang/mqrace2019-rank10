package io.solution.map;

import io.openmessaging.Message;
import io.solution.GlobalParams;
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
    private ArrayList<ArrayList<Long>> minTs2 = new ArrayList<>();
    private ArrayList<ArrayList<Long>> maxTs2 = new ArrayList<>();
    private ArrayList<ArrayList<Long>> minAs2 = new ArrayList<>();
    private ArrayList<ArrayList<Long>> maxAs2 = new ArrayList<>();
    private ArrayList<ArrayList<Long>> sums2 = new ArrayList<>();
    public int[] lastMsgAmount = new int[GlobalParams.MAX_THREAD_AMOUNT];
    private int size2 = 0;

    public int size = 0;

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
        size2 = Math.max(size2, idx + 1);
        minTs2.get(idx).add(minT);
        maxTs2.get(idx).add(maxT);
        minAs2.get(idx).add(minA);
        maxAs2.get(idx).add(maxA);
        sums2.get(idx).add(sum);
    }

    public List<Message> find2(long minT, long maxT, long minA, long maxA) {
        List<Message> res = new ArrayList<>();
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
                if (i < r && tMsgAmount < 1024 * 16) {
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

                long tPos = AyscBufferHolder.getIns().tPos[idx] + sIdx * GlobalParams.getBlockMessageLimit() * 8;
                long aPos = AyscBufferHolder.getIns().aPos[idx] + sIdx * GlobalParams.getBlockMessageLimit() * 8;
                long bPos = AyscBufferHolder.getIns().bPos[idx] + sIdx * GlobalParams.getBlockMessageLimit() * GlobalParams.getBodySize();
                long[] tList = HelpUtil.readT(idx, tPos, tMsgAmount);
                long[] aList = HelpUtil.readA(false, idx, aPos, tMsgAmount);
                byte[][] bodyList = HelpUtil.readBody(idx, bPos, tMsgAmount);

                for (int j = 0; j < tMsgAmount; ++j) {
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

    public long easyFind3(long minT, long maxT, long minA, long maxA) {
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
                        && tMsgAmount < 1024 * 32) {
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
                    long tPos = AyscBufferHolder.getIns().tPos[idx] + sIdx * GlobalParams.getBlockMessageLimit() * 8;
                    long aPos = AyscBufferHolder.getIns().aPos[idx] + sIdx * GlobalParams.getBlockMessageLimit() * 8;
                    long[] tList = HelpUtil.readT(idx, tPos, tMsgAmount);
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

}
