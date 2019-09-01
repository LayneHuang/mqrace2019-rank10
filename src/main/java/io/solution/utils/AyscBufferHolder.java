package io.solution.utils;

import io.openmessaging.Message;
import io.solution.GlobalParams;
import io.solution.data.HashData;
import io.solution.map.MyHash;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import static io.solution.GlobalParams.*;

public class AyscBufferHolder {

    private static AyscBufferHolder ins = new AyscBufferHolder();

    private ByteBuffer[] aBuffers = new ByteBuffer[MAX_THREAD_AMOUNT];
    private ByteBuffer[] bBuffers = new ByteBuffer[MAX_THREAD_AMOUNT];

    private FileChannel[] aChannels = new FileChannel[MAX_THREAD_AMOUNT];
    private FileChannel[] bChannels = new FileChannel[MAX_THREAD_AMOUNT];

    private long[] minT = new long[MAX_THREAD_AMOUNT];
    private long[] maxT = new long[MAX_THREAD_AMOUNT];
    private long[] minA = new long[MAX_THREAD_AMOUNT];
    private long[] maxA = new long[MAX_THREAD_AMOUNT];
    private long[] sums = new long[MAX_THREAD_AMOUNT];

    private int[] commitAmount = new int[MAX_THREAD_AMOUNT];
    private int[] msgAmount = new int[MAX_THREAD_AMOUNT];

    private ArrayList<ArrayList<Long>> aLists = new ArrayList<>();

    private long[][] tList = new long[MAX_THREAD_AMOUNT][getBlockMessageLimit()];

    public ArrayList<ArrayList<HashData>> hashInfos = new ArrayList<>(MAX_THREAD_AMOUNT);

    private int blockSize = 0;

    public long[] wLines = new long[A_RANGE];

    private AyscBufferHolder() {
        for (int i = 0; i < A_MOD; ++i) {
            wLines[i] = 0;
        }
        wLines[A_MOD] = Long.MAX_VALUE;

        for (int i = 0; i < MAX_THREAD_AMOUNT; ++i) {

            aLists.add(new ArrayList<>());
            hashInfos.add(new ArrayList<>());
            aBuffers[i] = ByteBuffer.allocateDirect(8 * getBlockMessageLimit());
            bBuffers[i] = ByteBuffer.allocateDirect(getBodySize() * getBlockMessageLimit());
            minT[i] = minA[i] = Long.MAX_VALUE;
            maxT[i] = maxA[i] = Long.MIN_VALUE;
            try {

                Path pathA = GlobalParams.getAPath(i, false);
                aChannels[i] = FileChannel.open(
                        pathA,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.APPEND
                );

                Path pathB = GlobalParams.getBPath(i);
                bChannels[i] = FileChannel.open(
                        pathB,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.APPEND
                );

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static AyscBufferHolder getIns() {
        return ins;
    }

    private ConcurrentHashMap<Long, Integer> indexMap = new ConcurrentHashMap<>();

    private static final String HEAP_CREATE_LOCK = "HEAP_CREATE_LOCK";

    private int total = 0;

    private long beginTime = 0;

    private int getIndex(long threadId) {
        if (indexMap.containsKey(threadId)) {
            return indexMap.get(threadId);
        } else {
            synchronized (HEAP_CREATE_LOCK) {
                if (indexMap.containsKey(threadId)) {
                    return indexMap.get(threadId);
                }
                int index = total++;
                if (index == 0) {
                    beginTime = System.currentTimeMillis();
                }
                indexMap.put(threadId, index);
                return index;
            }
        }
    }

    public void commit(long threadId, Message message) {
        int idx = getIndex(threadId);

        aLists.get(idx).add(message.getA());
        tList[idx][commitAmount[idx]] = message.getT();

        aBuffers[idx].putLong(message.getA());
        bBuffers[idx].put(message.getBody());

        minT[idx] = Math.min(minT[idx], message.getT());
        maxT[idx] = Math.max(maxT[idx], message.getT());
        minA[idx] = Math.min(minA[idx], message.getA());
        maxA[idx] = Math.max(maxA[idx], message.getA());
        sums[idx] += message.getA();
        msgAmount[idx]++;
        commitAmount[idx]++;

        if (commitAmount[idx] == getBlockMessageLimit()) {

            MyHash.getIns().insert(idx, minT[idx], maxT[idx], minA[idx], maxA[idx], sums[idx]);

            minT[idx] = minA[idx] = Long.MAX_VALUE;
            maxT[idx] = maxA[idx] = Long.MIN_VALUE;
            sums[idx] = 0;

            aLists.get(idx).sort(Long::compare);
            int size = aLists.get(idx).size();
            int distance = (size / A_MOD);

            for (int i = 0; i < A_MOD; ++i) {
                int pos = i * distance;
                wLines[i] = (long) ((1.0 * wLines[i] * blockSize + aLists.get(idx).get(pos)) / (1.0 + blockSize));
            }
            aLists.get(idx).clear();

            blockSize++;
            try {
                HashData data = new HashData();
                data.encode(tList[idx], commitAmount[idx]);
                hashInfos.get(idx).add(data);

                aBuffers[idx].flip();
                aChannels[idx].write(aBuffers[idx]);
                aBuffers[idx].clear();

                bBuffers[idx].flip();
                bChannels[idx].write(bBuffers[idx]);
                bBuffers[idx].clear();
            } catch (IOException e) {
                e.printStackTrace();
            }
            commitAmount[idx] = 0;
        }

    }

    public synchronized void flush() {
        if (GlobalParams.isStepOneFinished()) {
            return;
        }
        System.out.println("flush 开始~");
        try {

            for (int i = 0; i < total; ++i) {
                if (commitAmount[i] > 0) {

                    MyHash.getIns().lastMsgAmount[i] = commitAmount[i];
                    MyHash.getIns().insert(i, minT[i], maxT[i], minA[i], maxA[i], sums[i]);
//                    MyHash.getIns().insert(i, minT[i], maxT[i], minA[i], maxA[i]);
                    HashData data = new HashData();
                    data.encode(tList[i], commitAmount[i]);
                    hashInfos.get(i).add(data);

                    aBuffers[i].flip();
                    aChannels[i].write(aBuffers[i]);
                    aBuffers[i].clear();

                    bBuffers[i].flip();
                    bChannels[i].write(bBuffers[i]);
                    bBuffers[i].clear();
                } else {
                    MyHash.getIns().lastMsgAmount[i] = getBlockMessageLimit();
                }
//                tBuffers[i] = null;
                aBuffers[i] = null;
                bBuffers[i] = null;

                if (aChannels[i] != null) {
                    aChannels[i].close();
                    aChannels[i] = null;
                }
                if (bChannels[i] != null) {
                    bChannels[i].close();
                    bChannels[i] = null;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

//        int totalMsgAmount = 0;
//        for (int i = 0; i < total; ++i) {
//            totalMsgAmount += msgAmount[i];
//        }
//        System.out.print("[");
//        for (int i = 0; i < A_RANGE; ++i) {
//            System.out.print(wLines[i] + ",");
//        }
//        System.out.println("]");
//        System.out.println("消息总量:" + totalMsgAmount);
        System.out.println("flush 结束~ 第一阶段耗时:" + (System.currentTimeMillis() - beginTime) + "(ms)");
//        System.out.println("Rest memory:" + (Runtime.getRuntime().maxMemory()) / (1024 * 1024) + "(M)");
//        System.out.println("Rest memory:" + (Runtime.getRuntime().totalMemory()) / (1024 * 1024) + "(M)");
        System.out.println("Rest memory:" + (Runtime.getRuntime().maxMemory() - Runtime.getRuntime().totalMemory() + Runtime.getRuntime().freeMemory()) / (1024 * 1024) + "(M)");
        PretreatmentHolder.getIns().work();
        GlobalParams.setStepOneFinished();
    }

}
