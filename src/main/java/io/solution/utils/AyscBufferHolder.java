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

    private ByteBuffer[] taBuffers = new ByteBuffer[MAX_THREAD_AMOUNT];
    private ByteBuffer[] bBuffers = new ByteBuffer[MAX_THREAD_AMOUNT];

    private FileChannel[] taChannels = new FileChannel[MAX_THREAD_AMOUNT];
    private FileChannel[] bChannels = new FileChannel[MAX_THREAD_AMOUNT];

    private long[] minT = new long[MAX_THREAD_AMOUNT];
    private long[] maxT = new long[MAX_THREAD_AMOUNT];
    private long[] minA = new long[MAX_THREAD_AMOUNT];
    private long[] maxA = new long[MAX_THREAD_AMOUNT];
    private long[] sums = new long[MAX_THREAD_AMOUNT];

    // 目前消息提交数
    private int[] commitAmount = new int[MAX_THREAD_AMOUNT];
    // 目前块提交数
//    private int[] blockAMount = new int[MAX_THREAD_AMOUNT];

//    private int[] msgAmount = new int[MAX_THREAD_AMOUNT];

    private ArrayList<ArrayList<Long>> aLists = new ArrayList<>();

    private long[][] tList = new long[MAX_THREAD_AMOUNT][getBlockMessageLimit()];

//    public ArrayList<ArrayList<HashData>> hashInfos = new ArrayList<>(MAX_THREAD_AMOUNT);

    private int blockSize = 0;

    double[] wLines = new double[A_RANGE];

    private AyscBufferHolder() {

        for (int i = 0; i < A_MOD; ++i) wLines[i] = 0;
        wLines[A_MOD] = 1.0 * Long.MAX_VALUE;

        for (int i = 0; i < MAX_THREAD_AMOUNT; ++i) {

            aLists.add(new ArrayList<>());
//            hashInfos.add(new ArrayList<>());
            taBuffers[i] = ByteBuffer.allocateDirect(16 * getBlockMessageLimit());
            bBuffers[i] = ByteBuffer.allocateDirect(getBodySize() * getBlockMessageLimit());
            minT[i] = minA[i] = Long.MAX_VALUE;
            maxT[i] = maxA[i] = Long.MIN_VALUE;
            try {

                Path pathTA = GlobalParams.getTAPath(i);
                taChannels[i] = FileChannel.open(
                        pathTA,
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

        taBuffers[idx].putLong(message.getT());
        taBuffers[idx].putLong(message.getA());
        bBuffers[idx].put(message.getBody());

        minT[idx] = Math.min(minT[idx], message.getT());
        maxT[idx] = Math.max(maxT[idx], message.getT());
        minA[idx] = Math.min(minA[idx], message.getA());
        maxA[idx] = Math.max(maxA[idx], message.getA());
        sums[idx] += message.getA();
//        msgAmount[idx]++;
        commitAmount[idx]++;

        if (commitAmount[idx] == getBlockMessageLimit()) {

//            blockAMount[idx]++;
            MyHash.getIns().insert(idx, minT[idx], maxT[idx], minA[idx], maxA[idx], sums[idx]);

            minT[idx] = minA[idx] = Long.MAX_VALUE;
            maxT[idx] = maxA[idx] = Long.MIN_VALUE;
            sums[idx] = 0;

            // 值域划分计算
            aLists.get(idx).sort(Long::compare);
            for (int i = 0; i < A_MOD; ++i) {
                wLines[i] = (wLines[i] * blockSize + aLists.get(idx).get(i << 1)) / (1.0 + blockSize);
            }
            aLists.get(idx).clear();
            blockSize++;

            // 对t做hash
//            HashData data = new HashData();
//            data.encode(tList[idx], commitAmount[idx]);
//            hashInfos.get(idx).add(data);

//            if (blockAMount[idx] == WRITE_COMMIT_COUNT_LIMIT) {
            // 对 t & a & body 写入文件
            try {
                taBuffers[idx].flip();
                taChannels[idx].write(taBuffers[idx]);
                taBuffers[idx].clear();

                bBuffers[idx].flip();
                bChannels[idx].write(bBuffers[idx]);
                bBuffers[idx].clear();
            } catch (IOException e) {
                e.printStackTrace();
            }
//                blockAMount[idx] = 0;
//            }
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
//                    hashInfos.get(i).add(data);
                } else {
                    MyHash.getIns().lastMsgAmount[i] = getBlockMessageLimit();
                }

//                if (blockAMount[i] > 0) {
                taBuffers[i].flip();
                taChannels[i].write(taBuffers[i]);
                taBuffers[i].clear();

                bBuffers[i].flip();
                bChannels[i].write(bBuffers[i]);
                bBuffers[i].clear();
//                }

                taBuffers[i] = null;
                bBuffers[i] = null;

                if (taChannels[i] != null) {
                    taChannels[i].close();
                    taChannels[i] = null;
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
        System.out.print("[");
        for (int i = 0; i < A_RANGE; ++i) {
            System.out.print(String.format("%.2f", wLines[i]) + ",");
        }
        System.out.println("]");
//        System.out.println("消息总量:" + totalMsgAmount);
        System.out.println("flush 结束~ 第一阶段耗时:" + (System.currentTimeMillis() - beginTime) + "(ms)");
//        System.out.println("Rest memory:" + (Runtime.getRuntime().maxMemory()) / (1024 * 1024) + "(M)");
//        System.out.println("Rest memory:" + (Runtime.getRuntime().totalMemory()) / (1024 * 1024) + "(M)");
        System.out.println("Rest memory:" + (Runtime.getRuntime().maxMemory() - Runtime.getRuntime().totalMemory() + Runtime.getRuntime().freeMemory()) / (1024 * 1024) + "(M)");
        PretreatmentHolder.getIns().work();
        GlobalParams.setStepOneFinished();
    }

}
