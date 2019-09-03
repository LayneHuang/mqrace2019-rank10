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
    // 每个线程消息总数
//    private int[] msgAmount = new int[MAX_THREAD_AMOUNT];
    // buffer中块的数量
    private int[] sizeInBuffer = new int[MAX_THREAD_AMOUNT];

    private ArrayList<ArrayList<Long>> aLists = new ArrayList<>();

    private int blockSize = 0;

    double[] wLines = new double[A_RANGE];

    private AyscBufferHolder() {

        for (int i = 0; i < A_MOD; ++i) wLines[i] = 0;
        wLines[A_MOD] = 1.0 * Long.MAX_VALUE;

        for (int i = 0; i < MAX_THREAD_AMOUNT; ++i) {

            aLists.add(new ArrayList<>());
            taBuffers[i] = ByteBuffer.allocateDirect(16 * getBlockMessageLimit() * WRITE_COMMIT_COUNT_LIMIT);
            bBuffers[i] = ByteBuffer.allocateDirect(getBodySize() * getBlockMessageLimit() * WRITE_COMMIT_COUNT_LIMIT);
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

//    private long beginTime = 0;

    private int getIndex(long threadId) {
        if (indexMap.containsKey(threadId)) {
            return indexMap.get(threadId);
        } else {
            synchronized (HEAP_CREATE_LOCK) {
                if (indexMap.containsKey(threadId)) {
                    return indexMap.get(threadId);
                }
                int index = total++;
//                if (index == 0) {
//                    beginTime = System.currentTimeMillis();
//                }
                indexMap.put(threadId, index);
                return index;
            }
        }
    }

    public void commit(long threadId, Message message) {
        int idx = getIndex(threadId);

        aLists.get(idx).add(message.getA());
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
            MyHash.getIns().insert2(idx, minT[idx], maxT[idx], minA[idx], maxA[idx], sums[idx]);

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

            // 对 t & a & body 写入文件
            sizeInBuffer[idx]++;
            if (sizeInBuffer[idx] == WRITE_COMMIT_COUNT_LIMIT) {
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
                sizeInBuffer[idx] = 0;
            }
            commitAmount[idx] = 0;
        }

    }

    public synchronized void flush() {
        if (GlobalParams.isStepOneFinished()) {
            return;
        }
//        System.out.println("flush 开始~");
        try {

            for (int idx = 0; idx < total; ++idx) {

                if (commitAmount[idx] > 0) {
                    MyHash.getIns().lastMsgAmount[idx] = commitAmount[idx];
                    MyHash.getIns().insert2(idx, minT[idx], maxT[idx], minA[idx], maxA[idx], sums[idx]);
                    taBuffers[idx].flip();
                    taChannels[idx].write(taBuffers[idx]);
                    taBuffers[idx].clear();
                    bBuffers[idx].flip();
                    bChannels[idx].write(bBuffers[idx]);
                    bBuffers[idx].clear();
                } else {
                    MyHash.getIns().lastMsgAmount[idx] = getBlockMessageLimit();
                }

                // 清空buff
                taBuffers[idx] = null;
                bBuffers[idx] = null;
                if (taChannels[idx] != null) {
                    taChannels[idx].close();
                    taChannels[idx] = null;
                }
                if (bChannels[idx] != null) {
                    bChannels[idx].close();
                    bChannels[idx] = null;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

//        System.out.print("[");
//        for (int i = 0; i < A_RANGE; ++i) {
//            System.out.print(String.format("%.2f", wLines[i]) + ",");
//        }
//        System.out.println("]");
//        System.out.println("消息总量:" + totalMsgAmount);
//        System.out.println("flush 结束~ 第一阶段耗时:" + (System.currentTimeMillis() - beginTime) + "(ms)");
//        System.out.println("Rest memory:" + (Runtime.getRuntime().maxMemory() - Runtime.getRuntime().totalMemory() + Runtime.getRuntime().freeMemory()) / (1024 * 1024) + "(M)");
        PretreatmentHolder.getIns().work();
        GlobalParams.setStepOneFinished();
    }

}
