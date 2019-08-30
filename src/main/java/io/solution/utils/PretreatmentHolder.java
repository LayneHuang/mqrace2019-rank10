package io.solution.utils;

import io.solution.GlobalParams;
import io.solution.map.MyHash;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * @Author: laynehuang
 * @CreatedAt: 2019/8/30 0030
 */
class PretreatmentHolder {

    private static PretreatmentHolder ins = new PretreatmentHolder();

    private boolean isFinish = false;

    private FileChannel infoChannel;

    private long infoPos;

    private ByteBuffer[] aBuffers = new ByteBuffer[GlobalParams.A_RANGE];

    private int[] aBufferSize = new int[GlobalParams.A_RANGE];

    private FileChannel[] channels = new FileChannel[GlobalParams.A_RANGE];

    private long[] aPos = new long[GlobalParams.A_RANGE];

    private int[] cntSum = new int[GlobalParams.A_RANGE];

    // pre sum
    private int[] ks = new int[GlobalParams.A_RANGE];

    private long[] bs = new long[GlobalParams.A_RANGE];

    private int writeCount = 0;

    private ByteBuffer lineInfoBuffer = ByteBuffer.allocateDirect(
            GlobalParams.INFO_SIZE * GlobalParams.A_RANGE * GlobalParams.WRITE_COMMIT_COUNT_LIMIT
    );

    /**
     * 每条线程消息处理了的块的数量
     */
    private int[] preDealSize = new int[GlobalParams.MAX_THREAD_AMOUNT];

    /**
     * 每条线程消息正在处理的块的消息便宜
     */
    private int[] nowDealSize = new int[GlobalParams.MAX_THREAD_AMOUNT];

    private PretreatmentHolder() {
        try {
            infoChannel = FileChannel.open(
                    GlobalParams.getInfoPath(),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND
            );
            infoPos = infoChannel.position();
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < GlobalParams.A_RANGE; ++i) {
            aBuffers[i] = ByteBuffer.allocateDirect(GlobalParams.EIGHT_K * 2);
            Path path = GlobalParams.getAPath(i, true);
            try {
                channels[i] = FileChannel.open(
                        path,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.APPEND
                );
                aPos[i] = channels[i].position();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static PretreatmentHolder getIns() {
        return ins;
    }

    synchronized void work() {
        if (isFinish) {
            return;
        }
        Thread thread = new Thread(() -> {
            System.out.println("预处理开始~");
            long s0 = System.currentTimeMillis();
            try {

                long sn = System.nanoTime();
                int totalBlock = 0;
                for (int i = 0; i < MyHash.getIns().size2; ++i) {
                    totalBlock += MyHash.getIns().minTs2.size();
                }
                System.out.println("总块数:" + totalBlock + " 求和耗时:" + (System.nanoTime() - sn));
                int nowSize = 0;

                while (nowSize < totalBlock) {


                }


                for (int i = 0; i < MyHash.getIns().size; ++i) {
                    // 前缀
//                    MyHash.getIns().infoPos[i] = infoPos;
                    infoPos += GlobalParams.INFO_SIZE * GlobalParams.A_RANGE;
                    for (int j = 0; j < GlobalParams.A_RANGE; ++j) {
                        lineInfoBuffer.putLong(aPos[j]);
                        lineInfoBuffer.putInt(cntSum[j]);
                        lineInfoBuffer.putInt(ks[j]);
                        lineInfoBuffer.putLong(bs[j]);
                    }
                    writeCount++;
                    if (writeCount == GlobalParams.WRITE_COMMIT_COUNT_LIMIT) {
                        writeCount = 0;
                        try {
                            lineInfoBuffer.flip();
                            infoChannel.write(lineInfoBuffer);
                            lineInfoBuffer.clear();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
//                    int cnt = MyHash0.getIns().msgAmount[i];
                    long[] ats = HelpUtil.readAT(MyHash0.getIns().posATs[i], cnt);

                    for (int j = 0; j < cnt; ++j) {
                        long a = ats[j * 2 + 1];
                        int pos = HelpUtil.getPosition2(a);
                        aBuffers[pos].putLong(a);
                        aBufferSize[pos]++;
                        if (aBufferSize[pos] == 1024) {
                            aBuffers[pos].flip();
                            channels[pos].write(aBuffers[pos]);
                            aBuffers[pos].clear();
                            aBufferSize[pos] = 0;
                        }
                        aPos[pos] += 8;
                        cntSum[pos]++;
                        if (Long.MAX_VALUE - bs[pos] < a) {
                            ks[pos]++;
                            bs[pos] = a - (Long.MAX_VALUE - bs[pos]);
                        } else {
                            bs[pos] += a;
                        }
                    }
                }
                for (int i = 0; i < GlobalParams.A_RANGE; ++i) {
                    if (aBufferSize[i] > 0) {
                        aBuffers[i].flip();
                        channels[i].write(aBuffers[i]);
                        aBufferSize[i] = 0;
                    }
                    aBuffers[i].clear();
                }
                if (writeCount > 0) {
                    lineInfoBuffer.flip();
                    infoChannel.write(lineInfoBuffer);
                    lineInfoBuffer.clear();
                }
                if (infoChannel != null) {
                    infoChannel.close();
                    infoChannel = null;
                }
                for (int i = 0; i < GlobalParams.A_RANGE; ++i) {
                    if (channels[i] != null) {
                        channels[i].close();
                        channels[i] = null;
                    }
                }
                isFinish = true;
                System.out.println("预处理结束~ cost:" + (System.currentTimeMillis() - s0) + "(ms)");
                System.out.print("[");
                for (int i = 0; i < GlobalParams.A_RANGE; ++i) {
                    System.out.print(cntSum[i] + ",");
                }
                System.out.print("]");
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        thread.start();
    }
}
