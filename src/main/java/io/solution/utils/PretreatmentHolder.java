package io.solution.utils;

import io.solution.GlobalParams;
import io.solution.data.MsgForThree;
import io.solution.map.MyHash;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;

/**
 * @Author: laynehuang
 * @CreatedAt: 2019/8/30 0030
 */
public class PretreatmentHolder {

    private static PretreatmentHolder ins = new PretreatmentHolder();

    public boolean isFinish = false;

    private FileChannel infoChannel;

    private ByteBuffer[] aBuffers = new ByteBuffer[GlobalParams.A_RANGE];

    private int[] aBufferSize = new int[GlobalParams.A_RANGE];

    private FileChannel[] channels = new FileChannel[GlobalParams.A_RANGE];
    private FileChannel atChannel;

    private long[] aPos = new long[GlobalParams.A_RANGE];

    private int[] cntSum = new int[GlobalParams.A_RANGE];

    // pre sum
    private int[] ks = new int[GlobalParams.A_RANGE];

    private long[] bs = new long[GlobalParams.A_RANGE];

    private int writeCount = 0;

    private ByteBuffer atBuffer = ByteBuffer.allocateDirect(16 * GlobalParams.getBlockMessageLimit());

    private ByteBuffer lineInfoBuffer = ByteBuffer.allocateDirect(
            GlobalParams.INFO_SIZE * GlobalParams.A_RANGE * GlobalParams.WRITE_COMMIT_COUNT_LIMIT
    );


    private PretreatmentHolder() {
        try {
            infoChannel = FileChannel.open(
                    GlobalParams.getInfoPath(),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND
            );

            atChannel = FileChannel.open(
                    GlobalParams.getATPath(),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND
            );

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

    private long sleepCost = 0;

    synchronized void work() {
        if (isFinish) {
            return;
        }
        Thread thread = new Thread(() -> {
            System.out.println("预处理开始~");
            long s0 = System.currentTimeMillis();
            try {
                int totalBlock = 0;
                int threadAmount = MyHash.getIns().size2;
                int[] blocksSize = new int[threadAmount];
                // 每条线程消息处理了的块的数量
                int[] preDealSize = new int[threadAmount];
                // 每条线程消息正在处理的块的消息偏移
                int[] nowDealPos = new int[threadAmount];
                // t数据
                MsgForThree[][] at = new MsgForThree[threadAmount][GlobalParams.getBlockMessageLimit()];
                for (int idx = 0; idx < threadAmount; ++idx) {
                    int nowAmount = (preDealSize[idx] == blocksSize[idx] - 1 ? MyHash.getIns().lastMsgAmount[idx] : GlobalParams.getBlockMessageLimit());
                    long[] tList = MyHash.readT(idx, 0, 0, nowAmount);
                    long[] aList = HelpUtil.readA(false, idx, 0, nowAmount);
                    for (int i = 0; i < nowAmount; ++i) {
                        at[idx][i] = new MsgForThree(tList[i], aList[i]);
                    }
                }

                MsgForThree[] msgs = new MsgForThree[GlobalParams.getBlockMessageLimit()];
                Queue<MsgForThree> queue = new PriorityQueue<>(Comparator.comparingLong(t0 -> t0.t));

//                int tt = 0;
                for (int idx = 0; idx < threadAmount; ++idx) {
                    blocksSize[idx] = MyHash.getIns().minTs2.get(idx).size();
                    totalBlock += blocksSize[idx];
                }
//                System.out.println("总块数:" + totalBlock + " tt:" + tt);
                int finishSize = 0;
//                int pollCnt = 0;
//                int addCnt = 0;

                while (finishSize < totalBlock) {
                    // 选线程中最大值最小的块
                    long minOfMaxT = Long.MAX_VALUE;

                    for (int idx = 0; idx < threadAmount; ++idx) {
                        int nowPos = preDealSize[idx];
                        if (nowPos < blocksSize[idx]) {
                            long maxT = MyHash.getIns().maxTs2.get(idx).get(nowPos);
                            if (maxT < minOfMaxT) {
                                minOfMaxT = maxT;
                            }
                        }
                    }

                    // 把所有小于等于这个minOfMax的t放进队列 (最多thread amount个块)
                    for (int idx = 0; idx < threadAmount; ++idx) {
                        // 处理完的线程过掉
                        if (preDealSize[idx] >= blocksSize[idx]) {
                            continue;
                        }
                        int amount = (preDealSize[idx] == blocksSize[idx] - 1 ? MyHash.getIns().lastMsgAmount[idx] : GlobalParams.getBlockMessageLimit());
                        while (nowDealPos[idx] < amount && at[idx][nowDealPos[idx]].t <= minOfMaxT) {
                            queue.add(new MsgForThree(at[idx][nowDealPos[idx]].t, at[idx][nowDealPos[idx]].a));
                            nowDealPos[idx]++;
//                            addCnt++;
                        }

                        // 某块插入完了
                        if (nowDealPos[idx] >= amount) {
                            finishSize++;
                            nowDealPos[idx] = 0;
                            preDealSize[idx]++;
                            // 重新读入一块新块
                            if (preDealSize[idx] < blocksSize[idx]) {
                                int nowAmount = (preDealSize[idx] == blocksSize[idx] - 1 ? MyHash.getIns().lastMsgAmount[idx] : GlobalParams.getBlockMessageLimit());
                                long[] tList = MyHash.readT(idx, preDealSize[idx], preDealSize[idx], nowAmount);
                                long[] aList = HelpUtil.readA(false, idx, AyscBufferHolder.getIns().aPos[idx] + (long) preDealSize[idx] * GlobalParams.getBlockMessageLimit() * 8, nowAmount);
                                for (int i = 0; i < nowAmount; ++i) {
                                    at[idx][i].t = tList[i];
                                    at[idx][i].a = aList[i];
                                }
                            }
                        }
                    }

                    // 组成新块重新写
                    while (queue.size() >= GlobalParams.getBlockMessageLimit()) {
                        for (int i = 0; i < GlobalParams.getBlockMessageLimit(); ++i) {
                            msgs[i] = queue.poll();
//                            pollCnt++;
                        }
                        buildBlock(msgs, GlobalParams.getBlockMessageLimit());
                    }

                    if (MyHash.getIns().size3 % GlobalParams.WRITE_COMMIT_COUNT_LIMIT == 0) {
                        try {
                            Thread.sleep(1000);
                            sleepCost += 1000;
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
                // 处理剩余a t 落盘
                if (!queue.isEmpty()) {
                    int tempSize = 0;
                    while (!queue.isEmpty()) {
                        msgs[tempSize++] = queue.poll();
//                        pollCnt++;
                    }
                    buildBlock(msgs, tempSize);
                }

//                System.out.println("入队数:" + addCnt + " 出队数:" + pollCnt);

                // 处理剩余横向a
                for (int i = 0; i < GlobalParams.A_RANGE; ++i) {
                    if (aBufferSize[i] > 0) {
                        aBuffers[i].flip();
                        channels[i].write(aBuffers[i]);
                        aBufferSize[i] = 0;
                    }
                    aBuffers[i].clear();
                }

                // 处理剩余info
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
                System.out.println("预处理结束~ cost:" + (System.currentTimeMillis() - s0) + "(ms) 睡眠消耗:" + sleepCost);
                System.out.println("共处理块数:" + MyHash.getIns().size3);
                System.out.print("[");
                for (int i = 0; i < GlobalParams.A_RANGE; ++i) {
                    System.out.print(cntSum[i] + ",");
                }
                System.out.println("]");
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        thread.start();
    }

    private void buildBlock(MsgForThree[] msgForThrees, int size) {
        long minT = Long.MAX_VALUE;
        long maxT = Long.MIN_VALUE;
        long minA = Long.MAX_VALUE;
        long maxA = Long.MIN_VALUE;
        long sum = 0;

        // 记录&处理竖线位置
        for (int i = 0; i < GlobalParams.A_RANGE; ++i) {
            lineInfoBuffer.putLong(aPos[i]);
            lineInfoBuffer.putInt(cntSum[i]);
            lineInfoBuffer.putInt(ks[i]);
            lineInfoBuffer.putLong(bs[i]);
        }

        writeCount++;
        // 写到文件中
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

        for (int i = 0; i < size; ++i) {
            MsgForThree msg = msgForThrees[i];
            atBuffer.putLong(msg.t);
            atBuffer.putLong(msg.a);
            sum += msg.a;
            minT = Math.min(minT, msg.t);
            maxT = Math.max(maxT, msg.t);
            minA = Math.min(minA, msg.a);
            maxA = Math.max(maxA, msg.a);

            // 处理竖线数据
            int pos = HelpUtil.getPosition(msg.a);
            aBuffers[pos].putLong(msg.a);
            aBufferSize[pos]++;
            if (aBufferSize[pos] == 1024) {
                try {
                    aBuffers[pos].flip();
                    channels[pos].write(aBuffers[pos]);
                    aBuffers[pos].clear();
                    aBufferSize[pos] = 0;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            aPos[pos] += 8;
            cntSum[pos]++;
            if (Long.MAX_VALUE - bs[pos] < msg.a) {
                ks[pos]++;
                bs[pos] = msg.a - (Long.MAX_VALUE - bs[pos]);
            } else {
                bs[pos] += msg.a;
            }
        }
        if (size != GlobalParams.getBlockMessageLimit()) {
//            System.out.println("cnt:" + (MyHash.getIns().size3  * GlobalParams.getBlockMessageLimit() + size));
            MyHash.getIns().lastMsgAmount3 = size;
        }
        MyHash.getIns().insert3(minT, maxT, minA, maxA, sum);

        try {
            atBuffer.flip();
            atChannel.write(atBuffer);
            atBuffer.clear();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
