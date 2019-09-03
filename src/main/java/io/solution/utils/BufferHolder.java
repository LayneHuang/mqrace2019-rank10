package io.solution.utils;

import io.openmessaging.Message;
import io.solution.GlobalParams;
import io.solution.map.MyHash;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;

import static io.solution.GlobalParams.*;

public class BufferHolder {

    private ByteBuffer taBuffers;
    private ByteBuffer bBuffers;
    private FileChannel taChannels;
    private FileChannel bChannels;

    private long minT;
    private long maxT;
    private long minA;
    private long maxA;
    private long sums;

    // 目前消息提交数
    private int commitAmount;
    // buffer中块的数量
    private int sizeInBuffer;

    private ArrayList<Long> aList = new ArrayList<>();
    private ArrayList<Long> allAList = new ArrayList<>();

//    private int blockSize = 0;

    double[] wLines = new double[A_RANGE];

    private int id;

    BufferHolder(int id) {

        this.id = id;

        for (int i = 0; i < A_MOD; ++i) wLines[i] = 0;
        wLines[A_MOD] = 1.0 * Long.MAX_VALUE;

        taBuffers = ByteBuffer.allocateDirect(16 * getBlockMessageLimit() * WRITE_COMMIT_COUNT_LIMIT);
        bBuffers = ByteBuffer.allocateDirect(getBodySize() * getBlockMessageLimit() * WRITE_COMMIT_COUNT_LIMIT);
        minT = minA = Long.MAX_VALUE;
        maxT = maxA = Long.MIN_VALUE;

        try {
            Path pathTA = GlobalParams.getTAPath(id);
            taChannels = FileChannel.open(
                    pathTA,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND
            );

            Path pathB = GlobalParams.getBPath(id);
            bChannels = FileChannel.open(
                    pathB,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND
            );
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void commit(Message message) {

        aList.add(message.getA());
        taBuffers.putLong(message.getT());
        taBuffers.putLong(message.getA());
        bBuffers.put(message.getBody());

        minT = Math.min(minT, message.getT());
        maxT = Math.max(maxT, message.getT());
        minA = Math.min(minA, message.getA());
        maxA = Math.max(maxA, message.getA());
        sums += message.getA();
        commitAmount++;

        if (commitAmount == getBlockMessageLimit()) {
            MyHash.getIns().insert2(id, minT, maxT, minA, maxA, sums);

            minT = minA = Long.MAX_VALUE;
            maxT = maxA = Long.MIN_VALUE;
            sums = 0;

            // 值域划分计算
//            aList.sort(Long::compare);
//            for (int i = 0; i < A_MOD; ++i) {
//                wLines[i] = (wLines[i] * blockSize + aList.get(i << 1)) / (1.0 + blockSize);
//            }
//            aList.clear();
//            blockSize++;

            // 对 t & a & body 写入文件
            sizeInBuffer++;
            if (sizeInBuffer == WRITE_COMMIT_COUNT_LIMIT) {

                // 抽取集合部分
                aList.sort(Long::compare);
                for (int i = 0; i < aList.size(); i += A_DISTANCE) {
                    allAList.add(aList.get(i));
                }
                aList.clear();

                try {
                    taBuffers.flip();
                    taChannels.write(taBuffers);
                    taBuffers.clear();

                    bBuffers.flip();
                    bChannels.write(bBuffers);
                    bBuffers.clear();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                sizeInBuffer = 0;
            }
            commitAmount = 0;
        }

    }

    synchronized void flush() {
        try {
            if (commitAmount > 0) {
                MyHash.getIns().lastMsgAmount[id] = commitAmount;
                MyHash.getIns().insert2(id, minT, maxT, minA, maxA, sums);
                taBuffers.flip();
                taChannels.write(taBuffers);
                taBuffers.clear();
                bBuffers.flip();
                bChannels.write(bBuffers);
                bBuffers.clear();
                commitAmount = 0;
            } else {
                MyHash.getIns().lastMsgAmount[id] = getBlockMessageLimit();
            }
            // 清空buff
            taBuffers = null;
            bBuffers = null;
            if (taChannels != null) {
                taChannels.close();
                taChannels = null;
            }
            if (bChannels != null) {
                bChannels.close();
                bChannels = null;
            }

            allAList.sort(Long::compare);
            int size = allAList.size();
            int dis = size / (A_MOD - 1);
            for (int i = 0; i < A_MOD; ++i) {
                int pos = size - i * dis - 1;
                pos = Math.max(pos, 0);
                wLines[A_MOD - i - 1] = allAList.get(pos);
            }
            allAList.clear();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
