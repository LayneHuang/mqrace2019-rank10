package io.solution.utils;

import io.solution.GlobalParams;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

/**
 * @Author: laynehuang
 * @CreatedAt: 2019/8/8 0008
 */
public class HelpUtil {

    /**
     * 判矩阵相交
     */
    public static boolean intersect(
            long queryT1, long queryT2, long queryA1, long queryA2,
            long blockT1, long blockT2, long blockA1, long blockA2
    ) {
        return !(queryT2 < blockT1 || blockT2 < queryT1 || queryA2 < blockA1 || blockA2 < queryA1);
    }

    /**
     * 判矩阵包含
     */
    public static boolean matrixInside(
            long queryT1, long queryT2, long queryA1, long queryA2,
            long blockT1, long blockT2, long blockA1, long blockA2
    ) {
        return queryT1 <= blockT1 && blockT2 <= queryT2 && queryA1 <= blockA1 && blockA2 <= queryA2;
    }

    public static boolean inSide(long t, long a,
                                 long minT, long maxT, long minA, long maxA
    ) {
        return a <= maxA && a >= minA && t <= maxT && t >= minT;
    }

    /**
     * 只写入body的情况
     * 读取Block 中 message 列表
     */
    public static byte[][] readBody(long position, int messageCount) {
        int size = messageCount * GlobalParams.getBodySize();
        FileChannel channel = null;
        byte[][] res = new byte[messageCount][GlobalParams.getBodySize()];

        try {
            channel = FileChannel.open(
                    GlobalParams.getPath(2),
                    StandardOpenOption.READ
            );
            ByteBuffer buffer = ByteBuffer.allocateDirect(size);
            channel.read(buffer, position);
            buffer.flip();

            // trans
            for (int i = 0; i < messageCount; ++i) {
                buffer.get(res[i], 0, GlobalParams.getBodySize());
            }

            buffer.clear();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (channel != null) {
                    channel.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return res;
    }

    public static long[] readA(long position, int messageCount) {
        int size = messageCount * 8;
        FileChannel channel = null;
        long[] res = new long[messageCount];

        try {
            channel = FileChannel.open(
                    GlobalParams.getPath(1),
                    StandardOpenOption.READ
            );
            ByteBuffer buffer = ByteBuffer.allocateDirect(size);
            channel.read(buffer, position);
            buffer.flip();

            // trans
            for (int i = 0; i < messageCount; ++i) {
                res[i] = buffer.getLong();
            }

            buffer.clear();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (channel != null) {
                    channel.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return res;
    }

    public static long[] readT(long position, int messageCount) {
        int size = messageCount * 8;
        FileChannel channel = null;
        long[] res = new long[messageCount];

        try {
            channel = FileChannel.open(
                    GlobalParams.getPath(0),
                    StandardOpenOption.READ
            );
            ByteBuffer buffer = ByteBuffer.allocateDirect(size);
            channel.read(buffer, position);
            buffer.flip();

            // trans
            for (int i = 0; i < messageCount; ++i) {
                res[i] = buffer.getLong();
            }

            buffer.clear();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (channel != null) {
                    channel.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return res;
    }

}
