package io.solution.utils;

import io.solution.GlobalParams;
import io.solution.data.LineInfo;
import io.solution.map.MyHash;

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

    public static long[] readAT(long position, int messageCount) {
        int size = messageCount * 16;
        FileChannel channel = null;
        long[] res = new long[messageCount * 2];

        try {
            channel = FileChannel.open(
                    GlobalParams.getPath(0),
                    StandardOpenOption.READ
            );
            ByteBuffer buffer = ByteBuffer.allocateDirect(size);
            channel.read(buffer, position);
            buffer.flip();

            // trans
            for (int i = 0; i < messageCount * 2; ++i) {
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

    public static LineInfo[] readLineInfo(long position) {
        FileChannel channel = null;
        LineInfo[] res = new LineInfo[GlobalParams.A_RANGE];

        try {
            channel = FileChannel.open(
                    GlobalParams.getInfoPath(),
                    StandardOpenOption.READ
            );

            ByteBuffer buffer = ByteBuffer.allocateDirect(GlobalParams.INFO_SIZE * GlobalParams.A_RANGE);
            channel.read(buffer, position);
            buffer.flip();
//            System.out.println("buffer limit:" + buffer.limit() + " " + (24 * GlobalParams.A_RANGE));

            // trans
            for (int i = 0; i < GlobalParams.A_RANGE; ++i) {
                LineInfo lineInfo = new LineInfo();
                lineInfo.aPos = buffer.getLong();
                lineInfo.cntSum = buffer.getInt();
                lineInfo.ks = buffer.getInt();
                lineInfo.bs = buffer.getLong();
                res[i] = lineInfo;
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

    public static long[] readA(int range, long position, int count) {
        int size = count * 8;
        FileChannel channel = null;
        long[] res = new long[count];
        try {
            channel = FileChannel.open(
                    GlobalParams.getAPath(range),
                    StandardOpenOption.READ
            );
            ByteBuffer buffer = ByteBuffer.allocateDirect(size);
            channel.read(buffer, position);
            buffer.flip();
            // trans
            for (int i = 0; i < count; ++i) {
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

    private static final long MAX_VALUE = 300000000000000L;

    public static int getPosition(long a) {
        long distance = Math.floorDiv(
                (GlobalParams.IS_DEBUG ? MyHash.getIns().aNowMaxValue : MAX_VALUE),
                GlobalParams.A_MOD
        ) + 1;

        return Math.min((int) (a / distance), GlobalParams.A_RANGE - 1);
    }

}
