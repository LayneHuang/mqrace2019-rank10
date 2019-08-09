package io.solution.utils;

import io.openmessaging.Message;
import io.solution.GlobalParams;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: laynehuang
 * @CreatedAt: 2019/8/8 0008
 */
public class HelpUtil {

    /**
     * 判矩阵相交
     */
    public static boolean intersect(
            long minT, long maxT, long minA, long maxA,
            long minT2, long maxT2, long minA2, long maxA2
    ) {
        return !(maxT < minT2 || maxT2 < minT || maxA < minA2 || maxA2 < minA);
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

    public static List<Message> transToList(ByteBuffer buffer, int size) {
        List<Message> messages = new ArrayList<>();
        for (int i = 0; i < size; ++i) {
            int pSize = buffer.getInt();
            for (int j = 0; j < pSize; ++j) {
                long t = buffer.getLong();
                long a = buffer.getLong();
                byte[] bd = new byte[GlobalParams.getBodySize()];
                buffer.get(bd);
                Message message = new Message(t, a, bd);
                messages.add(message);
            }
        }
        return messages;
    }


    /**
     * 读取Block 中整个 message 列表
     */
    public static List<Message> readMessages(long position, int size) {
        FileChannel channel = null;
        List<Message> res = new ArrayList<>();
        try {
            channel = FileChannel.open(
                    GlobalParams.getPath(),
                    StandardOpenOption.READ
            );
            ByteBuffer buffer = ByteBuffer.allocateDirect(size);
            channel.read(buffer, position);
            buffer.flip();
            return transToList(buffer, size / GlobalParams.PAGE_SIZE);
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
