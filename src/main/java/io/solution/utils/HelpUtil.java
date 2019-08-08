package io.solution.utils;

import io.openmessaging.Message;
import io.solution.GlobalParams;
import io.solution.data.MyBlock;

import java.nio.ByteBuffer;
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
        if (maxT < minT2) return false;
        if (maxT2 < minT) return false;
        if (maxA < minA2) return false;
        if (maxA2 < minA) return false;
        return true;
    }

    public static boolean inSide(long t, long a,
                                 long minT, long maxT, long minA, long maxA
                                 ) {
        return a <= maxA && a >= minA && t <= maxT && t >= minT;
    }


    public static List<Message> transToList(ByteBuffer buffer, int size) {
        List<Message> messages = new ArrayList<>();
        for (int i = 0; i < size; ++i) {
            int ps = i * GlobalParams.PAGE_SIZE + 36;
            for (int j = 0; j < GlobalParams.getMessageSize(); ++j) {
                int p = ps + GlobalParams.getMessageSize() * j;
                long t = buffer.getLong(p);
                long a = buffer.getLong(p + 8);
                byte[] body = new byte[GlobalParams.getBodySize()];
                buffer = buffer.get(body);
                Message message = new Message(a, t, body);
                messages.add(message);
            }
        }
        return messages;
    }
}
