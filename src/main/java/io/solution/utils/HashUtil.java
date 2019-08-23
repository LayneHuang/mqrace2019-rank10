package io.solution.utils;

import io.solution.data.MyCursor;
import io.solution.data.PageInfo;
import io.solution.map.MyHash;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * zig zag (tested)
 *
 * @Author: laynehuang
 * @CreatedAt: 2019/8/10 0010
 */
public class HashUtil {

//    public static int readIntT(MyCursor cursor) throws IOException {
//        int pos = cursor.getPos();
//        ByteBuffer buffer = MyHash.getIns().gettBuffer();
//        int len = 1;
//        int b = buffer.get(pos) & 0xff;
//        int n = b & 0x7f;
//        if (b > 0x7f) {
//            b = buffer.get(pos + len++) & 0xff;
//            n ^= (b & 0x7f) << 7;
//            if (b > 0x7f) {
//                b = buffer.get(pos + len++) & 0xff;
//                n ^= (b & 0x7f) << 14;
//                if (b > 0x7f) {
//                    b = buffer.get(pos + len++) & 0xff;
//                    n ^= (b & 0x7f) << 21;
//                    if (b > 0x7f) {
//                        b = buffer.get(pos + len++) & 0xff;
//                        n ^= (b & 0x7f) << 28;
//                        if (b > 0x7f) {
//                            throw new IOException("Invalid int encoding");
//                        }
//                    }
//                }
//            }
//        }
//        pos += len;
//        cursor.setPos(pos);
//        return (n >>> 1) ^ -(n & 1); // back to two's-complement
//    }
//
//    public static int encodeInt(int n, PageInfo pageInfo, int pos) {
//
//        byte[] buf = pageInfo.getDataT();
//        int limit = pageInfo.getLimitT();
//
//        // 越界扩容
//        if (pos + 4 >= limit) {
//            limit += 1000;
//            buf = Arrays.copyOf(buf, limit);
//            pageInfo.setLimitT(limit);
//        }
//
//        n = (n << 1) ^ (n >> 31);
//        int start = pos;
//        if ((n & ~0x7F) != 0) {
//            buf[pos++] = (byte) ((n | 0x80) & 0xFF);
//            n >>>= 7;
//            if (n > 0x7F) {
//                buf[pos++] = (byte) ((n | 0x80) & 0xFF);
//                n >>>= 7;
//                if (n > 0x7F) {
//                    buf[pos++] = (byte) ((n | 0x80) & 0xFF);
//                    n >>>= 7;
//                    if (n > 0x7F) {
//                        buf[pos++] = (byte) ((n | 0x80) & 0xFF);
//                        n >>>= 7;
//                    }
//                }
//            }
//        }
//        buf[pos++] = (byte) n;
//        pageInfo.setSizeT(pos);
//        pageInfo.setDataT(buf);
//        return pos - start;
//    }

}
