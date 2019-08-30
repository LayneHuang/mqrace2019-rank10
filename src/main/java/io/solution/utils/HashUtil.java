package io.solution.utils;

import io.solution.data.HashData;
import io.solution.data.MyCursor;

import java.io.IOException;
import java.util.Arrays;

/**
 * @Author: laynehuang
 * @CreatedAt: 2019/8/10 0010
 */
public class HashUtil {

    // zig zag

    public static int readT(HashData data, MyCursor cursor) throws IOException {
        int pos = cursor.pos;
        byte[] buf = data.dataT;
        int len = 1;
        int b = buf[pos] & 0xff;
        int n = b & 0x7f;
        if (b > 0x7f) {
            b = buf[pos + len++] & 0xff;
            n ^= (b & 0x7f) << 7;
            if (b > 0x7f) {
                b = buf[pos + len++] & 0xff;
                n ^= (b & 0x7f) << 14;
                if (b > 0x7f) {
                    b = buf[pos + len++] & 0xff;
                    n ^= (b & 0x7f) << 21;
                    if (b > 0x7f) {
                        b = buf[pos + len++] & 0xff;
                        n ^= (b & 0x7f) << 28;
                        if (b > 0x7f) {
                            throw new IOException("Invalid int encoding");
                        }
                    }
                }
            }
        }
        pos += len;
        cursor.pos = pos;
        return (n >>> 1) ^ -(n & 1);
    }

    public static int encodeInt(int n, HashData data, int pos) {

        // 越界扩容
        if (pos + 4 >= data.dataSize) {
            data.dataSize += (data.dataSize >> 1);
            data.dataT = Arrays.copyOf(data.dataT, data.dataSize);
        }

        n = (n << 1) ^ (n >> 31);
        int start = pos;
        if ((n & ~0x7F) != 0) {
            data.dataT[pos++] = (byte) ((n | 0x80) & 0xFF);
            n >>>= 7;
            if (n > 0x7F) {
                data.dataT[pos++] = (byte) ((n | 0x80) & 0xFF);
                n >>>= 7;
                if (n > 0x7F) {
                    data.dataT[pos++] = (byte) ((n | 0x80) & 0xFF);
                    n >>>= 7;
                    if (n > 0x7F) {
                        data.dataT[pos++] = (byte) ((n | 0x80) & 0xFF);
                        n >>>= 7;
                    }
                }
            }
        }
        data.dataT[pos++] = (byte) n;
        return pos - start;
    }
}
