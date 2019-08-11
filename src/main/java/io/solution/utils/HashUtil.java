package io.solution.utils;

import io.solution.data.BlockInfo;

import java.io.IOException;
import java.util.Arrays;

/**
 * @Author: laynehuang
 * @CreatedAt: 2019/8/10 0010
 */
public class HashUtil {

    // zig zag (tested)
    public static int readInt(BlockInfo blockInfo, boolean isReadT) throws IOException {

        int pos = (isReadT ? blockInfo.getPosT() : blockInfo.getPosA());
        byte[] buf = (isReadT ? blockInfo.getDataT() : blockInfo.getDataA());

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
        if (isReadT) {
            blockInfo.setPosT(pos);
            blockInfo.setDataT(buf);
        } else {
            blockInfo.setPosA(pos);
            blockInfo.setDataA(buf);
        }
        return (n >>> 1) ^ -(n & 1); // back to two's-complement
    }

    public static int encodeInt(int n, BlockInfo blockInfo, boolean isHashT) {
// move sign to low-order bit, and flip others if negative
        int pos = (isHashT ? blockInfo.getSizeT() : blockInfo.getSizeA());
        byte[] buf = (isHashT ? blockInfo.getDataT() : blockInfo.getDataA());
        int limit = (isHashT ? blockInfo.getLimitT() : blockInfo.getLimitA());

        // 越界扩容
        if (pos + 4 >= limit) {
            limit += (limit >> 1);
            buf = Arrays.copyOf(buf, limit);
            if (isHashT) {
                blockInfo.setLimitT(limit);
            } else {
                blockInfo.setLimitA(limit);
            }
        }

        n = (n << 1) ^ (n >> 31);
        int start = pos;
        if ((n & ~0x7F) != 0) {
            buf[pos++] = (byte) ((n | 0x80) & 0xFF);
            n >>>= 7;
            if (n > 0x7F) {
                buf[pos++] = (byte) ((n | 0x80) & 0xFF);
                n >>>= 7;
                if (n > 0x7F) {
                    buf[pos++] = (byte) ((n | 0x80) & 0xFF);
                    n >>>= 7;
                    if (n > 0x7F) {
                        buf[pos++] = (byte) ((n | 0x80) & 0xFF);
                        n >>>= 7;
                    }
                }
            }
        }
        buf[pos++] = (byte) n;
        if (isHashT) {
            blockInfo.setSizeT(pos);
            blockInfo.setDataT(buf);
        } else {
            blockInfo.setSizeA(pos);
            blockInfo.setDataA(buf);
        }
        return pos - start;
    }

}
