package io.solution.data;

/**
 * 游标,多线程读块使用
 *
 * @Author: laynehuang
 * @CreatedAt: 2019/8/12 0012
 */
public class MyCursor {

    private int pos;

    private byte[] bytes;

    public int getPos() {
        return pos;
    }

    public void setPos(int pos) {
        this.pos = pos;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public void setBytes(byte[] bytes) {
        this.bytes = bytes;
    }

}
