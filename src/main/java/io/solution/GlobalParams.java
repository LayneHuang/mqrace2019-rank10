package io.solution;

/**
 * @Author: laynehuang
 * @CreatedAt: 2019/8/1 0001
 */
public class GlobalParams {

    public static final int PAGE_SIZE = 4 * 1024;

    public static final int BLOCK_SIZE = 8 * 1024 * 1024;

    public static final int CIRCLE_BUFFER_SIZE = 128 * 1024 * 1024;

    public static final int CIRCLE_BUFFER_COUNT = 4;

    public static final int FOUR_GB = 4 * 1024 * 1024 * 1024;

    public static final int BLOCK_SIZE_LIMIT = BLOCK_SIZE / PAGE_SIZE;

    /**
     * 拥塞队列的大小
     */
    public static final int BLOCK_COUNT_LIMIT = FOUR_GB / BLOCK_SIZE;

    /**
     * 能够写入缓存时候的合法数量
     */
    public static final int WRITE_COUNT_LIMIT = CIRCLE_BUFFER_SIZE / BLOCK_SIZE;


    public static int getQueueLimit() {
        return Math.floorDiv(BLOCK_SIZE, 50);
    }

}
