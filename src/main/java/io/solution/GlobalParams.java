package io.solution;

/**
 * @Author: laynehuang
 * @CreatedAt: 2019/8/1 0001
 */
public class GlobalParams {

    public static final int PAGE_SIZE = 4 * 1024;

    public static final int BLOCK_SIZE = 8 * 1024 * 1024;

    public static final int CIRCLE_BUFFER_SIZE = 128 * 1024 * 1024;


    public static final int FOUR_GB = 4 * 1024 * 1024 * 1024;

    public static final int BLOCK_COUNT_LIMIT = FOUR_GB / BLOCK_SIZE;

    public static int getQueueLimit() {
        return Math.floorDiv(BLOCK_SIZE, 50);
    }

}
