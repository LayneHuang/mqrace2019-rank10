package io.solution;

/**
 * @Author: laynehuang
 * @CreatedAt: 2019/8/1 0001
 */
public class GlobalParams {

    public static final int MESSAGE_SIZE = 50;

    public static final int PAGE_SIZE = 4 * 1024;

    public static final int PAGE_MESSAGE_COUNT = 80;

    public static final long BLOCK_SIZE = 4 * 1024 * 1024;

    public static final long CIRCLE_BUFFER_SIZE = 128 * 1024 * 1024;

    public static final int CIRCLE_BUFFER_COUNT = 4;

    public static final long TWO_GB = 2L * 1024 * 1024 * 1024;

    public static final long BLOCK_SIZE_LIMIT = BLOCK_SIZE / PAGE_SIZE;

    /**
     * 拥塞队列的大小
     */
    public static final long BLOCK_COUNT_LIMIT = TWO_GB / BLOCK_SIZE;

    /**
     * 能够写入缓存时候的合法数量
     */
    public static final long WRITE_COUNT_LIMIT = CIRCLE_BUFFER_SIZE / BLOCK_SIZE;

    public static long getQueueLimit() {
        return Math.floorDiv(BLOCK_SIZE, PAGE_SIZE) * PAGE_MESSAGE_COUNT;
    }

    private static boolean isStepOneFinished = false;

    public static void setStepOneFinished() {
        isStepOneFinished = true;
    }

    public static boolean isStepOneFinished() {
        return isStepOneFinished;
    }

}
