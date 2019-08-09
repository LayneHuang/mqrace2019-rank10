package io.solution;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @Author: laynehuang
 * @CreatedAt: 2019/8/1 0001
 */
public class GlobalParams {

    public static boolean IS_DEBUG = Boolean.valueOf(System.getProperty("debug", "false"));

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

    private static boolean isStepOneFinished = false;

    public static final Object FINISH_LOCK = new Object();

    public static void setStepOneFinished() {
        isStepOneFinished = true;
    }

    public static boolean isStepOneFinished() {
        return isStepOneFinished;
    }

    public static long getWriteCountLimit() {
        return IS_DEBUG ? WRITE_COUNT_LIMIT / 8 : WRITE_COUNT_LIMIT;
    }

    public static Path getPath() {
        Path path = Paths.get("/alidata1/race2019/data");
        if (GlobalParams.IS_DEBUG) {
            path = Paths.get(System.getProperty("user.dir"), "/data");
        }
        return path;
    }

    public static int getMessageSize() {
        return IS_DEBUG ? 24 : 50;
    }

    public static int getBodySize() {
        return IS_DEBUG ? 8 : 34;
    }

    public static int getPageMessageCount() {
        return Math.floorDiv(PAGE_SIZE - 2, getMessageSize());
    }

    public static int getBlockMessageCount() {
        return getPageMessageCount() * (int) (BLOCK_SIZE / PAGE_SIZE);
    }
}
