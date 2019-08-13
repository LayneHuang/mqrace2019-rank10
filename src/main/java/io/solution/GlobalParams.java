package io.solution;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @Author: laynehuang
 * @CreatedAt: 2019/8/1 0001
 */
public class GlobalParams {

    private static boolean IS_DEBUG = Boolean.valueOf(System.getProperty("debug", "false"));

    public static final int DIRECT_MEMORY_SIZE = (IS_DEBUG ? 1 : 2) * 1050 * 1000 * 1000;

    public static final int PAGE_SIZE = 2 * 1024;

    public static final int BLOCK_SIZE = 1024 * (IS_DEBUG ? 2 : 32);

    public static final long BLOCK_SIZE_LIMIT = BLOCK_SIZE / PAGE_SIZE;

    /**
     * 拥塞队列的大小
     */
    public static final int BLOCK_COUNT_LIMIT = 10;

    /**
     * 写文件拥塞队列大小
     */
    public static final int WRITE_COUNT_LIMIT = 40;

    private static boolean isStepOneFinished = false;

    public static void setStepOneFinished() {
        isStepOneFinished = true;
    }

    public static boolean isStepOneFinished() {
        return isStepOneFinished;
    }

    public static Path getPath() {
        Path path = Paths.get("/alidata1/race2019/data/mydata.in");
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
//        return Math.floorDiv(PAGE_SIZE - 2, getMessageSize());
        // 只写body
        return Math.floorDiv(PAGE_SIZE, getBodySize());
    }

    public static int getBlockPageLimit() {
        return Math.floorDiv(BLOCK_SIZE, PAGE_SIZE);
    }

    public static int getBlockMessageLimit() {
        return getPageMessageCount() * (int) (BLOCK_SIZE / PAGE_SIZE);
    }

    /**
     * 块的数量
     */
    public static int getBlockInfoLimit() {
        return (int) (110L * 1024 * 1024 * 1024 / BLOCK_SIZE) / (IS_DEBUG ? 8 : 1);
    }

    public static final int MAX_R_TREE_CHILDREN_AMOUNT = 8;

}
