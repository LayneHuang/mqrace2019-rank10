package io.solution;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @Author: laynehuang
 * @CreatedAt: 2019/8/1 0001
 */
public class GlobalParams {

    private static boolean IS_DEBUG = Boolean.valueOf(System.getProperty("debug", "false"));

    private static final int PAGE_SIZE = 1024;

    private static final int BLOCK_SIZE = PAGE_SIZE * (IS_DEBUG ? 1 : 4);

    /**
     * 消息总数
     */
    private static long MSG_COUNT = (IS_DEBUG ? 20_000_000L : 2_100_000_000L);

    /**
     * 拥塞队列的大小
     */
    public static final int BLOCK_COUNT_LIMIT = 1024;

    /**
     * 提交个数
     */
    public static final int BLOCK_COMMIT_COUNT_LIMIT = 128;

    /**
     * 写文件拥塞队列大小
     */
    public static final int WRITE_COUNT_LIMIT = 512;

    /**
     * 提交个数
     */
    public static final int WRITE_COMMIT_COUNT_LIMIT = 1024;         // min = 4k / 50 * 8 * this

    private static boolean isStepOneFinished = false;

    public static void setStepOneFinished() {
        isStepOneFinished = true;
    }

    public static boolean isStepOneFinished() {
        return isStepOneFinished;
    }

    // 0 -> t , 1 -> a , 2 -> body
    public static Path getPath(int d) {
        Path path;
        if (GlobalParams.IS_DEBUG) {
            if (d == 2) {
                path = Paths.get(System.getProperty("user.dir"), "/data.b");
            } else if (d == 1) {
                path = Paths.get(System.getProperty("user.dir"), "/data.a");
            } else {
                path = Paths.get(System.getProperty("user.dir"), "/data.t");
            }
        } else {
            if (d == 2) {
                path = Paths.get("/alidata1/race2019/data/mydata.b");
            } else if (d == 1) {
                path = Paths.get("/alidata1/race2019/data/mydata.a");
            } else {
                path = Paths.get("/alidata1/race2019/data/mydata.t");
            }
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
        return Math.floorDiv(PAGE_SIZE, getMessageSize());
    }

    public static int getBlockPageLimit() {
        return Math.floorDiv(BLOCK_SIZE, PAGE_SIZE);
    }

    public static int getBlockMessageLimit() {
        return getBlockPageLimit() * getPageMessageCount();
    }

    public static final int MAX_R_TREE_CHILDREN_AMOUNT = 8;

    public static final int FATHER_TREE_BLOCK_SIZE = 4000;

    public static int getRTreeSubTreeSize() {
        return (int) Math.floorDiv(MSG_COUNT, (long) getBlockMessageLimit() * FATHER_TREE_BLOCK_SIZE) + 10;
    }

}
