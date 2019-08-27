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

    private static final int BLOCK_SIZE = PAGE_SIZE * (IS_DEBUG ? 1 : 8);

    /**
     * 消息总数
     */
    private static int MSG_COUNT = (IS_DEBUG ? 20_000_000 : 2_080_000_000);

    /**
     * 拥塞队列的大小
     */
    public static final int MSG_BLOCK_QUEUE_LIMIT = 1024 * 50;

    /**
     * 提交个数
     */
    public static final int BLOCK_COMMIT_COUNT_LIMIT = 256;

    /**
     * 写文件拥塞队列大小
     */
    public static final int WRITE_COUNT_LIMIT = 512;

    /**
     * 提交个数
     */
    public static final int WRITE_COMMIT_COUNT_LIMIT = 1024;         // min = 4k / 50 * 8 * this

    public static final long MAX_A_VALUE = 3000000000000000L;

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
                path = Paths.get(System.getProperty("user.dir"), "/d/data.b");
            } else if (d == 1) {
                path = Paths.get(System.getProperty("user.dir"), "/d/data.a");
            } else {
                path = Paths.get(System.getProperty("user.dir"), "/d/data.t");
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

    private static int getMessageSize() {
        return IS_DEBUG ? 24 : 50;
    }

    public static int getBodySize() {
        return IS_DEBUG ? 8 : 34;
    }

    public static int getBlockMessageLimit() {
        return Math.floorDiv(BLOCK_SIZE, getMessageSize());
    }

    public static int getBlockInfoLimit() {
        return Math.floorDiv(MSG_COUNT, getBlockMessageLimit());
    }

}
