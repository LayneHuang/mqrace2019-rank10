package io.solution;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @Author: laynehuang
 * @CreatedAt: 2019/8/1 0001
 */
public class GlobalParams {

    public static boolean IS_DEBUG = Boolean.valueOf(System.getProperty("debug", "false"));

    private static final int PAGE_SIZE = 1024;

    private static final int BLOCK_SIZE = PAGE_SIZE * (IS_DEBUG ? 8 : 8);

    /**
     * 消息总数
     */
    private static int MSG_COUNT = (IS_DEBUG ? 200_000_000 : 2_080_000_000);

    /**
     * 拥塞队列的大小
     */
    public static final int MSG_BLOCK_QUEUE_LIMIT = 1024 * 100;

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
    public static final int WRITE_COMMIT_COUNT_LIMIT = 50;         // min = 4k / 50 * 8 * this

    public static final int MAX_THREAD_AMOUNT = 20;

    public static final int A_MOD = (IS_DEBUG ? 9 : 199);

    public static final int A_RANGE = (IS_DEBUG ? 10 : 200);

    public static final int EIGHT_K = 8 * 1024;

    private static boolean isStepOneFinished = false;

    public static void setStepOneFinished() {
        isStepOneFinished = true;
    }

    public static boolean isStepOneFinished() {
        return isStepOneFinished;
    }

    private static String PRE_PATH = "/alidata1/race2019/data";

    // 0 -> t , 1 -> a , 2 -> body
    public static Path getPath(int d) {
        Path path;
        if (GlobalParams.IS_DEBUG) {
            if (d == 2) {
                path = Paths.get(System.getProperty("user.dir"), "/d/data.b");
            } else {
                path = Paths.get(System.getProperty("user.dir"), "/d/data.t");
            }
        } else {
            if (d == 2) {
                path = Paths.get(PRE_PATH + "/mydata.b");
            } else {
                path = Paths.get(PRE_PATH + "/mydata.t");
            }
        }
        return path;
    }

    public static Path getTPath(int d) {
        Path path;
        if (GlobalParams.IS_DEBUG) {
            path = Paths.get(System.getProperty("user.dir"), "/d/data" + d + ".t");
        } else {
            path = Paths.get(PRE_PATH + "/mydata" + d + ".t");
        }
        return path;
    }

    public static Path getAPath(int d, boolean w) {
        Path path;
        if (GlobalParams.IS_DEBUG) {
            path = Paths.get(System.getProperty("user.dir"), "/d/data" + (w ? "_w" : "_h") + d + ".a");
        } else {
            path = Paths.get(PRE_PATH + "/mydata" + d + ".a");
        }
        return path;
    }

    public static Path getBPath(int d) {
        Path path;
        if (GlobalParams.IS_DEBUG) {
            path = Paths.get(System.getProperty("user.dir"), "/d/data" + d + ".b");
        } else {
            path = Paths.get(PRE_PATH + "/mydata" + d + ".b");
        }
        return path;
    }

    public static Path getInfoPath() {
        Path path;
        if (GlobalParams.IS_DEBUG) {
            path = Paths.get(System.getProperty("user.dir"), "/d/info.a");
        } else {
            path = Paths.get(PRE_PATH + "/info.i");
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

    public static int INFO_SIZE = 24;
}
