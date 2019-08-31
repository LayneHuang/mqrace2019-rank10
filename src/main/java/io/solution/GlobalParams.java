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

    public static final int BLOCK_SIZE = PAGE_SIZE * (IS_DEBUG ? 24 : 50);

    /**
     * 消息总数
     */
    public static int MSG_COUNT = (IS_DEBUG ? 25_000_000 : 2_080_000_000);

    /**
     * 写文件拥塞队列大小
     */
    public static final int WRITE_COMMIT_COUNT_LIMIT = 1024;

    public static final int MAX_THREAD_AMOUNT = 20;

    public static final int A_MOD = (IS_DEBUG ? 9 : 59);

    public static final int A_RANGE = (IS_DEBUG ? 10 : 60);

    public static final int EIGHT_K = 8 * 1024;

    private static boolean isStepOneFinished = false;

    public static void setStepOneFinished() {
        isStepOneFinished = true;
    }

    public static boolean isStepOneFinished() {
        return isStepOneFinished;
    }

    private static String PRE_PATH = "/alidata1/race2019/data";

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
            path = Paths.get(PRE_PATH + "/mydata" + (w ? "_w" : "_h") + d + ".a");
        }
        return path;
    }

    public static Path getBPath(int d) {
        Path path;
        if (GlobalParams.IS_DEBUG) {
            path = Paths.get(System.getProperty("user.dir"), "/d/data" + d + ".body");
        } else {
            path = Paths.get(PRE_PATH + "/mydata" + d + ".body");
        }
        return path;
    }

    public static Path getATPath() {
        Path path;
        if (GlobalParams.IS_DEBUG) {
            path = Paths.get(System.getProperty("user.dir"), "/d/data.at");
        } else {
            path = Paths.get(PRE_PATH + "/mydata.at");
        }
        return path;
    }

    public static Path getInfoPath() {
        Path path;
        if (GlobalParams.IS_DEBUG) {
            path = Paths.get(System.getProperty("user.dir"), "/d/info.i");
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
