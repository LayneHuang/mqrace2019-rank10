package io.solution.utils;

import io.solution.GlobalParams;
import io.solution.data.MyBlock;
import io.solution.data.MyPage;

import java.util.ArrayList;
import java.util.List;

/**
 * 排序工具
 *
 * @Author: laynehuang
 * @CreatedAt: 2019/8/5 0005
 */
// Todo: 待测试
public class SortUtil {


    /**
     * block size 不大
     * 每个block的pages有序
     * 直接for过去做归并
     *
     * @param blocks
     * @return
     */
    public static List<MyBlock> myMergeSort(List<MyBlock> blocks) {

        if (blocks == null || blocks.isEmpty()) {
            return new ArrayList<>();
        }

        // 页结果集
        List<MyPage> pageResult = new ArrayList<>();
        // 记录每个block处理到的下标
        List<Integer> indexs = new ArrayList<>();
        // 当前选择的block
        int selectedBlockIndex = 0;
        // 插入结果的数量
        int nowSize = 0;
        // 页总数量
        int totalSize = 0;
        for (MyBlock block : blocks) {
            totalSize += block.getSize();
            indexs.add(0);
        }

        while (nowSize < totalSize) {
            int idx = 0;
            for (MyBlock block : blocks) {
                long minA = block.getPages()
                        .get(indexs.get(idx))
                        .getMinA();
                long sMinA = blocks.get(selectedBlockIndex)
                        .getPages()
                        .get(indexs.get(selectedBlockIndex))
                        .getMinA();
                if (minA < sMinA) {
                    selectedBlockIndex = idx;
                }
                idx++;
            }

            pageResult.add(
                    blocks.get(selectedBlockIndex)
                            .getPages()
                            .get(indexs.get(selectedBlockIndex))
            );
            // 处理下标 + 1
            indexs.set(selectedBlockIndex, indexs.get(selectedBlockIndex) + 1);
            nowSize++;
        }

        // 结果集
        List<MyBlock> result = new ArrayList<>();
        int tempSize = 0;
        for (MyPage page : pageResult) {
            if (tempSize == 0) {
                MyBlock block = new MyBlock();
                result.add(block);
            }
            result.get(result.size() - 1)
                    .getPages()
                    .add(page);
            tempSize++;
            if (tempSize == GlobalParams.BLOCK_SIZE_LIMIT) {
                tempSize = 0;
            }
        }

        return result;
    }

}
