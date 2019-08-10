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

public class SortUtil {

    /**
     * block size 不大
     * 每个block的pages有序
     * 直接for过去做归并
     *
     * @param blocks
     * @return
     */
    static List<MyBlock> myMergeSort(List<MyBlock> blocks) {

        if (blocks == null || blocks.isEmpty()) {
            return new ArrayList<>();
        }
        if (blocks.size() == 1) {
            return blocks;
        }

        // 页结果集
        List<MyPage> pageResult = new ArrayList<>();
        // 记录每个block处理到的下标
        List<Integer> indexs = new ArrayList<>();
        // 当前选择的block
        int selectedBlockIndex = -1;
        // 选择到最小的a
        long sMinA = Long.MAX_VALUE;
        // 插入结果的数量
        int nowSize = 0;
        // 页总数量
        int totalSize = 0;
        for (MyBlock block : blocks) {
            totalSize += block.getSize();
            indexs.add(0);
        }

        while (nowSize < totalSize) {
            for (int i = 0; i < blocks.size(); ++i) {
                MyBlock block = blocks.get(i);
                if (indexs.get(i) >= block.getSize()) {
                    continue;
                }
                long minA = block.getPages()
                        .get(indexs.get(i))
                        .getMinA();
                if (selectedBlockIndex == -1 || minA < sMinA) {
                    selectedBlockIndex = i;
                    sMinA = minA;
                }
            }
            pageResult.add(
                    blocks.get(selectedBlockIndex)
                            .getPages()
                            .get(indexs.get(selectedBlockIndex))
            );
            // 处理下标 + 1
            int nIdx = indexs.get(selectedBlockIndex) + 1;
            indexs.set(selectedBlockIndex, nIdx);
            selectedBlockIndex = -1;
            sMinA = Long.MAX_VALUE;
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
            result.get(result.size() - 1).addPage(page);
            tempSize++;
            if (tempSize == GlobalParams.BLOCK_SIZE_LIMIT) {
                tempSize = 0;
            }
        }

        return result;
    }

}
