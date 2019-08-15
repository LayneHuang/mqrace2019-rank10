package io.solution.utils;

import io.openmessaging.Message;
import io.solution.GlobalParams;
import io.solution.data.MyBlock;

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

        int size = blocks.size();
        if (size <= 1) {
            return blocks;
        }

        // 记录每个block处理到的下标
        int[] indexs = new int[size];
        // 当前选择的block
        int selectedBlockIndex = -1;
        // 选择到最小的a
        long sMinA = Long.MAX_VALUE;
        // 插入结果的数量
        int nowSize = 0;
        // 页总数量
        int totalSize = 0;

        int idx = 0;
        for (MyBlock block : blocks) {
            totalSize += block.getMessageAmount();
            indexs[idx] = 0;
            idx++;
        }

        Message[] messages = new Message[totalSize];

        while (nowSize < totalSize) {
            for (int i = 0; i < blocks.size(); ++i) {
                MyBlock block = blocks.get(i);
                if (indexs[i] >= block.getMessageAmount()) {
                    continue;
                }
                long minA = block.getMessages()[indexs[i]].getT();
                if (selectedBlockIndex == -1 || minA < sMinA) {
                    selectedBlockIndex = i;
                    sMinA = minA;
                }
            }
            messages[nowSize++] = blocks.get(selectedBlockIndex).getMessages()[indexs[selectedBlockIndex]];
            // 处理下标 + 1
            indexs[selectedBlockIndex]++;
            selectedBlockIndex = -1;
            sMinA = Long.MAX_VALUE;

        }

        // 结果集
        List<MyBlock> result = new ArrayList<>();
        int tempSize = 0;
        for (int i = 0; i < totalSize; ++i) {
            if (tempSize == 0) {
                MyBlock block = new MyBlock();
                result.add(block);
            }
            result.get(result.size() - 1).addMessage(messages[i]);
            tempSize++;
            if (tempSize == GlobalParams.getBlockMessageLimit()) {
                tempSize = 0;
            }
        }

        return result;
    }

}
