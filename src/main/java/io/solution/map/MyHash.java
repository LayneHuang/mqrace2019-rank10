package io.solution.map;

import io.openmessaging.Message;
import io.solution.GlobalParams;
import io.solution.data.BlockInfo;
import io.solution.utils.HelpUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author: laynehuang
 * @CreatedAt: 2019/8/6 0006
 */
public class MyHash {

    private int limit = 2000;

    private static MyHash ins = new MyHash();

    private BlockInfo[] all;

    private int size = 0;

    public int getSize() {
        return size;
    }

    private MyHash() {
        all = new BlockInfo[limit];
    }

    public static MyHash getIns() {
        return ins;
    }

    public void insert(BlockInfo info) {
        System.out.println("插入块的信息:");
        info.show();
        all[size] = info;
        size++;
        if (size == limit) {
            limit += (size >> 1);
            all = Arrays.copyOf(all, limit);
        }
    }

    public List<Message> find2(long minT, long maxT, long minA, long maxA) {
//        System.out.println("hash list size: " + size);
        List<Message> res = new ArrayList<>();
        for (int i = 0; i < size; ++i) {
            BlockInfo info = all[i];
            if (HelpUtil.intersect(
                    minT, maxT, minA, maxA,
                    info.getMinT(), info.getMaxT(), info.getMinA(), info.getMaxA()
            )) {
                List<Message> messages = HelpUtil.readMessages(
                        info.getPosition(),
                        info.getAmount() * GlobalParams.PAGE_SIZE
                );
                for (Message message : messages) {
                    if (
                            HelpUtil.inSide(
                                    message.getT(), message.getA(),
                                    minT, maxT, minA, maxA
                            )
                    ) {
                        res.add(message);
                    }
                }
            }
        }

        res.sort(Comparator.comparingLong(Message::getT));
        return res;
    }

    public long find3(long minT, long maxT, long minA, long maxA) {
        long res = 0;
        long messageAmount = 0;
        for (int i = 0; i < size; ++i) {
            BlockInfo info = all[i];
            if (
                    HelpUtil.matrixInside(
                            minT, maxT, minA, maxA,
                            info.getMinT(), info.getMaxT(), info.getMinA(), info.getMaxA()
                    )
            ) {
                res += info.getSum();
                messageAmount += info.getMessageAmount();
            } else if (HelpUtil.intersect(
                    minT, maxT, minA, maxA,
                    info.getMinT(), info.getMaxT(), info.getMinA(), info.getMaxA()
            )) {
                List<Message> messages = HelpUtil.readMessages(
                        info.getPosition(),
                        info.getAmount() * GlobalParams.PAGE_SIZE
                );
                for (Message message : messages) {
                    if (
                            HelpUtil.inSide(
                                    message.getT(), message.getA(),
                                    minT, maxT, minA, maxA
                            )
                    ) {
                        res += message.getA();
                        messageAmount++;
                    }
                }
            }
        }
        return messageAmount == 0 ? 0 : Math.floorDiv(res, messageAmount);
    }
}
