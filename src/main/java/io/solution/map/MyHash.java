package io.solution.map;

import io.openmessaging.Message;
import io.solution.GlobalParams;
import io.solution.data.BlockInfo;
import io.solution.utils.HelpUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @Author: laynehuang
 * @CreatedAt: 2019/8/6 0006
 */
public class MyHash {

    private static MyHash ins;

    private List<BlockInfo> all;

    public int getSize() {
        return this.all.size();
    }

    private MyHash() {
        all = new ArrayList<>();
    }

    static public MyHash getIns() {
        // double check locking
        if (ins != null) {
            return ins;
        } else {
            synchronized (MyHash.class) {
                if (ins == null) {
                    ins = new MyHash();
                }
            }
            return ins;
        }
    }

    public synchronized void insert(BlockInfo info) {
        all.add(info);
    }

    public void showMyHash() {
        for (BlockInfo info : all) {
            info.show();
        }
    }

    public List<Message>
    find2(long minT, long maxT, long minA, long maxA) {
        FileChannel channel = null;
        Path path = GlobalParams.getPath();
        try {
            channel = FileChannel.open(path, StandardOpenOption.READ);
        } catch (IOException e) {
            e.printStackTrace();
        }
        List<Message> res = new ArrayList<>();
        for (BlockInfo info : all) {
            if (HelpUtil.intersect(
                    minT, maxT, minA, maxA,
                    info.getMinT(), info.getMaxT(), info.getMinA(), info.getMaxA()
            )) {
                ByteBuffer buffer = ByteBuffer.allocateDirect((GlobalParams.PAGE_SIZE * info.getAmount()));
                try {
                    channel.read(buffer);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                // Todo : buffer -> myBlock
                List<Message> messages = HelpUtil.transToList(buffer, info.getAmount());
                for (Message message : messages) {
                    if (HelpUtil.inSide(
                            message.getT(), message.getA(),
                            minT, maxT, minA, maxA
                    )) {
                        res.add(message);
                    }
                }
                buffer.clear();
            }
        }
        res.sort(Comparator.comparingLong(Message::getT));
        return res;
    }


}
