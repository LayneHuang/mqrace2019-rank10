package io.solution.data;

import io.openmessaging.Message;
import io.solution.GlobalParams;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * @Author: laynehuang
 * @CreatedAt: 2019/8/1 0001
 */
public class MyPage {

    private ByteBuffer buffer;
    private long minA;
    private long maxA;
    private long minT;
    private long maxT;

    public MyPage() {
        // 4k页 , 规定不能使用DIO
        buffer = ByteBuffer.allocate(GlobalParams.PAGE_SIZE);
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public long getMaxT() {
        return maxT;
    }

    public long getMinA() {
        return minA;
    }

    public long getMaxA() {
        return maxA;
    }

    public long getMinT() {
        return minT;
    }

    public void addMessages(List<Message> messages) {
        if (messages == null || messages.isEmpty()) {
            System.out.println("error: message list is empty");
            return;
        }
        minA = maxA = messages.get(0).getA();
        minT = maxT = messages.get(0).getT();
        for (Message message : messages) {
            minT = Math.min(minT, message.getT());
            maxT = Math.max(maxT, message.getT());
            minA = Math.min(minA, message.getA());
            maxA = Math.max(maxA, message.getA());
        }
        buffer.putLong(minT);
        buffer.putLong(maxT);
        buffer.putLong(minA);
        buffer.putLong(maxA);
        for (Message message : messages) {
            buffer.putLong(message.getT());
            buffer.putLong(message.getA());
            buffer.put(message.getBody());
        }
    }

}
