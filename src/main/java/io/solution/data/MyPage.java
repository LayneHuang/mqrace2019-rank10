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
        buffer = ByteBuffer.allocateDirect(GlobalParams.PAGE_SIZE);
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
        buffer.putInt(messages.size());
        buffer.putLong(minT);
        buffer.putLong(maxT);
        buffer.putLong(minA);
        buffer.putLong(maxA);
        for (Message message : messages) {
            buffer.putLong(message.getT());
            buffer.putLong(message.getA());
            buffer.put(message.getBody());
        }
//        System.out.println("origin:");
//        System.out.print(minT + " ");
//        System.out.print(maxT + " ");
//        System.out.print(minA + " ");
//        System.out.print(maxA + " ");
//        for (Message message : messages) {
//            System.out.print(message.getT() + " ");
//            System.out.print(message.getA() + " ");
//        }
//        System.out.println();
//        showPage();
    }

    public void showPage() {
        System.out.println("page:");
        System.out.print(buffer.getInt(0) + " ");
        System.out.print(buffer.getLong(4) + " ");
        System.out.print(buffer.getLong(12) + " ");
        System.out.print(buffer.getLong(20) + " ");
        System.out.print(buffer.getLong(28) + " ");
        for (int i = 0; i < GlobalParams.PAGE_MESSAGE_COUNT; ++i) {
            int idx = i * GlobalParams.getMessageSize() + 36;
            System.out.print(buffer.getLong(idx) + " ");
            System.out.print(buffer.getLong(idx + 8) + " ");
            System.out.println();
        }
    }

}
