package io.solution.data;

import io.openmessaging.Message;
import io.solution.GlobalParams;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: laynehuang
 * @CreatedAt: 2019/8/1 0001
 */
public class MyPage {

    //    private ByteBuffer buffer;
    private long minA;
    private long maxA;
    private long minT;
    private long maxT;
    private long sum;

    List<Message> messages;

    public MyPage() {
        // 4k页 , 规定不能使用DIO
        messages = new ArrayList<>();
    }

    public List<Message> getMessages() {
        return messages;
    }

    public ByteBuffer getBuffer(ByteBuffer buffer) {
        buffer.position(0);
        buffer.putInt(messages.size());
        for (Message message : messages) {
            buffer.putLong(message.getT());
            buffer.putLong(message.getA());
            buffer.put(message.getBody());
        }
        buffer.flip();
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
        sum = 0;
        minA = maxA = messages.get(0).getA();
        minT = maxT = messages.get(0).getT();
        for (Message message : messages) {
            sum += message.getA();
            minT = Math.min(minT, message.getT());
            maxT = Math.max(maxT, message.getT());
            minA = Math.min(minA, message.getA());
            maxA = Math.max(maxA, message.getA());
        }
        this.messages.addAll(messages);
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

    public int getSize() {
        return this.messages.size();
    }

    public long getSum() {
        return sum;
    }

    public void setSum(long sum) {
        this.sum = sum;
    }
}
