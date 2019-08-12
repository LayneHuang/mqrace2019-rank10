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

    //    private ByteBuffer buffer;
    private long minA;
    private long maxA;
    private long minT;
    private long maxT;
    private long sum;

    private Message[] messages;

    private int messageAmount;

    MyPage() {
        // 4k页 , 规定不能使用DIO
        messages = new Message[GlobalParams.getPageMessageCount()];
    }

    public Message[] getMessages() {
        return messages;
    }

    public void writeBuffer(ByteBuffer buffer) {
        buffer.putInt(messageAmount);
        for (Message message : messages) {
            buffer.putLong(message.getT());
            buffer.putLong(message.getA());
            buffer.put(message.getBody());
        }
        buffer.flip();
    }

    public void writeBufferOnlyBody(ByteBuffer buffer) {
        for (int i = 0; i < messageAmount; ++i) {
            buffer.put(messages[i].getBody());
        }
//        buffer.flip();
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

    public void addMessages(Message[] messages, int size) {
        if (size == 0) {
            System.out.println("error: message list is empty");
            return;
        }
        sum = 0;
        minA = maxA = messages[0].getA();
        minT = maxT = messages[0].getT();
        messageAmount = 0;
        for (int i = 0; i < size; ++i) {
            Message message = messages[i];
            sum += message.getA();
            minT = Math.min(minT, message.getT());
            maxT = Math.max(maxT, message.getT());
            minA = Math.min(minA, message.getA());
            maxA = Math.max(maxA, message.getA());
            this.messages[messageAmount++] = message;
        }
//        this.messages.addAll(messages);

    }

    public long getSum() {
        return sum;
    }

    public void setSum(long sum) {
        this.sum = sum;
    }

    public int getMessageAmount() {
        return messageAmount;
    }

    public void setMessageAmount(int messageAmount) {
        this.messageAmount = messageAmount;
    }
}
