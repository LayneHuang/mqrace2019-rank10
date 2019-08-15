package io.solution.data;

import io.openmessaging.Message;
import io.solution.GlobalParams;

/**
 * @Author: laynehuang
 * @CreatedAt: 2019/8/1 0001
 */
public class MyBlock {

    private long minA;
    private long maxA;
    private long minT;
    private long maxT;
    private long sum;

    private Message[] messages;

    private int messageAmount;

    public MyBlock() {
        sum = 0;
        messageAmount = 0;
        messages = new Message[GlobalParams.getBlockMessageLimit()];
    }

    public void addMessages(Message[] messages, int size) {
        sum = 0;
        if (size == 0) return;
        this.messageAmount = size;
        for (int i = 0; i < size; ++i) {
            this.messages[i] = messages[i];
            sum += messages[i].getA();
            if (i == 0) {
                minA = maxA = messages[i].getA();
                minT = maxT = messages[i].getT();
            } else {
                minA = Math.min(minA, messages[i].getA());
                maxA = Math.max(maxA, messages[i].getA());
                minT = Math.min(minT, messages[i].getT());
                maxT = Math.max(maxT, messages[i].getT());
            }
        }
    }

    public void addMessagesStupid(Message[] messages, int size) {
        if (size == 0) return;
        messageAmount = size;
        System.arraycopy(messages, 0, this.messages, 0, messageAmount);
    }

    public void addMessage(Message message) {
        sum += message.getA();
        if (messageAmount == 0) {
            minA = maxA = message.getA();
            minT = maxT = message.getT();
        } else {
            minA = Math.min(minA, message.getA());
            maxA = Math.max(maxA, message.getA());
            minT = Math.min(minT, message.getT());
            maxT = Math.max(maxT, message.getT());
        }
        messages[messageAmount++] = message;
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

    public long getMaxT() {
        return maxT;
    }

    public long getSum() {
        return sum;
    }

    public void setSum(long sum) {
        this.sum = sum;
    }

    public void showSquare() {
        System.out.println(minA + " " + maxA + " " + minT + " " + maxT);
    }

    public int getMessageAmount() {
        return messageAmount;
    }

    public Message[] getMessages() {
        return messages;
    }
}
