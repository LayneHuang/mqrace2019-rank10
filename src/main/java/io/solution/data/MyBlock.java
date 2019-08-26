package io.solution.data;

import io.openmessaging.Message;
import io.solution.GlobalParams;

/**
 * @Author: laynehuang
 * @CreatedAt: 2019/8/1 0001
 */
public class MyBlock {

    public long minA;
    public long maxA;
    public long minT;
    public long maxT;
    public long sum;

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

    public int getMessageAmount() {
        return messageAmount;
    }

    public Message[] getMessages() {
        return messages;
    }

}
