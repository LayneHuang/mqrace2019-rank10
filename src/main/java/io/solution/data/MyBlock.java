package io.solution.data;

import io.openmessaging.Message;
import io.solution.GlobalParams;

import java.util.ArrayList;
import java.util.List;

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

    private MyPage[] pages;

    private int pageAmount;

    private int messageAmount;

    public MyBlock() {
        pages = new MyPage[GlobalParams.getBlockPageLimit()];
    }

    public MyPage[] getPages() {
        return this.pages;
    }

    public int getPageAmount() {
        return pageAmount;
    }

    public void addMessages(Message[] messages, int size) {
        sum = 0;
        Message[] tempMsgs = new Message[GlobalParams.getPageMessageCount()];
        int pageMessageAmount = 0;
        for (int i = 0; i < size; ++i) {
            Message message = messages[i];
            tempMsgs[pageMessageAmount++] = message;
            if (pageMessageAmount == GlobalParams.getPageMessageCount()) {
                MyPage page = new MyPage();
                page.addMessages(tempMsgs, pageMessageAmount);
                addPage(page);
                pageMessageAmount = 0;
            }
        }
        if (pageMessageAmount > 0) {
            MyPage page = new MyPage();
            page.addMessages(tempMsgs, pageMessageAmount);
            addPage(page);
        }
        this.messageAmount = size;
    }

    public void addPage(MyPage page) {
        sum += page.getSum();
        if (pageAmount == 0) {
            minA = page.getMinA();
            maxA = page.getMaxA();
            minT = page.getMinT();
            maxT = page.getMaxT();
        } else {
            minA = Math.min(minA, page.getMinA());
            maxA = Math.max(maxA, page.getMaxA());
            minT = Math.min(minT, page.getMinT());
            maxT = Math.max(maxT, page.getMaxT());
        }
        pages[pageAmount++] = page;
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
}
