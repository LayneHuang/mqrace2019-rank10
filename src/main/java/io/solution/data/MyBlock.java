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

    private List<MyPage> pages;

    public MyBlock() {
        pages = new ArrayList<>();
    }

    public List<MyPage> getPages() {
        return this.pages;
    }

    public int getSize() {
        return this.pages.size();
    }

    public void addMessages(List<Message> messages) {
        sum = 0;
        List<Message> tempMsgs = new ArrayList<>();
        for (Message message : messages) {
            tempMsgs.add(message);
            if (tempMsgs.size() == GlobalParams.getPageMessageCount()) {
                MyPage page = new MyPage();
                page.addMessages(tempMsgs);
                addPage(page);
                tempMsgs.clear();
            }
        }
        if (!tempMsgs.isEmpty()) {
            MyPage page = new MyPage();
            page.addMessages(tempMsgs);
            addPage(page);
            tempMsgs.clear();
        }
    }

    public void addPage(MyPage page) {
        sum += page.getSum();
        if (pages.isEmpty()) {
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
        this.pages.add(page);
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
}
