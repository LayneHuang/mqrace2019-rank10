package io.solution.data;

import io.openmessaging.Message;

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

    }

    public void addPage(MyPage page) {

    }

}
