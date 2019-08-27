package io.solution.data;

import io.openmessaging.Message;

public class MyMsg {
    public Message msg;
    public int idx;

    public MyMsg(Message msg, int idx) {
        this.msg = msg;
        this.idx = idx;
    }
}
