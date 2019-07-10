package io.openmessaging;

import java.util.Arrays;
import java.util.Objects;


public class Message {
    //随机整数,可能重复
    private long a;

    //输入时间戳,基本是升序的,可能重复
    private long t;

    //消息体, 随机byte数组, 大小固定34字节
    private byte[] body;

    private int hash = 0;

    public Message(long a, long t, byte[] body) {
        this.a = a;
        this.t = t;
        this.body = body;
    }


    public long getA() {
        return a;
    }


    public void setA(long a) {
        this.a = a;
    }


    public long getT() {
        return t;
    }


    public void setT(long t) {
        this.t = t;
    }

    public byte[] getBody() {
        return body;
    }


    public void setBody(byte[] body) {
        this.body = body;
    }

    @Override
    public int hashCode() {
        if (hash != 0)
            return hash;
        final int prime = 31;
        int result = 1;
        result = prime * result + Long.hashCode(a);
        result = prime * result + Long.hashCode(t);
        result = prime * result + Arrays.hashCode(body);
        this.hash = result;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Message other = (Message) obj;
        return a == other.a && t == other.t && Arrays.equals(body, other.body);
    }

}
