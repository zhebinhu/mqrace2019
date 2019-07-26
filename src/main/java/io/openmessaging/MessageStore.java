package io.openmessaging;

import java.util.List;


public abstract class MessageStore {
    /**
     * 写入一个消息；
     * 这个接口需要是线程安全的，也即评测程序会并发调用该接口进行put；
     * @param message message，代表消息的内容，评测时内容会随机产生
     */
    abstract void put(Message message);

    /**
     * 根据a和t的条件,返回符合条件的消息的集合. t是输入时间戳模拟值，和实际时间戳没有关系, 线程内升序
     * 这个接口需要是线程安全的，也即评测程序会并发调用该接口
     * 返回的List需要按照t升序排列. 如果没有符合的消息, 返回大小为0的List. 单条线程最大返回消息数量不超过50万
     * @param aMin 代表a的最小值(包含此值)
     * @param aMax 代表a的最大值(包含此值)
     * @param tMin 代表t的最小值(包含此值)
     * @param tMax 代表t的最大值(包含此值)
     */
    abstract List<Message> getMessage(long aMin, long aMax, long tMin, long tMax);


    /**
     * 根据a和t的条件,返回符合条件消息的a值的求平均结果. t是输入时间戳模拟值，和实际时间戳没有关系, 线程内升序
     * 这个接口需要是线程安全的，也即评测程序会并发调用该接口
     * 结果忽略小数位,返回整数位即可. 如果没有符合的消息, 返回0
     * @param aMin 代表a的最小值(包含此值)
     * @param aMax 代表a的最大值(包含此值)
     * @param tMin 代表t的最小值(包含此值)
     * @param tMax 代表t的最大值(包含此值)
     */
    abstract long getAvgValue(long aMin, long aMax, long tMin, long tMax);

}
