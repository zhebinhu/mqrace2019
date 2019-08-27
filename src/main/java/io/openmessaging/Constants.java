package io.openmessaging;

/**
 * Created by huzhebin on 2019/07/23.
 */
public class Constants {
    // writer缓冲区
    public final static int WRITER_CAP = 16 * 1024;
    // data写入缓冲区
    public final static int DATA_CAP = 34 * 1024 * 1024;
    // value写入缓冲区
    public final static int VALUE_CAP = 32 * 1024 * 1024;
    // data 的大小
    public final static int DATA_SIZE = 34;

    public final static int DATA_NUM = 4*1024;

    // value 的大小
    public final static int VALUE_SIZE = 8;

    public final static int VALUE_NUM = 512;

    public final static int VALUE_BUF_NUM = 160;

    public final static int PAGE_SIZE = 4096;

    public final static String URL = "/users/huzhebin/data/";

    //public final static String URL = "/alidata1/race2019/data/";
}
