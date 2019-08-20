package io.openmessaging;

/**
 * @author huzb
 * @version v1.0.0
 * @date 2019/8/20
 */
public class AvgResult {
    public long sum;
    public int count;

    public AvgResult(long sum, int count) {
        this.sum = sum;
        this.count = count;
    }
}
