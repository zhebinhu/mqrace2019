package io.openmessaging.Context;

import io.openmessaging.Constants;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by huzhebin on 2019/08/08.
 */
public class DataContext {
    public DataContext() {
        for (int i = 0; i < 20; i++) {
            bufferList.add(ByteBuffer.allocateDirect(Constants.DATA_SIZE * Constants.DATA_NUM));
        }
    }

    public List<ByteBuffer> bufferList = new ArrayList<>();

    public Future[] readFutures = new Future[20];

    public ExecutorService executorService = Executors.newSingleThreadExecutor(r -> {
        Thread thread = new Thread(r);
        thread.setDaemon(true);
        return thread;
    });

    public int offsetA = 0;
}