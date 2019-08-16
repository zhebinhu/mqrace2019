package io.openmessaging;

import sun.misc.Unsafe;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意； 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageStoreImpl2 extends MessageStore {

    private static void printFile(String path) {
        System.out.println(String.format("======== %s ========", path));
        try {
            System.out.print(new String(Files.readAllBytes(Paths.get(path))));
        } catch (Exception e) {
            System.out.println("READ ERROR!");
        }
        System.out.println("======== END OF FILE ========");
    }

    static {
        //printFile("/proc/cpuinfo");
        printFile("/proc/meminfo");
    }

    private static String dumpMessage(Message message) {
        char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();
        StringBuilder s = new StringBuilder();
        s.append(String.format("%08X,", message.getT()));
        s.append(String.format("%08X,", message.getA()));
        byte[] bytes = message.getBody();
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            s.append(HEX_ARRAY[v >>> 4]);
            s.append(HEX_ARRAY[v & 0x0F]);
        }
        return s.toString();
    }

    private static void doSortMessage(ArrayList<Message> a) {
        Collections.sort(a, new Comparator<Message>() {
            public int compare(Message a, Message b) {
                return Long.compare(a.getT(), b.getT());
            }
        });
    }

    private static final int MAX_MSGBUF = 1000;

    private static final int MESSAGE_SIZE = 50;

    private static Message deserializeMessage(ByteBuffer buffer, int position) {
        byte body[] = new byte[34];
        long t = buffer.getLong(position + 0);
        long a = buffer.getLong(position + 8);
        System.arraycopy(buffer.array(), position + 16, body, 0, body.length);
        return new Message(a, t, body);
    }

    private static long makeLong(int high, int low) {
        return ((long) high << 32) | ((long) low & 0xFFFFFFFFL);
    }

    private static boolean rectOverlap(int aLeft, int aRight, int aBottom, int aTop, int bLeft, int bRight, int bBottom, int bTop) {
        return (aLeft <= bRight && aRight >= bLeft && aTop >= bBottom && aBottom <= bTop);
    }

    private static boolean pointInRect(int lr, int bt, int rectLeft, int rectRight, int rectBottom, int rectTop) {
        return (rectLeft <= lr && lr <= rectRight && rectBottom <= bt && bt <= rectTop);
    }

    private static boolean rectInRect(int aLeft, int aRight, int aBottom, int aTop, int bLeft, int bRight, int bBottom, int bTop) {
        return bLeft <= aLeft && aRight <= bRight && bBottom <= aBottom && aTop <= bTop;
    }

    private static boolean pointInRectL(long lr, long bt, long rectLeft, long rectRight, long rectBottom, long rectTop) {
        return (rectLeft <= lr && lr <= rectRight && rectBottom <= bt && bt <= rectTop);
    }

    static class MessageCompressor {
        public static final int AOFFSET = 10000;

        // compress T and A in one message with given tBase to a 24-bit integer
        public static int tryCompress(int tBase, long t, long a) {
            //if (t % 2 == 0 || (t / 2) % 2 == 0) return 0xFFFFFF;
            long o = t - tBase;
            long d = a - tBase + AOFFSET;
            if ((0 <= o && o <= 254) && (0 <= d && d <= 65535)) {
                return ((int) d & 0xFFFF) | ((int) o << 16);
            } else {
                return 0xFFFFFF;
            }
        }

        public static boolean isValid(int m) {
            return (m & 0xFFFFFF) != 0xFFFFFF;
        }

        public static int extractT(int tBase, int m) {
            return tBase + ((m >> 16) & 0xFF);
        }

        public static int extractA(int tBase, int m) {
            return tBase + (m & 0xFFFF) - AOFFSET;
        }
    }

    ;

    private static final Unsafe unsafe;

    static {
        Unsafe theUnsafe;
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            theUnsafe = (Unsafe) f.get(null);
        } catch (Exception e) {
            theUnsafe = null;
        }
        unsafe = theUnsafe;
    }

    //private static final String storagePath = "./";
    private static final String storagePath = "/alidata1/race2019/data/";

    //private static final long MEMSZ = 30000000L * 3;
    private static final long MEMSZ = 2100000000L * 3;

    private static final long memBase;

    static {
        memBase = unsafe.allocateMemory(MEMSZ);
        unsafe.setMemory(memBase, MEMSZ, (byte) 0xFF);
    }

    private static final int I_SIZE = 8; // index-record size

    private static final int I_MINT = 0;

    private static final int I_MAXT = 1;

    private static final int I_MINA = 2;

    private static final int I_MAXA = 3;

    private static final int I_SUML = 4;

    private static final int I_SUMH = 5;

    private static final int I_CNT = 6;

    private static final int I_TBASE = 7;

    private static final int L_NREC = 256; // n-record in one block

    private static final int H = 23; // max height of HEAP

    private static final int HEAP_SIZE = ((1 << (H + 1)) + 1);

    private static final int HEAP_LEAF_BASE = 1 << H;

    private static final int HEAP_NINT = HEAP_SIZE * I_SIZE;

    private static final int HEAP_NBYTE = HEAP_NINT * 4;

    private static final long heapBase;

    static {
        heapBase = unsafe.allocateMemory(HEAP_NBYTE);
        unsafe.setMemory(heapBase, HEAP_NBYTE, (byte) 0);
    }

    private static int indexHeap(int offset) {
        return unsafe.getInt(heapBase + offset * 4L);
    }

    private static long indexHeapL(int offset) {
        return unsafe.getLong(heapBase + offset * 4L);
    }

    private static void indexHeapSet(int offset, int val) {
        if (offset < 0 || offset >= HEAP_NINT) {
            System.out.println("INDEX ARRAY FULL!");
            System.exit(-1);
        }
        unsafe.putInt(heapBase + offset * 4L, val);
    }

    static {
        System.out.println(String.format("memBase=%016X", memBase));
        System.out.println(String.format("heapBase=%016X", heapBase));
    }

    private static volatile int state = 0;

    private static Object stateLock = new Object();

    private static boolean haveIncompressibleRecord = false;

    private static ArrayList<Message> incompressibleRecords = new ArrayList<Message>();

    private static void updateLeafTBase(int leafBlockId, int tBase) {
        indexHeapSet((HEAP_LEAF_BASE + leafBlockId) * I_SIZE + I_TBASE, tBase);
    }

    private static void updateLeafIndex(int leafBlockId) {
        int base = (HEAP_LEAF_BASE + leafBlockId) * I_SIZE;
        int tBase = indexHeap(base + I_TBASE);

        int minT = Integer.MAX_VALUE;
        int maxT = Integer.MIN_VALUE;
        int minA = Integer.MAX_VALUE;
        int maxA = Integer.MIN_VALUE;
        long sumA = 0;
        int cnt = 0;

        long l = (long) leafBlockId * L_NREC;
        long r = l + L_NREC;
        for (long i = l; i < r; i++) {

            int m = unsafe.getInt(memBase + i * 3);

            if (MessageCompressor.isValid(m)) {
                int t = MessageCompressor.extractT(tBase, m);
                int a = MessageCompressor.extractA(tBase, m);

                minT = Math.min(minT, t);
                maxT = Math.max(maxT, t);
                minA = Math.min(minA, a);
                maxA = Math.max(maxA, a);

                sumA += a;
                cnt++;
            }
        }

        indexHeapSet(base + I_MINT, minT);
        indexHeapSet(base + I_MAXT, maxT);
        indexHeapSet(base + I_MINA, minA);
        indexHeapSet(base + I_MAXA, maxA);
        indexHeapSet(base + I_SUML, (int) (sumA & 0xFFFFFFFF));
        indexHeapSet(base + I_SUMH, (int) (sumA >> 32));
        indexHeapSet(base + I_CNT, cnt);
    }

    private static int realRecordId = 0;

    private static long recordId = 0;

    private static int curTBase = 0;

    private static int unfullBlocks = 0;

    private static long doPutMessage(long msgT, long msgA) {

        // 如果是一个新块，则更新 tBase
        if (recordId % L_NREC == 0) {
            curTBase = (int) msgT;
        }

        // 尝试压缩消息
        int msgz = MessageCompressor.tryCompress(curTBase, msgT, msgA);

        if (!MessageCompressor.isValid(msgz)) {

            // 若压缩失败，可能是因为 偏移太大
            // 尝试用它自己的 tBase 去压缩
            msgz = MessageCompressor.tryCompress((int) msgT, msgT, msgA);

            if (!MessageCompressor.isValid(msgz)) {
                // 若还是不能压缩，说明消息不能压缩，转slow path处理

                return -1;
            }

            // 用新偏移压缩成功，新开一个块
            recordId = (recordId / L_NREC + 1) * L_NREC;
            curTBase = (int) msgT;
            unfullBlocks++;
        }

        // 写入存储区
        long memOffset = recordId * 3L;
        if (recordId % 1000000 == 0) {
            System.out.println(String.format("[%s]: realRecordId=%d recordId=%d unfullBlocks=%d", new Date().toString(), realRecordId, recordId, unfullBlocks));
        }
        if (memOffset + 4 > MEMSZ) {
            System.out.println("ERROR: MEMORY FULL!");
            System.exit(-1);
        }
        unsafe.putShort(memBase + memOffset, (short) (msgz & 0xFFFF));
        unsafe.putByte(memBase + memOffset + 2, (byte) ((msgz >> 16) & 0xFF));

        // 若是新的块
        if (recordId % L_NREC == 0) {
            int blockId = (int) (recordId / L_NREC);

            // 登记新块的tBase
            updateLeafTBase(blockId, curTBase);

            // 对上一个块计算metadata
            if (blockId > 0) {
                updateLeafIndex(blockId - 1);
            }
        }

        realRecordId++;

        return recordId++;
    }

    private static final int MAXTHREAD = 100;

    private static class PutThreadData {
        RandomAccessFile outputFile;

        ByteBuffer buffer;

        int outputPtr = 0;

        RandomAccessFile inputFile;
    }

    private static final PutThreadData putThreadDataArray[] = new PutThreadData[MAXTHREAD];

    private static final AtomicInteger putThreadCount = new AtomicInteger(0);

    private static final ThreadLocal<PutThreadData> putThreadIdData = new ThreadLocal<PutThreadData>() {
        @Override
        protected PutThreadData initialValue() {
            int threadId = putThreadCount.getAndIncrement();
            PutThreadData ret = new PutThreadData();

            ret.buffer = ByteBuffer.allocate(MESSAGE_SIZE * MAX_MSGBUF);
            ret.outputPtr = 0;

            String fn = String.format(storagePath + "thread%04d.data", threadId);
            try {
                ret.outputFile = new RandomAccessFile(fn, "rw");
                ret.outputFile.setLength(0);

                ret.inputFile = new RandomAccessFile(fn, "r");

            } catch (IOException e) {
                System.out.println("CAN'T OPEN FILE: " + fn);
                e.printStackTrace();
                System.exit(-1);
            }

            putThreadDataArray[threadId] = ret;
            return ret;
        }
    };

    private static RandomAccessFile sortedDataFile;

    private static void externalMergeSort() {
        System.out.println("[" + new Date().toString() + "]: merge-sort begin!");

        try {
            sortedDataFile = new RandomAccessFile(storagePath + "sorted.data", "rw");
            sortedDataFile.setLength(0);

            byte dummyRecord[] = new byte[MESSAGE_SIZE];

            int inputPtr[] = new int[MAXTHREAD];
            int bufferPtr[] = new int[MAXTHREAD];
            int bufferCap[] = new int[MAXTHREAD];
            ByteBuffer buffer[] = new ByteBuffer[MAXTHREAD];

            for (int i = 0; i < MAXTHREAD; i++) {
                buffer[i] = ByteBuffer.allocate(MESSAGE_SIZE * MAX_MSGBUF);
            }

            ByteBuffer writeBuffer = ByteBuffer.allocate(MESSAGE_SIZE * MAX_MSGBUF);
            int writeCount = 0;

            int nThread = putThreadCount.get();

            while (true) {

                // 尝试补充数据
                for (int i = 0; i < nThread; i++) {
                    if (bufferPtr[i] >= bufferCap[i]) {
                        PutThreadData cur = putThreadDataArray[i];

                        // 计算文件中剩余记录数
                        int remainingCount = cur.outputPtr - inputPtr[i];

                        if (remainingCount > 0) {
                            int readCnt = Math.min(remainingCount, MAX_MSGBUF);
                            cur.inputFile.readFully(buffer[i].array(), 0, readCnt * MESSAGE_SIZE);
                            bufferPtr[i] = 0;
                            bufferCap[i] = readCnt;
                            inputPtr[i] += readCnt;
                        }
                    }
                }

                long minValue = Long.MAX_VALUE;
                int minPos = -1;
                for (int i = 0; i < nThread; i++) {
                    if (bufferPtr[i] < bufferCap[i]) {
                        long curValue = buffer[i].getLong(bufferPtr[i] * MESSAGE_SIZE);
                        if (curValue <= minValue) {
                            minValue = curValue;
                            minPos = i;
                        }
                    }
                }

                if (minPos == -1) {
                    break;
                }

                // 登记当前记录到索引
                long msgT = buffer[minPos].getLong(bufferPtr[minPos] * MESSAGE_SIZE + 0);
                long msgA = buffer[minPos].getLong(bufferPtr[minPos] * MESSAGE_SIZE + 8);

                long recordId = doPutMessage(msgT, msgA);
                if (recordId < 0) {

                    // 如果不能压缩，则放入slow-path
                    Message message = deserializeMessage(buffer[minPos], bufferPtr[minPos] * MESSAGE_SIZE);

                    //System.out.println("incompressible: " + dumpMessage(message));
                    haveIncompressibleRecord = true;
                    incompressibleRecords.add(message);
                } else {

                    // 同步 recordId
                    // 若压缩成功，输出当前记录到排序好的文件
                    while (true) {
                        if (!writeBuffer.hasRemaining()) {
                            sortedDataFile.write(writeBuffer.array());
                            writeBuffer.position(0);
                        }

                        if (writeCount < recordId) {

                            writeBuffer.put(dummyRecord);
                            writeCount++;
                        } else {
                            break;
                        }
                    }

                    writeBuffer.put(buffer[minPos].array(), bufferPtr[minPos] * MESSAGE_SIZE, MESSAGE_SIZE);
                    writeCount++;
                }

                bufferPtr[minPos]++;

            }

            sortedDataFile.write(writeBuffer.array(), 0, writeBuffer.position());

        } catch (Exception e) {
            System.out.println("SORT ERROR!");
            e.printStackTrace();
            System.exit(-1);
        }

        System.out.println("[" + new Date().toString() + "]: merge-sort completed!");
    }

    @Override
    public void put(Message message) {

        if (state == 0) {
            synchronized (stateLock) {
                if (state == 0) {
                    System.out.println("[" + new Date() + "]: put() started");
                    state = 1;
                }
            }
        }

        PutThreadData td = putThreadIdData.get();
        ByteBuffer buffer = td.buffer;

        long t = message.getT();
        long a = message.getA();
        byte[] body = message.getBody();

        buffer.putLong(t);
        buffer.putLong(a);
        buffer.put(body);

        if (!buffer.hasRemaining()) {
            try {
                td.outputFile.write(buffer.array());
                buffer.position(0);
                td.outputPtr += MAX_MSGBUF;
            } catch (IOException e) {
                System.out.println("ERROR WRITING MESSAGE!");
                e.printStackTrace();
                System.exit(-1);
            }
        }
    }

    private static void flushPutBuffer() {
        try {
            System.out.println("[" + new Date() + "]: flushing remaining buffers ...");
            int totalMsg = 0;
            int nThread = putThreadCount.get();
            for (int i = 0; i < nThread; i++) {
                PutThreadData td = putThreadDataArray[i];
                int remainingCnt = td.buffer.position() / MESSAGE_SIZE;
                if (remainingCnt > 0) {
                    td.outputFile.write(td.buffer.array(), 0, td.buffer.position());
                    td.outputPtr += remainingCnt;
                }
                totalMsg += td.outputPtr;
                System.out.println(String.format("thread %d: %d", i, td.outputPtr));
            }
            System.out.println(String.format("total: %d", totalMsg));
        } catch (IOException e) {
            System.out.println("FLUSH PUT BUFFER ERROR!");
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private static void updateIndexHeap() {

        // flush last block
        long nLeaf = recordId;
        int nBlock = (int) (nLeaf / L_NREC);
        if (nLeaf % L_NREC != 0) {
            updateLeafIndex(nBlock);
            nBlock++;
        } else {
            updateLeafIndex(nBlock - 1);
        }

        for (int j = H - 1; j >= 0; j--) {
            int l = 1 << j;
            int r = 1 << (j + 1);
            for (int cur = l; cur < r; cur++) {
                int cur_base = I_SIZE * (cur);
                int lch_base = I_SIZE * (cur * 2);
                int rch_base = I_SIZE * (cur * 2 + 1);

                indexHeapSet(cur_base + I_MINT, Math.min(indexHeap(lch_base + I_MINT), indexHeap(rch_base + I_MINT)));
                indexHeapSet(cur_base + I_MAXT, Math.max(indexHeap(lch_base + I_MAXT), indexHeap(rch_base + I_MAXT)));
                indexHeapSet(cur_base + I_MINA, Math.min(indexHeap(lch_base + I_MINA), indexHeap(rch_base + I_MINA)));
                indexHeapSet(cur_base + I_MAXA, Math.max(indexHeap(lch_base + I_MAXA), indexHeap(rch_base + I_MAXA)));

                long lch_sum = makeLong(indexHeap(lch_base + I_SUMH), indexHeap(lch_base + I_SUML));
                long rch_sum = makeLong(indexHeap(rch_base + I_SUMH), indexHeap(rch_base + I_SUML));
                long cur_sum = lch_sum + rch_sum;

                indexHeapSet(cur_base + I_SUML, (int) (cur_sum & 0xFFFFFFFF));
                indexHeapSet(cur_base + I_SUMH, (int) (cur_sum >> 32));

                indexHeapSet(cur_base + I_CNT, indexHeap(lch_base + I_CNT) + indexHeap(rch_base + I_CNT));
            }
        }

        System.out.println("nLeaf : " + nLeaf);
        System.out.println("nBlock: " + nBlock);
    }

    private static void createIndex() {
        try {

            flushPutBuffer();
            externalMergeSort();

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

        System.out.println("[" + new Date() + "]: updateIndexHeap()");
        updateIndexHeap();
        System.out.println("unfullBlocks: " + unfullBlocks);
        System.out.println("incompressibleRecords: " + incompressibleRecords.size());

        printFile("/proc/meminfo");
    }

    private static void doGetMessage(ArrayList<Message> result, int cur, int aMin, int aMax, int tMin, int tMax) {

        if (cur >= HEAP_LEAF_BASE) {
            int tBase = indexHeap(cur * I_SIZE + I_TBASE);

            long l = (cur - HEAP_LEAF_BASE) * L_NREC;
            long r = l + L_NREC;

            ByteBuffer block = ByteBuffer.allocate(MESSAGE_SIZE * L_NREC);

            try {
                sortedDataFile.seek(l * MESSAGE_SIZE);
                sortedDataFile.read(block.array());
            } catch (IOException e) {
                System.out.println("ERROR READING BLOCK!");
                e.printStackTrace();
                System.exit(-1);
            }

            for (long i = l; i < r; i++) {

                int m = unsafe.getInt(memBase + i * 3);
                if (MessageCompressor.isValid(m)) {
                    int t = MessageCompressor.extractT(tBase, m);
                    int a = MessageCompressor.extractA(tBase, m);

                    if (pointInRect(t, a, tMin, tMax, aMin, aMax)) {
                        Message msg = deserializeMessage(block, (int) (i - l) * MESSAGE_SIZE);
                        if (msg.getT() != t || msg.getA() != a) {
                            System.out.println(String.format("ERROR! msgT=%d msgA=%d; t=%d a=%d", msg.getT(), msg.getA(), t, a));
                        }
                        result.add(msg);
                    }
                }
            }

            return;
        }
        int lch = cur * 2;
        int rch = cur * 2 + 1;
        int lch_base = I_SIZE * lch;
        int rch_base = I_SIZE * rch;

        if (rectOverlap(indexHeap(lch_base + I_MINT), indexHeap(lch_base + I_MAXT), indexHeap(lch_base + I_MINA), indexHeap(lch_base + I_MAXA), tMin, tMax, aMin, aMax)) {
            doGetMessage(result, lch, aMin, aMax, tMin, tMax);
        }
        if (rectOverlap(indexHeap(rch_base + I_MINT), indexHeap(rch_base + I_MAXT), indexHeap(rch_base + I_MINA), indexHeap(rch_base + I_MAXA), tMin, tMax, aMin, aMax)) {
            doGetMessage(result, rch, aMin, aMax, tMin, tMax);
        }
    }

    @Override
    public synchronized List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {

        boolean firstFlag = false;

        if (state == 1) {
            System.out.println("[" + new Date() + "]: getMessage() started");
            createIndex();
            state = 2;
            firstFlag = true;
        }

        System.gc();

        ArrayList<Message> result = new ArrayList<Message>();

        doGetMessage(result, 1, (int) aMin, (int) aMax, (int) tMin, (int) tMax);

        if (haveIncompressibleRecord) {
            //for (Message msg: result) {
            //	System.out.println(msg.getT());
            //}
            //System.out.println(String.format("%d %d %d %d", aMin, aMax, tMin, tMax));
            for (Message msg : incompressibleRecords) {
                if (pointInRectL(msg.getT(), msg.getA(), tMin, tMax, aMin, aMax)) {
                    result.add(msg);
                    //System.out.println("put: " + msg.getT());
                }
            }

            doSortMessage(result);

        }

        // 为最后的查询平均值预热JVM
        //    	avgQueryCount.set(-1);
        getAvgValue(aMin, aMax, tMin, tMax);
        if (firstFlag) {
            for (int i = 0; i < 30000; i++) {
                //    			avgQueryCount.set(-1);
                getAvgValue(aMin, aMax, tMin, tMax);
            }
        }

        return result;
    }

    private static class AvgResult {
        long sum;

        int cnt;
    }

    //    private static final AtomicInteger avgQueryCount = new AtomicInteger(0);
    //    private static final AtomicLong avgQueryCost = new AtomicLong(0);
    //    private static final AtomicInteger avgQueryLeafCount = new AtomicInteger(0);
    //    private static final AtomicLong avgQueryLeafCost = new AtomicLong(0);
    //    private static final AtomicLong avgQueryStartTime = new AtomicLong(0);

    private static void doGetAvgValue(AvgResult result, int cur, int aMin, int aMax, int tMin, int tMax) {

        if (cur >= HEAP_LEAF_BASE) {

            //    		long st = System.nanoTime();

            int tBase = indexHeap(cur * I_SIZE + I_TBASE);

            int tRelMin = tMin - tBase;
            int tRelMax = Math.min(254, tMax - tBase);

            int aRelMin = aMin - tBase + MessageCompressor.AOFFSET;
            int aRelMax = aMax - tBase + MessageCompressor.AOFFSET;

            long l = (cur - HEAP_LEAF_BASE) * L_NREC;
            long r = l + L_NREC;

            int cnt = 0;
            int sum = 0;

            long lUpper = r;
            while (lUpper - l > 1) {
                long m = (l + lUpper) / 2;
                int tRel = ((int) unsafe.getByte(memBase + m * 3 + 2) & 0xFF);
                if (tRel < tRelMin) {
                    l = m;
                } else {
                    lUpper = m;
                }
            }

            for (long i = l; i < r; i++) {

                int tRel = ((int) unsafe.getByte(memBase + i * 3 + 2) & 0xFF);
                if (tRel > tRelMax) {
                    break;
                }

                if (tRelMin <= tRel) {

                    int aRel = ((int) unsafe.getShort(memBase + i * 3) & 0xFFFF);

                    if (aRelMin <= aRel && aRel <= aRelMax) {
                        sum += aRel;
                        cnt++;
                    }
                }
            }

            result.sum += sum + cnt * ((long) tBase - MessageCompressor.AOFFSET);
            result.cnt += cnt;

            //    		avgQueryLeafCost.addAndGet(System.nanoTime() - st);
            //    		avgQueryLeafCount.incrementAndGet();

            return;
        }

        int lch = cur * 2;
        int lch_base = I_SIZE * lch;
        int lch_minT = indexHeap(lch_base + I_MINT);
        int lch_maxT = indexHeap(lch_base + I_MAXT);
        int lch_minA = indexHeap(lch_base + I_MINA);
        int lch_maxA = indexHeap(lch_base + I_MAXA);

        int rch = cur * 2 + 1;
        int rch_base = I_SIZE * rch;
        int rch_minT = indexHeap(rch_base + I_MINT);
        int rch_maxT = indexHeap(rch_base + I_MAXT);
        int rch_minA = indexHeap(rch_base + I_MINA);
        int rch_maxA = indexHeap(rch_base + I_MAXA);

        if (rectOverlap(lch_minT, lch_maxT, lch_minA, lch_maxA, tMin, tMax, aMin, aMax)) {
            if (rectInRect(lch_minT, lch_maxT, lch_minA, lch_maxA, tMin, tMax, aMin, aMax)) {
                result.sum += indexHeapL(lch_base + I_SUML);
                result.cnt += indexHeap(lch_base + I_CNT);
            } else {
                doGetAvgValue(result, lch, aMin, aMax, tMin, tMax);
            }
        }

        if (rectOverlap(rch_minT, rch_maxT, rch_minA, rch_maxA, tMin, tMax, aMin, aMax)) {
            if (rectInRect(rch_minT, rch_maxT, rch_minA, rch_maxA, tMin, tMax, aMin, aMax)) {
                result.sum += indexHeapL(rch_base + I_SUML);
                result.cnt += indexHeap(rch_base + I_CNT);
            } else {
                doGetAvgValue(result, rch, aMin, aMax, tMin, tMax);
            }
        }
    }

    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {

        //    	int qId = avgQueryCount.getAndIncrement();
        //    	if (qId == 0) {
        //    		avgQueryStartTime.set(System.nanoTime());
        //    	    avgQueryCost.set(0);
        //    	    avgQueryLeafCount.set(0);
        //    	    avgQueryLeafCost.set(0);
        //    	}
        //    	long st = System.nanoTime();

        AvgResult result = new AvgResult();
        doGetAvgValue(result, 1, (int) aMin, (int) aMax, (int) tMin, (int) tMax);

        if (haveIncompressibleRecord) {
            for (Message msg : incompressibleRecords) {
                if (pointInRectL(msg.getT(), msg.getA(), tMin, tMax, aMin, aMax)) {
                    result.sum += msg.getA();
                    result.cnt++;
                }
            }
        }

        //    	avgQueryCost.addAndGet(System.nanoTime() - st);
        //
        //    	if (qId >= 30652 - 1) {
        //    		long avgQueryEndTime = System.nanoTime();
        //
        //    		System.out.println("====== SUMMARY ======\n" + new Date().toString());
        //    		System.out.println(String.format("total query time: %.4f", (avgQueryEndTime - avgQueryStartTime.get()) * 1e-6));
        //    		System.out.println(String.format("cost : leaf %.1f query %.1f (%.4f)", avgQueryLeafCost.get() * 1e-6, avgQueryCost.get() * 1e-6, (double)avgQueryLeafCost.get() / avgQueryCost.get()));
        //    		System.out.println(String.format("count: leaf %d query %d (%.4f)", avgQueryLeafCount.get(), avgQueryCount.get(), (double)avgQueryLeafCount.get() / avgQueryCount.get()));
        //    	}

        return result.cnt == 0 ? 0 : result.sum / result.cnt;
    }

}
