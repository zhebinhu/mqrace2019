package io.openmessaging;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by huzhebin on 2019/08/01.
 */
public class LRUCache<K, V> extends LinkedHashMap<K, V> {
    private int cacheSize;

    //private Map.Entry<K, V> eldest;

    private Lock lock = new ReentrantLock();

    public LRUCache(int cacheSize) {
        super(cacheSize, 0.75f, false);
        this.cacheSize = cacheSize;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        if(size() > cacheSize){
            //this.eldest = eldest;
            return true;
        }
        return false;
    }

//    public V getOldest() {
//        if (eldest == null) {
//            return null;
//        }
//        return eldest.getValue();
//    }

    @Override
    public V get(Object key) {
        lock.lock();
        try {
            return super.get(key);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public V put(K key, V value) {
        lock.lock();
        try {
            return super.put(key, value);
        } finally {
            lock.unlock();
        }
    }
}
