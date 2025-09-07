package com.hmdp.utils;

public interface ILock {
    boolean tryLock(long timeout);

    void unlock();
}
