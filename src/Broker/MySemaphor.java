package Broker;

import Util.ThreadColor;

class MySemaphor {

    private final Object object;
    private boolean free;
    private volatile int signal;

    MySemaphor() {
        object = new Object();
        free = true;
        signal = 0;
    }

    void acquire() {
        synchronized (object) {
            while (signal <= 0 || !free) {
                try {
                    object.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            signal--;
            free = false;
        }
    }

    void release() {
        synchronized (object) {
            free = true;
            object.notifyAll();
        }
    }

    void addsignal(int a) {
        synchronized (object) {
            signal += a;
            object.notifyAll();
        }
    }
}
