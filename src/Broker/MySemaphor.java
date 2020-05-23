package Broker;

class MySemaphor {

    private final Object object;
    private boolean free;

    MySemaphor() {
        object = new Object();
        free = true;
    }

    void acquire() {
        synchronized (object) {
            while (!free) {
                try {
                    object.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            free = false;
        }
    }

    void release() {
        synchronized (object) {
            free = true;
            object.notify();
        }
    }
}
