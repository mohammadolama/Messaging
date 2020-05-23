package Broker;

import Util.ThreadColor;

import java.io.*;

class TopicReader {

    private RandomAccessFile topicFile;

    private Topic topic;
    private String groupName;
    private volatile String consumerName;
    private int numberOfConsumers;
    private final MySemaphor mySemaphor;

    TopicReader(Topic topic, String groupName, int numberOfConsumers) {
        this.topic = topic;
        this.groupName = groupName;
        this.numberOfConsumers = numberOfConsumers;
        mySemaphor = new MySemaphor();
        try {
            topicFile = new RandomAccessFile(topic.getTopicFile(), "rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    int get(String consumerName) {
        if (!consumerName.equals(this.consumerName)) {
            mySemaphor.acquire();
        }
        if (!hasnext()) {
            endFile();
            mySemaphor.release();
            return -20;
        }
        int value = readNext();
        if (value == 0) {
            this.consumerName = consumerName;
            value = readNext();
        }
        if (this.consumerName != null) {
            if (nextValue() == -1) {
                readNext();
                this.consumerName = null;
                mySemaphor.release();
                return value;
            }
            return value;
        }
        mySemaphor.release();
        return value;
    }


    private boolean hasnext() {
        try {
            return topicFile.getFilePointer() < topicFile.length();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private int nextValue() {
        try {
            long pointer = this.topicFile.getFilePointer();
            int i = this.topicFile.readInt();
            this.topicFile.seek(pointer);
            return i;
        } catch (EOFException e) {
            return -20;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private int readNext() {
        try {
            return topicFile.readInt();
        } catch (EOFException e) {
            return -20;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -20;
    }

    void notifyReader() {
        mySemaphor.addsignal(1);
    }


    private void endFile() {
        mySemaphor.addsignal(this.numberOfConsumers + 2);
    }

}
