package Broker;

import java.io.*;

class TopicReader {

    private RandomAccessFile topicFile;

    private Topic topic;
    private String groupName;
    private volatile String consumerName;
    private volatile boolean transactionOpened;
    private final MySemaphor mySemaphor;

    TopicReader(Topic topic, String groupName) {
        this.topic = topic;
        this.groupName = groupName;
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

        if (nextValue() == -20) {
            mySemaphor.release();
            return -20;
        }
        int value=readNext();
        if (value == 0) {
            this.consumerName = consumerName;
            value=readNext();
        }
        if (this.consumerName != null) {
            if (nextValue() == -1) {
                readNext();
                this.consumerName=null;
                mySemaphor.release();
                return value;
            }
            return value;
        }
        mySemaphor.release();
        return value;
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

}
