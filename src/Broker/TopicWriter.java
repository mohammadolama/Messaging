package Broker;

import Util.LOGGER;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;

class TopicWriter {
    RandomAccessFile buffer;
    private Topic topic;
    private HashMap<String, Transaction> transactions;

    TopicWriter(Topic topic) {
        try {
            buffer = new RandomAccessFile(topic.getTopicFile(), "rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        this.topic = topic;
        transactions = new HashMap<>();
    }

    synchronized void put(String producerName, int value) {
        if (value <= 0) {
            handleTransactionOperation(producerName, value);
        } else {
            handleInsertOperation(producerName, value);
        }
    }

    private synchronized void handleTransactionOperation(String producerName, int value) {
        switch (value) {
            case 0:
                startTransaction(producerName);
                break;
            case -1:
                commitTransaction(producerName);
                break;
            case -2:
                cancelTransaction(producerName);
        }
    }

    private synchronized void handleInsertOperation(String producerName, int value) {
        if (transactions.containsKey(producerName)) {
            transactions.get(producerName).put(value);
        } else {
            synchronized (this) {
                writeValue(value);
                this.topic.notifyTopicReaders();
                LOGGER.BrokerLog("producer: " + producerName + " send message: " + value);
            }
        }
    }

    private void addTransaction(String producerName) {
        transactions.put(producerName, new Transaction(this, producerName));
    }

    /**
     * This method is used to start a transaction for putting a transaction of values inside the buffer.
     *
     * @return Nothing.
     */
    private void startTransaction(String producerName) {
        if (transactions.containsKey(producerName)) {
            LOGGER.BrokerLog("Producer " + producerName + " start transaction but dont finalize previous transaction.");
            commitTransaction(producerName);
            transactions.remove(producerName);
        }
        addTransaction(producerName);
        LOGGER.BrokerLog("Producer " + producerName + " start a transaction.");
    }

    /**
     * This method is used to end the transaction for putting a its values inside the file.
     *
     * @return Nothing.
     */
    private void commitTransaction(String producerName) {
        if (transactions.containsKey(producerName)) {
            transactions.get(producerName).commit();
            transactions.remove(producerName);
        } else {
            LOGGER.BrokerLog("producer " + producerName + " commit a non-existing transaction");
        }
    }

    /**
     * This method is used to cancel a transaction.
     *
     * @return Nothing.
     */
    private void cancelTransaction(String producerName) {
        if (transactions.containsKey(producerName)) {
            transactions.remove(producerName);
            LOGGER.BrokerLog("Producer " + producerName + " Cancle a transaction");
        } else {
            LOGGER.BrokerLog("Producer " + producerName + " Cancle a non-existing transaction");
        }
    }

    synchronized void writeValue(int value) {
        try {
            buffer.writeInt(value);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    Topic getTopic() {
        return topic;
    }
}
