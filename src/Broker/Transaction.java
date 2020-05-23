package Broker;

import Util.LOGGER;

import java.util.LinkedList;
import java.util.Queue;

class Transaction {

    private final TopicWriter topicWriter;
    private String producerName;
    private Queue<Integer> values;

    Transaction(TopicWriter topicWriter, String producerName) {
        this.topicWriter = topicWriter;
        this.producerName = producerName;
        values = new LinkedList<>();
    }

    void put(int value) {
        values.add(value);
    }

    void commit() {
        synchronized (topicWriter) {
            LOGGER.BrokerLog("producer " + producerName + " start committing a transaction");
            topicWriter.writeValue(0);
            while (!values.isEmpty()) {
                int value =values.remove();
                LOGGER.BrokerLog("producer: " + producerName + " send message: " + value);
                topicWriter.writeValue(value);

            }
            topicWriter.writeValue(-1);
            LOGGER.BrokerLog("producer " + producerName + " end committing a transaction");

            topicWriter.getTopic().notifyTopicReaders();
        }
    }
}
