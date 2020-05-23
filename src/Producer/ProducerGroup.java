package Producer;

import Broker.MessageBroker;
import Util.LOGGER;

import java.io.File;
import java.util.ArrayList;

public class ProducerGroup extends Thread {
    private ArrayList<Producer> producers;
    private File producerGroupDirectory;
    private MessageBroker messageBroker;
    private String topicName;

    public ProducerGroup(MessageBroker messageBroker, File producerGroupDirectory, String topicName) {
        this.messageBroker = messageBroker;
        this.producerGroupDirectory = producerGroupDirectory;
        this.topicName = topicName;
        this.messageBroker.addTopic(topicName);
        producers = new ArrayList<>();
        LOGGER.BrokerLog("producer group created, topic name : " + topicName);
    }

    private void initialize() {
        for(File file: producerGroupDirectory.listFiles()) {
            producers.add(new Producer(messageBroker,topicName, file.getName(), file));
        }
    }

    public void run() {
        initialize();

        for(Producer producer: producers) {
            producer.start();
        }

    }
}
