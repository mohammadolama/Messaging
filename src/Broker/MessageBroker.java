package Broker;

import java.util.HashMap;
import java.util.Map;

public class MessageBroker {
    private Map<String, Topic> topics = new HashMap<>();

    public void addTopic(String name) {
        topics.put(name, new Topic(name));
    }

    public void put(String topic, String producerName, int value) {
        topics.get(topic).put(producerName, value);
    }

    public int getTopicValue(String topic, String groupName, String consumerName, int numberofconsumers) throws NoSuchTopicException {
        if (!topics.containsKey(topic))
            throw new NoSuchTopicException(topic);

        return topics.get(topic).getFirstValueOf(groupName, consumerName, numberofconsumers);

    }

    public void addConsumerGroup(String topic, String groupName, int numberofconsumers)
            throws NoSuchTopicException {
        if (!topics.containsKey(topic))
            throw new NoSuchTopicException(topic);
        topics.get(topic).addGroup(groupName, numberofconsumers);
    }
}
