package Consumer;

import Broker.MessageBroker;
import Broker.NoSuchTopicException;
import Util.LOGGER;

public class Consumer extends Thread {
    private ConsumerGroup consumerGroup;
    private String consumerName;

    Consumer(ConsumerGroup consumerGroup, String consumerName) {
        this.consumerGroup = consumerGroup;
        this.consumerName = consumerName;
        LOGGER.BrokerLog("consumer created, consumer name:" + consumerName);

    }

    private int getValue() throws NoSuchTopicException {
        return this.getMessageBroker().getTopicValue(getTopicName(), consumerGroup.getGroupName(), this.consumerName, consumerGroup.getNumberOfConsumers());
    }

    public void run() {
//        try {
//            Thread.sleep(100);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
        while (true) {
            int value = 0;
            try {
                value = getValue();
                consumerGroup.performAction(this, value);
            } catch (NoSuchTopicException e) {
                e.printStackTrace();
            }

        }
    }

    private MessageBroker getMessageBroker() {
        return consumerGroup.getMessageBroker();
    }

    String getConsumerName() {
        return consumerName;
    }

    private String getTopicName() {
        return consumerGroup.getTopicName();
    }
}
