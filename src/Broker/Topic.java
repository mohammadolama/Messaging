package Broker;

import java.io.File;
import java.util.HashMap;

class Topic {
    private String name;

    private File topicFile;
    private TopicWriter topicWriter;
    private HashMap<String, TopicReader> topicReaders;

    Topic(String name) {
        this.name = name;
        topicFile = new File("topics" + File.separator + name + ".dat");
        if (topicFile.exists()) {
            topicFile.delete();
        } else if (topicFile.getParentFile() != null && topicFile.getParentFile().mkdirs()) {
        }
        topicWriter = new TopicWriter(this);
        topicReaders = new HashMap<>();
    }

    File getTopicFile() {
        return topicFile;
    }

    void addGroup(String groupName, int numberofconsumers) {
        topicReaders.put(groupName, new TopicReader(this, groupName, numberofconsumers));
    }


    /**
     * This method is used to get the first value in the topic file which is not read in the given group yet, and serve it for the appropriate consumer.
     *
     * @return the value of the first remained item.
     */
    int getFirstValueOf(String groupName, String consumerName, int numberofconsumers) {
        if (!topicReaders.containsKey(groupName)) {
            addGroup(groupName, numberofconsumers);
        }

        return topicReaders.get(groupName).get(consumerName);
    }

    /**
     * This method is used to put the given value at the tail of the topic file.
     *
     * @param value the value to be put at the end of the topic file
     * @return Nothing.
     */
    void put(String producerName, int value) {
        topicWriter.put(producerName, value);
    }


    void notifyTopicReaders() {
        for (TopicReader value : topicReaders.values()) {
            value.notifyReader();
        }

    }

}
