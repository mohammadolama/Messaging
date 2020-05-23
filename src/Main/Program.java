package Main;

import Broker.MessageBroker;
import Consumer.ConsumerGroup;
import Producer.ProducerGroup;

import java.io.File;

class Program {

    private String[] args;
    private MessageBroker messageBroker;

    Program(String args[]) {
        this.args = args;
        messageBroker = new MessageBroker();
    }

    private File getProducerGroupDirectory(String name) {
        File producerDirectory = new File(name);
//        if(args.length>0) {
//            producerDirectory = new File(args[0]);
//        }

        return producerDirectory;
    }

    void run() {

        File file2=new File("Output/");
        if (!file2.isDirectory()){
            file2.mkdirs();
        }

        String SourceDir="Datas/";
        File directory=new File(SourceDir);
        for (File file : directory.listFiles()){
                String filedir = String.format(SourceDir + file.getName());
                File producerGroupDirectory = getProducerGroupDirectory(filedir);
                String topicName = producerGroupDirectory.getName();



                String consumerGroupName = topicName + "Readers1";
                String consumerGroupName1 = topicName + "Readers2";
            File consumerGroupFile = new File("Output/"+consumerGroupName + ".txt");
            File consumerGroupFile1 = new File("Output/"+consumerGroupName1 + ".txt");
                int numberOfConsumers = 10;
                int numberOfConsumers1=8;
                ProducerGroup producerGroup = new ProducerGroup(messageBroker, producerGroupDirectory, topicName);
                ConsumerGroup consumerGroup = new ConsumerGroup(messageBroker, topicName, consumerGroupName, consumerGroupFile, numberOfConsumers);
            ConsumerGroup consumerGroup1 = new ConsumerGroup(messageBroker, topicName, consumerGroupName1, consumerGroupFile1, numberOfConsumers1);


                producerGroup.start();
                consumerGroup.start();
                consumerGroup1.start();
        }
    }
}

