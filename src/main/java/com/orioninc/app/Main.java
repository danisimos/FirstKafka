package com.orioninc.app;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        createTopic();
    }

    private static void createTopic() {
        System.out.println("Enter topic name:");
        String topicName = new Scanner(System.in).nextLine();

        int partitions = 1;
        short replicationFactor = 1;

        NewTopic topic = new NewTopic(topicName, partitions, replicationFactor);

        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        Admin admin = Admin.create(properties);
        admin.createTopics(List.of(topic));

        System.out.println("new topic with name " + topicName + " created");

        produceMessages(topicName);
        consumeMessages(topicName);
    }

    private static void produceMessages(String topicName) {
        Thread thread = new Thread(() -> {
            Properties properties = new Properties();
            properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

            Producer<String, String> producer = new KafkaProducer<>(properties);

            Scanner scanner = new Scanner(System.in);

            while (true) {
                System.out.println("Enter a new message: ");
                String message = scanner.nextLine();
                if(message.equals("exit")) {
                    break;
                }

                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, "Message", message);

                producer.send(producerRecord);

                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, "exit", "exit");
            producer.send(producerRecord);
        });

        thread.start();
    }

    private static void consumeMessages(String topicName) {
        Thread thread = new Thread(() -> {
            Properties properties = new Properties();
            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
            properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

            Consumer<String, String> consumer = new KafkaConsumer<>(properties);

            consumer.subscribe(List.of(topicName));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record: records) {
                    System.out.printf("Message received: offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
                    if(record.key().equals("exit")) {
                        System.out.println("exiting");
                        return;
                    }
                }
            }
        });

        thread.start();
    }
}
