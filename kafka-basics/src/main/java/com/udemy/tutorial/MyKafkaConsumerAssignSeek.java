package com.udemy.tutorial;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.udemy.tutorial.Base.*;

@Slf4j
public class MyKafkaConsumerAssignSeek {
  public static void main(String[] args) {
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getProperties("my-first-application"));

    // assign and seek are mostly used to replay data or fetch a specific message
    TopicPartition partitionToReadFrom = new TopicPartition(TOPIC_DEFAULT, 0);
    long offsetToReadFrom = 15L;
    consumer.assign(Collections.singletonList(partitionToReadFrom));

    // seek
    consumer.seek(partitionToReadFrom, offsetToReadFrom);

    final int numberOfMessagesToRead = 5;

    AtomicBoolean keepOnReading = new AtomicBoolean(true);
    AtomicInteger numberOfMessagesReadSoFar = new AtomicInteger(0);
    while (keepOnReading.get()) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

      for (ConsumerRecord<String, String> record : records) {
        numberOfMessagesReadSoFar.getAndIncrement();
        printConsumerRecordMetaData(record);

        if (numberOfMessagesReadSoFar.get() >= numberOfMessagesToRead) {
          keepOnReading.set(false);
          break; // to exit the for loop
        }
      }
    }
    log.info("Exiting the application");
  }
}
