package com.udemy.tutorial;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static com.udemy.tutorial.Base.TOPIC_DEFAULT;
import static com.udemy.tutorial.Base.printConsumerRecordMetaData;

@Slf4j
public class MyKafkaConsumer {
  public static void main(String[] args) {
    Properties properties = Base.getProperties("my-first-application");

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
    consumer.subscribe(Collections.singletonList(TOPIC_DEFAULT));

    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
      for (ConsumerRecord record : records) {
        printConsumerRecordMetaData(record);
      }
    }

  }
}
