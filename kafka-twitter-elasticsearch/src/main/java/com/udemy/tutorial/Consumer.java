package com.udemy.tutorial;

import lombok.NonNull;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

import static com.udemy.tutorial.Base.getProperties;

/**
 * @author Alexander Shakhov
 */
public class Consumer {

  public KafkaConsumer<String, String> getConsumer(@NonNull String topic) {
    Properties properties = getProperties("twitter-elasticsearch");
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
    consumer.subscribe(Collections.singleton(topic));
    return consumer;
  }
}
