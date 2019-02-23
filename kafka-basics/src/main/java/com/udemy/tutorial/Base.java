package com.udemy.tutorial;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class Base {

  public static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
  public static final String TOPIC_DEFAULT = "first_topic";
  public static final String TOPIC_TWITTER = "twitter_tweets";

  public static Properties getProperties(@NonNull String groupId) {
    final Properties properties = new Properties();

    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Base.BOOTSTRAP_SERVER);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    return properties;
  }

  /**
  * Prints Key, Value, Partition and Offset
  * */
  public static void printConsumerRecordMetaData(ConsumerRecord record) {
    log.info("Topic     {}", record.topic());
    log.info("Key:      {}", record.key());
    log.info("Value:    {}", record.value());
    log.info("Partition:{}", record.partition());
    log.info("Offset:   {}", record.offset()
            + "\n-----------------------------");
  }
}
