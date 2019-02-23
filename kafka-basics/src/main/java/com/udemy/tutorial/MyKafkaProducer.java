package com.udemy.tutorial;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import static com.udemy.tutorial.Base.TOPIC_DEFAULT;

public class MyKafkaProducer {

  public static void main(String[] args) {
    KafkaProducer<String, String> producer = new KafkaProducer<>(Base.getProperties("my-first-application"));
    ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_DEFAULT, "key", "Hello World!!!");

    producer.send(record);
    producer.close();
  }
}
