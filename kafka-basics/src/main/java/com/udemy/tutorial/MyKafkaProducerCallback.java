package com.udemy.tutorial;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import static com.udemy.tutorial.Base.*;

@Slf4j
public class MyKafkaProducerCallback {

  public static void main(String[] args) {
    KafkaProducer<String, String> producer = new KafkaProducer<>(getProperties("my-first-application"));
    for (int i = 0; i < 100; i++) {
      String key = "id_" + i;
      String value = "Produced message to Kafka #" + i;

      ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_DEFAULT, key, value);
      producer.send(record, (recordMetadata, e) -> {
        if (e == null) {
          log.info("Received new metadata. \n" +
                  "Topic:" + recordMetadata.topic() + "\n" +
                  "Partition: " + recordMetadata.partition() + "\n" +
                  "Offset: " + recordMetadata.offset() + "\n" +
                  "Timestamp: " + recordMetadata.timestamp());
        } else {
          log.error("Error while producing", e);
        }
      });
    }
    producer.close();
  }
}
