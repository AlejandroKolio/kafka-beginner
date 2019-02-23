package com.udemy.tutorial;

import com.twitter.hbc.core.Client;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.udemy.tutorial.Base.getProperties;

/**
 * @author Alexander Shakhov
 */
@Slf4j
@NoArgsConstructor
public class TwitterProducer {

  public static void main(String[] args) {
    new TwitterProducer().run("bitcoins");
  }

  public void run(@NonNull String... theme) {
    BlockingQueue<String> messageQueue = new LinkedBlockingQueue<>(100000);
    TwitterClient tc = new TwitterClient();
    final Client client = tc.createTwitterClient(messageQueue, theme);
    client.connect();
    KafkaProducer<String, String> producer = createKafkaProducer();

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      log.warn("Shutting down client & producer");
      client.stop();
      producer.close();
    }));

    while (!client.isDone()) {
      String msg = null;
      try {
        msg = messageQueue.poll(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        e.printStackTrace();
        client.stop();
      }
      if (Objects.nonNull(msg)) {
        log.info(msg);
        producer.send(new ProducerRecord<>("twitter_tweets", msg), (recordMetadata, e) -> {
          if (Objects.nonNull(e)) {
            log.error("Error happened {}", e);
          }
        });
      }

    }
  }
  private KafkaProducer<String, String> createKafkaProducer() {
    return new KafkaProducer<>(getProperties("my-first-application"));
  }
}
