package com.udemy.tutorial;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

import static com.udemy.tutorial.Base.*;

@Slf4j
public class MyKafkaConsumerThreads {

  public MyKafkaConsumerThreads() {
  }

  public static void main(String[] args) {
    new MyKafkaConsumerThreads().run();
  }

  private void run() {
    CountDownLatch latch = new CountDownLatch(1);
    ConsumerRunnable consumerRunnable = new ConsumerRunnable(latch);

    Thread thread = new Thread(consumerRunnable);
    thread.start();

    // add a shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      log.warn("Caught shutdown hook");
      consumerRunnable.shutdown();
      try {
        latch.await();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      log.info("Application has exited");
    }));

    try {
      latch.await();
    } catch (InterruptedException e) {
      log.error("Application got interrupted", e);
    } finally {
      log.warn("Application is closing");
    }
  }

  public class ConsumerRunnable implements Runnable {

    private final CountDownLatch latch;
    private final KafkaConsumer<String, String> consumer;

    public ConsumerRunnable(CountDownLatch latch) {
      this.latch = latch;
      this.consumer = new KafkaConsumer<>(getProperties("my-first-application"));

      consumer.subscribe(Collections.singletonList(TOPIC_DEFAULT));
    }

    @Override
    public void run() {
      try {
        while (true) {
          ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
          for (ConsumerRecord<String, String> record : records) {
            printConsumerRecordMetaData(record);
          }
        }
      } catch (WakeupException e) {
        log.warn("Received a shutdown signal!");
      } finally {
        consumer.close();
        latch.countDown();
      }
    }

    public void shutdown() {
      // the wakeup() method is a special method to interrupt consumer.poll()
      // it will throw the exception WakeUpException
      consumer.wakeup();
    }
  }
}
