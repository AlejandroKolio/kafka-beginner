package com.udemy.tutorial;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

import static com.udemy.tutorial.Base.TOPIC_TWITTER;

/**
 * @author Alexander Shakhov
 */
@Slf4j
public class ElasticSearchConsumer {

  private static ObjectMapper mapper = new ObjectMapper();

  private static final String USERNAME = "<your username>";
  private static final String PASSWORD = "<your password>";
  private static final String HOST_NAME = "<your host name>";

  public static void main(String[] args) {
    RestHighLevelClient client = new ElasticSearchConsumer().client();
    Consumer consumer = new Consumer();

    try {
      KafkaConsumer<String, String> kafkaConsumer = consumer.getConsumer(TOPIC_TWITTER);

      while (true) {
        ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));

        final int numberOfRecords = records.count();
        log.info("Records {} received", numberOfRecords);

        final BulkRequest bulkRequest = new BulkRequest();
        for (ConsumerRecord<String, String> record : records) {
          final String tweetId = extractId(record.value()).orElseThrow(RuntimeException::new);
          IndexRequest index = new IndexRequest("twitter", "tweets", tweetId).source(record.value(), XContentType.JSON);
          bulkRequest.add(index);
        }
        if (numberOfRecords > 0) {
          final BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
          log.info("Committing the offset...");
          kafkaConsumer.commitSync();
          log.info("Committed successfully!");
        }
      }

      //client.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static Optional<String> extractId(@NonNull String value) {
    try {
      JsonNode node = mapper.readTree(value);
      final String id = node.get("id_str").asText();
      if (Objects.nonNull(id)) {
        return Optional.of(id);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return Optional.empty();
  }

  private RestHighLevelClient client() {
    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(USERNAME, PASSWORD));

    RestClientBuilder builder = RestClient.builder(new HttpHost(HOST_NAME, 443, "https"))
            .setHttpClientConfigCallback(httpAsyncClientBuilder ->
                    httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
    return new RestHighLevelClient(builder);
  }
}
