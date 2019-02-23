package com.udemy.tutorial;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import java.util.List;
import java.util.concurrent.BlockingQueue;

import static com.udemy.tutorial.PropertyHandler.getValue;

/**
 * @author Alexander Shakhov
 */
@NoArgsConstructor
public class TwitterClient {

  public Client createTwitterClient(@NonNull BlockingQueue<String> messageQueue, @NonNull String... theme) {

    final Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
    final StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

    List<String> terms = Lists.newArrayList(theme);
    hosebirdEndpoint.trackTerms(terms);
    Authentication hosebirdAuth = new OAuth1(getValue("twitter.key"),
            getValue("twitter.secret"),
            getValue("twitter.access.token"),
            getValue("twitter.token.secret"));

    ClientBuilder builder = new ClientBuilder()
            .name("Hosebird-Client-01")
            .hosts(hosebirdHosts)
            .authentication(hosebirdAuth)
            .endpoint(hosebirdEndpoint)
            .processor(new StringDelimitedProcessor(messageQueue));

    return builder.build();
  }

}
