package com.udemy.tutorial.model;

import lombok.Builder;
import lombok.Data;

/**
 * @author Alexander Shakhov
 */
@Data
@Builder
public class Tweet {
  private String id;
  private String author;
}
