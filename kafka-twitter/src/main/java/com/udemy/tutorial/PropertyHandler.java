package com.udemy.tutorial;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author Alexander Shakhov
 */
@Slf4j
public final class PropertyHandler {

  public static String getValue(@NonNull String key) {
    final Properties property = new Properties();
    final ClassLoader loader = Thread.currentThread().getContextClassLoader();
    try (final InputStream stream = loader.getResourceAsStream("credentials.properties")) {
      property.load(stream);
    } catch (IOException e) {
      log.error("Property not found: ", e.getCause());
    }
    return property.getProperty(key);
  }
}
