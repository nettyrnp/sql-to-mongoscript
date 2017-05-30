package org.mongo.client;

public class Utils {
  
  public static final String HOST = "localhost";
  public static final int PORT = 27017;

  public static void error(String message) {
//    log.error(message); TODO
    throw new RuntimeException(message);
  }

}
