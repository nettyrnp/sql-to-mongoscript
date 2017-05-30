package org.mongo.client;

public class Utils {
  
  public static final String HOST = "localhost";
  public static final int PORT = 27017;
  
  public static void error(String message) {
//   TODO log.error(message); 
    throw new RuntimeException(message);
  }

  public static void info(String message) {
//   TODO log.info(message); 
    System.out.println(message);
  }

}
