package org.mongo.client;

import static org.junit.Assert.*;

import org.junit.Test;
import org.mongo.client.ParsedSql;

public class ParsedSqlTest {
  
  @Test
  public void testParsing() {
    String sql = "SELECT address.zipcode FROM restaurants WHERE address.zipcode=11691 AND address.street=Seagirt Avenue OR restaurant_id=40359480 OR address.street=Beach 25 Street";
    ParsedSql parsedSql = new ParsedSql(sql);
    assertEquals("Wrong extraction of SELECT clause", parsedSql.getFields(), "address.zipcode");
    assertEquals("Wrong extraction of FROM clause", parsedSql.getFrom(), "restaurants");
//    assertEquals("Wrong extraction of WHERE clause", parsedSql.getWhereClause().toString(), "{ \"$or\" : [ { \"$and\" : [ { \"address.zipcode\" : \"11691\"} , { \"address.street\" : \"Seagirt Avenue\"}]} , { \"$and\" : [ { \"restaurant_id\" : \"40359480\"}]} , { \"$and\" : [ { \"address.street\" : \"Beach 25 Street\"}]}]}");
    assertTrue("Wrong extraction of WHERE clause", parsedSql.getWhereClause().toString().contains("{ \"address.zipcode\" : \"11691\"}"));
    assertTrue("Wrong extraction of WHERE clause", parsedSql.getWhereClause().toString().contains("{ \"address.street\" : \"Seagirt Avenue\"}"));
    assertTrue("Wrong extraction of WHERE clause", parsedSql.getWhereClause().toString().contains("{ \"$and\" : [ { \"address.street\" : \"Beach 25 Street\"}]}"));
    assertTrue("Wrong extraction of WHERE clause", parsedSql.getWhereClause().toString().contains("{ \"$and\" : [ { \"restaurant_id\" : \"40359480\"}]}"));
  }

  @Test
  public void testParsing2() {
    String sql = "SELECT * FROM restaurants WHERE address.zipcode=11691 AND address.street=Seagirt Avenue OR restaurant_id=40359480 OR address.street=Beach 25 Street";
    ParsedSql parsedSql = new ParsedSql(sql);
    assertEquals("Wrong extraction of SELECT clause", parsedSql.getFields(), "*");
  }
 
}
