package org.mongo.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class ClientTest {
  
  @Test
  public void testParsing() {
    String sql = "SELECT address.zipcode FROM restaurants WHERE address.zipcode=11691 AND address.street=Seagirt Avenue OR restaurant_id=40359480 OR address.street=Beach 25 Street";
    ParsedSql parsedSql = new ParsedSql(sql);
    
    assertEquals("Wrong extraction of SELECT clause", parsedSql.getFields(), "address.zipcode");
    
    assertEquals("Wrong extraction of FROM clause", parsedSql.getFrom(), "restaurants");
    
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

  /** 
   * Uncomment this test when you have MongoDB running at "127.0.0.1:27017" 
   * or other address where the 'primer-dataset.json' DB is placed.
   * 
  @Test
  public void testQuery() {
    String sql = "SELECT restaurant_id FROM restaurants" + 
        "  WHERE address.zipcode=11691" + 
        "    AND address.street=Seagirt Avenue" + 
        "    OR restaurant_id>=40359480" + 
        "    AND restaurant_id<40359485" + 
        "    Order by address.zipcode";

    Client client = new Client();
    if (!client.connect("test"))
      return;
    client.query(sql);
    
    assertEquals("Wrong size of resultSet", 2, client.getLastResultSet().size());
    assertTrue("Wrong results", client.getLastResultSet().contains("\"40359480\""));
    assertTrue("Wrong results", client.getLastResultSet().contains("\"41269872\""));
  }
  */
 
}
