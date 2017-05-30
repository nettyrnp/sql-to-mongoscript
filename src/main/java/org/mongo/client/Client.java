package org.mongo.client;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Scanner;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;

/*
 * An alternative MongoClient that can be run both as a console or a GUI
 */
public class Client {

  // if I use a GUI or not
  private ClientGUI cg;

  // the server
  private MongoClient mongo;
  private String dbName;
  private DB db;
  private ObjectMapper _mapper = new ObjectMapper();
  private Collection<String> _lastResultSet = new LinkedHashSet<String>();

  /*
   * Constructor called by console mode server: the server address port: the
   * port number username: the username
   */
  Client() {
    // which calls the common constructor with the GUI set to null
    this(null);
  }

  /*
   * Constructor call when used from a GUI in console mode the ClienGUI
   * parameter is null
   */
  Client(ClientGUI cg) {
    // save if we are in GUI mode or not
    this.cg = cg;
  }

  /*
   * To start the dialog
   */
  public boolean connect(String dbName) {
    this.dbName = dbName;
    // try to connect to the server
    try {
      mongo = new MongoClient(Utils.HOST, Utils.PORT);
      db = mongo.getDB(dbName);
    }
    // if it failed not much I can so
    catch (Exception ec) {
      display("Error connectiong to server:" + ec);
      return false;
    }

    String msg = "Connection accepted at address: " + Utils.HOST + ":" + Utils.PORT;
    display(msg);

    // success we inform the caller that it worked
    return true;
  }

  /*
   * To send a message to the console or the GUI
   */
  private void display(String msg) {
    if (cg == null) {
//      _lastResultSet = msg;
      System.out.println(msg); // println in console mode
    }
    else
      cg.append(msg + "\n"); // append to the ClientGUI JTextArea
  }

  /*
   * To send a message to the server
   */
  public void query(String sql) {
    try {
      ParsedSql parsedSql = new ParsedSql(sql);
      display("\nParsed SQL : " + parsedSql);
      int maxLength = 10000;

      DBCollection collection = db.getCollection(parsedSql.getFrom());

      // Map<String, Object> map = new HashMap<>();
      display("\n---------------------------------------------------");
      display("Search query (SQL):\t " + sql);
      display("Search query (Mongo):\t db." + parsedSql.getFrom() + ".find( " + parsedSql.getWhereClause() + " )");

      if (parsedSql.getWhereClause() != null) {
        DBCursor cursor = collection.find(parsedSql.getWhereClause());
        if (parsedSql.getGroupBy() != null) {
          // Reserved
        }
        if (parsedSql.getSortClause() != null) {
          for (BasicDBObject sortQuery : parsedSql.getSortClause()) cursor.sort(sortQuery);
        }
        if (parsedSql.getSkip() > 0) {
          cursor.skip(parsedSql.getSkip());
        }
        if (parsedSql.getLimit() > 0) {
          cursor.limit(parsedSql.getLimit());
        }
        StringBuilder sb = new StringBuilder();
        while (cursor.hasNext()) {
          sb.append((sb.length() > 0 ? "\n," : "") + cursor.next());
        }
        sb.append("]");
        sb.insert(0, "[");
        String serverResponse = sb.toString();
        if (cg!=null && cg.isbPrettifyJson() && serverResponse!=null) serverResponse = prettify(serverResponse);
        
        display("\n---------------------------------------------------");
        if (serverResponse==null) {
          display("Search results: NO MATCHING ROWS FOUND");
          _lastResultSet = new LinkedHashSet<String>();
          
        } else {
          display("Search result (unparsed): "
              + (serverResponse.length() <= maxLength ? serverResponse : serverResponse.substring(0, maxLength) + "..."));
          display("\nSearch result (parsed):");
          Collection<String> resultSet = getResultSet(serverResponse, parsedSql.getFields());
          int i = 1;
//          sb = new StringBuilder("\n");
          for (String row : resultSet) {
            String s = " -- [" + i++ + "] " + parsedSql.getFields() + " : " + row;
            display(s);
//            sb.append(s);
//            sb.append("\n");
          }
//          _lastResultSet = sb.toString();
          _lastResultSet = resultSet;
        }
        display("======================================================================================================\n\n");

      } else {
        display("\nError in mongo script syntacsis: Where clause is absent");
      }

    } catch (IOException e) {
      display("Exception parsing response from the server: " + e);
    }
  }

  private String prettify(String jsonStr) throws JsonParseException, JsonMappingException, IOException {
    Object json = _mapper.readValue(jsonStr, Object.class);
    String jsonPretty = _mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json);
    return jsonPretty;
  }

  /*
   * When something goes wrong, close the client
   */
  public void disconnect() {
    try {
      if (mongo != null)
        mongo.close();
      display("\nMongoClient closed connection");
    } catch (Exception e) {
    } // not much else I can do

    // inform the GUI
    if (cg != null)
      cg.connectionFailed();
  }

  private Collection<String> getResultSet(String json, String projections) throws JsonProcessingException, IOException {
    JsonNode tree = _mapper.readTree(json);
    Collection<String> list = new LinkedHashSet<String>((int) (Math.round(tree.size() / 0.75) + 1));
    int i = 0;
    for (JsonNode node : tree) {
      String row = getResultRow(node, projections);
      list.add(row);
    }
    return list;
  }

  private String getResultRow(JsonNode node, String projections) {
    if ("*".equals(projections))
      return node.toString();
    String[] arr = projections.split("\\s*\\.\\s*"); // e.g. 'address.zipcode'
    int i = 0;
    // First part of projections, e.g. 'address'
    String projectionsPart = arr[i++];
    node = node.path(projectionsPart);
    if (node.isArray())
      Utils.error("Projections for arrays are not supported so far");

    // Sub-projections, e.g. 'zipcode'
    while (i < arr.length) {
      projectionsPart = arr[i++];
      if ("*".equals(projectionsPart))
        return node.toString();
      node = node.path(projectionsPart);
    }
    if (node.isArray())
      Utils.error("Projections for arrays are not supported in this version of application");
    return node.toString();
  }

  public Collection<String> getLastResultSet() {
    return _lastResultSet;
  }

  /*
   * To start the Client in console mode use one of the following command > java Client.
   * In console mode, if an error occurs the program simply stops. When a GUI is used,
   * the GUI is informed of the disconnection
   */
  public static void main(String[] args) {

    String dbName = null;
    // depending of the number of arguments provided we fall through
    if (args.length == 1) {
      dbName = "test";
    } else {
      System.out.println("Usage: \n> java Client");
      System.exit(0);
    }
    // create the Client object
    Client client = new Client();
    // test if we can start the connection to the Server
    // if it failed nothing we can do
    if (!client.connect(dbName))
      return;

    // wait for messages from user
    Scanner scan = new Scanner(System.in);
    // loop forever for message from the user console
    while (true) {
      System.out.print("> ");
      // read message from user
      String msg = scan.nextLine();
      // Exit if message is 'Exit'
      if (msg.equalsIgnoreCase("EXIT")) {
        client.disconnect();
        System.exit(0);
        break;

      } else { // default
        client.query(msg);
      }
    }
    // done disconnect
    client.disconnect();
  }

}
