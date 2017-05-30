package org.mongo.client;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;

public class ParsedSql {
  
  private String _fields;
  private String _from;
  private BasicDBObject _whereClause;
  private String _groupBy;
  private List<BasicDBObject> _orderByClause;
  private int _skip = -1;
  private int _limit = -1;
 
//  private static String regexp = "(?i:(SELECT ((\\*)|((\\w+)(\\.(\\*|\\w+))*))) (FROM (\\w+)) (WHERE (\\w+.*)))";
  private static String regexp = "(?i:\\s*(SELECT\\s+((\\*)|((\\w+)(\\.(\\*|\\w+))*)))\\s+(FROM\\s+(\\w+))\\s+(WHERE\\s+(\\w+.*?))(\\s+(GROUP BY)\\s+(\\w+.*?))?(\\s+(ORDER BY)\\s+(\\w+.*?)(\\s+(ASC|DESC))?)?(\\s+(SKIP)\\s+(\\w+.*?))?(\\s+(LIMIT)\\s+(\\w+.*?))?\\s*)";
  private static String whereRegexp = "(?i:(\\w+.*))";

  public ParsedSql(String sql) {
    validateAndParse(sql);
    System.out.println("\n>>: " + this);
  }

  private void validateAndParse(String sql) {
    // ------------------------------------------------------------------------
    //                    General validation and parsing
    // ------------------------------------------------------------------------
    Pattern pattern = Pattern.compile(regexp);
    Matcher matcher = pattern.matcher(sql);
    if (!matcher.matches()) {
      Utils.error("User sql \n" + sql + " \ndoesn't match the regexp \n" + regexp);
    } else {
      System.out.println("General validation : OK");
    }
    int i = 1;
    while (i<=matcher.groupCount()) {
      System.out.println("group(" + i + "): " + matcher.group(i++));
    }
    _fields = matcher.group(2);
    _from = matcher.group(9);
    String whereSql = matcher.group(11);
    
    // ------------------------------------------------------------------------
    //               Validation and parsing of the "WHERE part"
    // ------------------------------------------------------------------------
    if (!whereSql.matches(whereRegexp)) {
      Utils.error("The WHERE clause in user's sql \n" + whereSql + " \ndoesn't match the whereRegexp \n" + whereRegexp);
    } else {
      System.out.println("WHERE validation : OK");
    }

    // Translating WHERE part to a Mongo query
    String[] whereSql_OR = whereSql.split("\\s+(?i:OR)\\s+");
    BasicDBObject querySub = null;
    BasicDBList criteria = new BasicDBList();
    for (String s : whereSql_OR) {
      
      String[] whereSql_AND = s.split("\\s+(?i:AND)\\s+");
      Collection<String[]> setSub = new HashSet<String[]>();
      for (String whereSubQuery : whereSql_AND) {
        String whereRegexp2 = "(\\w+.*?)\\s*(=|<>|>|>=|<|<=)\\s*(\\w+.*?)";
        pattern = Pattern.compile(whereRegexp2);
        Matcher matcher2 = pattern.matcher(whereSubQuery);
        if (!matcher2.matches()) 
          Utils.error("Error in the WHERE clause: \n" + whereSubQuery + " \ndoesn't match the whereRegexp \n" + whereRegexp2);
        setSub.add(new String[]{matcher2.group(1), toHtmlForm(matcher2.group(2)), matcher2.group(3)});
      }
      querySub = buildQuery(setSub, "AND", null);
      criteria.add(querySub);
    }
    if (whereSql_OR.length>1) 
      _whereClause = buildQuery(null, "OR", criteria);
    else 
      _whereClause = querySub;

    // ------------------------------------------------------------------------
    //        Further clauses, i.e. GROUP BY|ORDER BY|SKIP|LIMIT
    // ------------------------------------------------------------------------
    if (matcher.group(14)!=null) {
      _groupBy = matcher.group(14); // Reserved
    }
    if (matcher.group(17)!=null) {
      String arg1 = matcher.group(17);
      String arg2 = matcher.group(19);
      if (arg2==null) arg2="ASC";
      _orderByClause = new ArrayList<BasicDBObject>();
      _orderByClause.add( new BasicDBObject(arg1, (arg2.equalsIgnoreCase("ASC") ? 1 : -1)) );
    }
    if (matcher.group(22)!=null) {
      _skip = Integer.parseInt(matcher.group(22));
    }
    if (matcher.group(25)!=null) {
      _limit = Integer.parseInt(matcher.group(25));
    }
  }

  private static BasicDBObject buildQuery(Collection<String[]> set, String superOperator, BasicDBList criteria) {
    if (criteria==null) criteria = new BasicDBList();
    if (set==null) set = new HashSet<String[]>();
    if (superOperator.equalsIgnoreCase("AND")) {
      for (String[] arr : set) {
        String operator = arr[1];
        if (!operator.equals("=")) 
          criteria.add(new BasicDBObject(arr[0], new BasicDBObject(operator, arr[2])));
        else 
          criteria.add(new BasicDBObject(arr[0], arr[2]));
      }
      
    } else if (superOperator.equalsIgnoreCase("OR")) {
      if (set.size()>1) ;
      for (String[] arr : set) {
        String operator = arr[1];
        if (!operator.equals("=")) 
          criteria.add(new BasicDBObject(arr[0], new BasicDBObject(operator, arr[2])));
        else 
          criteria.add(new BasicDBObject(arr[0], arr[2]));
      }
      
    } else {
      Utils.error("Unrecognized operator: " + superOperator);
    }
    return new BasicDBObject("$" + superOperator.toLowerCase(), criteria);
  }

  private String toHtmlForm(String s) {
    switch (s) {
    case "=":
      return s;
    case "<>":
      return "$ne";
    case ">":
      return "$gt";
    case ">=":
      return "$gte";
    case "<":
      return "$lt";
    case "<=":
      return "$lte";
    default:
      Utils.error("Unrecognized operator: " + s);
      return null;
    }
  }

  public String getFields() {
    return _fields;
  }

  public String getFrom() {
    return _from;
  }

  public BasicDBObject getWhereClause() {
    return _whereClause;
  }

  public String getGroupBy() {
    return _groupBy;
  }

  public int getSkip() {
    return _skip;
  }

  public int getLimit() {
    return _limit;
  }

  public List<BasicDBObject> getSortClause() {
    return _orderByClause;
  }

  @Override
  public String toString() {
    return "ParsedSql [ _fields=" + _fields + ", \n\t_from=" + _from + ", \n\t_whereClause=" + _whereClause + ", \n\t_groupBy="
        + _groupBy + ", \n\t_orderByClause=" + _orderByClause + ", \n\t_skip=" + _skip + ", \n\t_limit=" + _limit + " ]";
  }

  
  public static void main(String[] args) {
//    String sql = "SELECT address.zipcode FROM restaurants WHERE address.zipcode=11691 AND address.street=Seagirt Avenue OR restaurant_id=40359480 OR address.street=Beach 25 Street";
//    String sql = "SELECT address.zipcode FROM restaurants WHERE address.zipcode=11691 AND address.street=Seagirt Avenue OR restaurant_id=40359480 OR address.street=Beach 25 Street ORDER BY restaurant_id DESC";
//    String sql = "SELECT address.zipcode FROM restaurants WHERE address.zipcode=11691 AND address.street=Seagirt Avenue OR restaurant_id=40359480 OR address.street=Beach 25 Street GROUP BY address.street ORDER BY restaurant_id DESC SKIP 300 LIMIT 100";
//    String sql = "SELECT address.zipcode FROM restaurants WHERE address.zipcode=11691 AND address.street=Seagirt Avenue OR restaurant_id=40359480 OR address.street=Beach 25 Street ORDER BY restaurant_id DESC LIMIT 100";
    String sql = "SELECT restaurant_id FROM restaurants WHERE address.zipcode=11691 AND address.street=Seagirt Avenue OR restaurant_id>=40359480 AND restaurant_id<40359485 Order by address.street";
//    String sql = "SELECT restaurant_id FROM restaurants WHERE address.zipcode=11691 AND address.street=Seagirt Avenue Order by restaurant_id desc";
    new ParsedSql(sql);
  }
  
}
