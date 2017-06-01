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
  
  private String fields;
  private String from;
  private BasicDBObject whereClause;
  private String groupBy;
  private List<BasicDBObject> orderByClause;
  private int skip = -1;
  private int limit = -1;
 
  private static String regexp = "(?i:\\s*(SELECT\\s+((\\*)|((\\w+)(\\.(\\*|\\w+))*)))\\s+(FROM\\s+(\\w+))\\s+(WHERE\\s+(\\w+.*?))(\\s+(GROUP BY)\\s+(\\w+.*?))?(\\s+(ORDER BY)\\s+(\\w+.*?)(\\s+(ASC|DESC))?)?(\\s+(SKIP)\\s+(\\w+.*?))?(\\s+(LIMIT)\\s+(\\w+.*?))?\\s*)";
  private static String whereRegexp = "(?i:(\\w+.*))";

  public ParsedSql(String sql) {
    Utils.info("\n----------------- New SQL request ------------------");
    Utils.info("Raw SQL : " + sql);
    validateAndParse(sql);
//    Utils.info("Parsed SQL : " + this);
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
      Utils.info("SQL general validation : OK");
    }
//    int i = 1;
//    while (i<=matcher.groupCount()) {
//      Utils.info("group(" + i + "): " + matcher.group(i++));
//    }
    fields = matcher.group(2);
    from = matcher.group(9);
    String whereSql = matcher.group(11);
    
    // ------------------------------------------------------------------------
    //               Validation and parsing of the "WHERE part"
    // ------------------------------------------------------------------------
    if (!whereSql.matches(whereRegexp)) {
      Utils.error("The WHERE clause in user's sql \n" + whereSql + " \ndoesn't match the whereRegexp \n" + whereRegexp);
    } else {
      Utils.info("SQL WHERE clause validation : OK");
    }

    // Translating WHERE part to a Mongo query
    String[] whereSqlOR = whereSql.split("\\s+(?i:OR)\\s+");
    BasicDBObject querySub = null;
    BasicDBList criteria = new BasicDBList();
    for (String s : whereSqlOR) {
      
      String[] whereSqlAND = s.split("\\s+(?i:AND)\\s+");
      Collection<String[]> setSub = new HashSet<String[]>();
      for (String whereSubQuery : whereSqlAND) {
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
    if (whereSqlOR.length>1) 
      whereClause = buildQuery(null, "OR", criteria);
    else 
      whereClause = querySub;

    // ------------------------------------------------------------------------
    //        Further clauses, i.e. GROUP BY|ORDER BY|SKIP|LIMIT
    // ------------------------------------------------------------------------
    if (matcher.group(14)!=null) {
      groupBy = matcher.group(14); // Reserved
    }
    if (matcher.group(17)!=null) {
      String arg1 = matcher.group(17);
      String arg2 = matcher.group(19);
      if (arg2==null) arg2="ASC";
      orderByClause = new ArrayList<BasicDBObject>();
      orderByClause.add( new BasicDBObject(arg1, (arg2.equalsIgnoreCase("ASC") ? 1 : -1)) );
    }
    if (matcher.group(22)!=null) {
      skip = Integer.parseInt(matcher.group(22));
    }
    if (matcher.group(25)!=null) {
      limit = Integer.parseInt(matcher.group(25));
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
    return fields;
  }

  public String getFrom() {
    return from;
  }

  public BasicDBObject getWhereClause() {
    return whereClause;
  }

  public String getGroupBy() {
    return groupBy;
  }

  public int getSkip() {
    return skip;
  }

  public int getLimit() {
    return limit;
  }

  public List<BasicDBObject> getSortClause() {
    return orderByClause;
  }

  @Override
  public String toString() {
    return "ParsedSql [ fields=" + fields + ", \n\tfrom=" + from + ", \n\twhereClause=" + whereClause + ", \n\tgroupBy="
        + groupBy + ", \n\torderByClause=" + orderByClause + ", \n\tskip=" + skip + ", \n\tlimit=" + limit + " ]";
  }

}
