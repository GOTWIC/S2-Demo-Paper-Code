package src;

import exceptions.QueryParseExceptions;
import src.convertCSV;
import utility.Helper;
import src._00_Database_Table_Creator.Database_Table_Creator;
import src._01_oneColumnNumberSearch.client.Client01;
import src._02_oneColumnStringSearch.client.Client02;
import src._03_AND_Search.client.Client03;
import src._04_OR_Search.client.Client04;
import src._05_Multiplicative_Row_Fetch.client.Client05;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class QueryParser {

    // To store column names, column values, protocol as OR/AND, type as */count/sum
    private static ArrayList<String> columnNames = new ArrayList<>();
    private static ArrayList<String> columnValues = new ArrayList<>();
    private static String protocol = "single";
    private static String type = "";
    private static String predicate = "";
    private static String aggregateAttribute;


    // to store metadata contents
    private static HashMap<String, Integer> columnList = new HashMap<>();
    private static String tableName;
    private static String databaseName;
    private static int numRows;
    private static int noOfColumns;

    // to store query and other information
    private static String origQuery;
    private static String columns_to_split = "";


    private static int getQueryType(String query) {
        if(query.startsWith("create"))
            return 0;
        else if(query.startsWith("select"))
            return 1;
        else
            return -1;
    }

    private static void createTable(String query) throws QueryParseExceptions, IOException{
        query = query.substring("create".length()).stripLeading();

        if (!query.toLowerCase().startsWith("table "))
            throw new QueryParseExceptions("Invalid query syntax: Missing keyword 'table'.");

        query = query.substring("table".length()).stripLeading();

        if (!query.toLowerCase().startsWith("from"))
            throw new QueryParseExceptions("Invalid query syntax: Missing keyword 'from'.");

        query = query.substring("from".length()).stripLeading();

        if (!query.toLowerCase().startsWith(tableName.toLowerCase()) && !query.toLowerCase().startsWith(databaseName.toLowerCase() + "." + tableName.toLowerCase()))
            throw new QueryParseExceptions("Invalid query syntax: Incorrect database or table name. This is what we have: " + query);


        for (Map.Entry<String, Integer> entry : columnList.entrySet()) {
            String colName = entry.getKey();
            columns_to_split += colName + ",";
        }


        String[] arguments = new String[]{String.valueOf(numRows), databaseName, tableName,columns_to_split};
        Database_Table_Creator.main(arguments);

        // Create server tables 
        for (int i = 0; i < 4; i++) {
            String createTableQuery = 
            """
                CREATE TABLE  """ +
                    " " + databaseName + "." + tableName +
                """
                    _SERVERTABLE """ + String.valueOf(i+1) + """
                (
            """;

            String loadTableQuery = 
            """
LOAD DATA LOCAL INFILE 
            """ 
            + "'C:/Users/shoum/Documents/VLDBPaperDemo/S2-VLDB-2023-main/data/shares/ServerTable" + String.valueOf(i+1) + ".csv' " + 
            """
INTO TABLE
            """
 + databaseName + "." + tableName + 
            """ 
_SERVERTABLE """ + String.valueOf(i+1) + " " + 
            """    
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\\n'
            """;
            //IGNORE 1 ROWS;
            
            // Append additive columns for table definition
            for (Map.Entry<String, Integer> entry : columnList.entrySet()) {
                String colName = entry.getKey();
                int colType = entry.getValue();
                String colTypeString = "int";

                // basic check to prevent non-specific columns from being added
                if(!columns_to_split.contains(colName))
                    continue;
                
                // converts non-int columns to varchars
                if(colType != 0)
                    colTypeString = "varchar(255)";
                
                createTableQuery += "\n" + "A_" + colName + " " + colTypeString + ", ";
            }

            // Append multiplicative columns for table definition
            for (Map.Entry<String, Integer> entry : columnList.entrySet()) {
                String colName = entry.getKey();
                int colType = entry.getValue();
                String colTypeString = "int";
 
                // basic check to prevent non-specific columns from being added
                if(!columns_to_split.contains(colName))
                    continue;

                /// converts non-int columns to varchars
                if(colType != 0)
                    colTypeString = "varchar(255)";
                
                createTableQuery += "\n" + "M_" + colName + " " + colTypeString + ", ";
            }

            // Append rowID column
            createTableQuery += "\n" + "rowID   int \n);";

            Connection con = null;
            try {
                    con = Helper.getConnection();
            } catch (SQLException e) {
                    e.printStackTrace();
            }
            Statement stmt;

            try {
                    stmt = con.createStatement();  
                    stmt.executeUpdate("SET GLOBAL local_infile=1;");
                    //stmt.executeUpdate("drop table tpch.test_servertable1, tpch.test_servertable2, tpch.test_servertable3, tpch.test_servertable4");
                    stmt.executeUpdate(createTableQuery);
                    stmt.executeUpdate(loadTableQuery);
            } catch (Exception e) {
                    e.printStackTrace();
            }                            
        }
    }

    private static String basicChecks(String query) throws QueryParseExceptions {
        if (!query.toLowerCase().startsWith("select "))
            throw new QueryParseExceptions("Invalid query syntax: Missing keyword 'select'.");
        if (!query.toLowerCase().endsWith(";"))
            throw new QueryParseExceptions("Invalid query syntax: Missing ';'.");
        return query.substring("select".length()).stripLeading();
    }

    private static String extractprotocolType(String query) throws QueryParseExceptions {
        if (query.isEmpty())
            throw new QueryParseExceptions("Invalid query syntax: Missing Type.");
        if (query.startsWith("* "))
            type = "*";
        if (query.toLowerCase().startsWith("count(*) "))
            type = "count(*)";
        if(query.toLowerCase().startsWith("sum(")){
            int index= query.indexOf(")");
            if(index==-1)
                throw new QueryParseExceptions("Invalid query syntax: Missing closing')' braces.");
            aggregateAttribute=query.substring("sum(".length(),index);
            if(!columnList.containsKey(aggregateAttribute) && !columnList.get(aggregateAttribute).equals("number") )
                throw new QueryParseExceptions("Invalid query syntax: Aggregate attribute not present or incorrect data type.");
            type="sum("+aggregateAttribute+")";
        }

        // TODO: Add code for sum function.
        if (type == "")
            throw new QueryParseExceptions("Invalid query syntax: Missing Type (count, sum or *).");
        return query.substring(type.length()).stripLeading();
    }

    private static String validateTableName(String query) throws QueryParseExceptions {
        if (!query.toLowerCase().startsWith("from "))
            throw new QueryParseExceptions("Invalid query syntax: Missing keyword 'from'.");
        query = query.substring("from".length()).stripLeading();
        if (!query.toLowerCase().startsWith(tableName.toLowerCase()) && !query.toLowerCase().startsWith(databaseName.toLowerCase() + "." + tableName.toLowerCase()))
            throw new QueryParseExceptions("Invalid query syntax: Incorrect table name.");
        if(query.toLowerCase().startsWith(databaseName.toLowerCase() + "." + tableName.toLowerCase()))
            query = query.substring(databaseName.length() + 1 + tableName.length()).stripLeading();
        if(query.toLowerCase().startsWith(tableName.toLowerCase()))
            query = query.substring(tableName.length()).stripLeading();
        if (query.toLowerCase().startsWith("where "))
            query = query.substring("where".length()).stripLeading();

        return query;
    }

    private static void extractPredicates(String query) throws QueryParseExceptions {
        String attribute, value;
        int endIndex;

        // Not sure what this is for
        //if (query.isEmpty() && columnValues.size() > 0)
        //    return;

        if (query.equals("")) {
            protocol = "NULL";
            return;
        }

        //if (!query.contains("=") && columnValues.size() == 0)
        //    throw new QueryParseExceptions("Unsupported protocol: only '=' is supported");

        // Restore original query to preserve case
        query = origQuery.substring(origQuery.indexOf("where") + "where".length()).stripLeading();

        if(query.toLowerCase().contains(" and "))
            protocol = "and";
        else if(query.toLowerCase().contains(" or "))
            protocol = "or";
        else
            protocol = "single";


        query = query.replaceAll(" and ", ",").replaceAll(" or ", ",").replaceAll(" AND ", ",").replaceAll(" OR ", ",").replaceAll(";", "");

        String[] predicates = query.split(",");
        HashMap<String, Integer> columnListWithNumTypes = Helper.getColumnListWithNumType();
        for (String predicate : predicates) {
            String[] predicateSplit = predicate.split("=");
            String colName = predicateSplit[0].replaceAll(" ", "");
            String colValue = predicateSplit[1].stripLeading().stripTrailing();
            if(!columnList.containsKey(colName.toLowerCase()))
                throw new QueryParseExceptions("Invalid query syntax: Column " + colName + " not found.");
            columnNames.add(colName.toUpperCase());
            // Remove quotations
            if(colValue.startsWith("'") && colValue.endsWith("'"))
                colValue = colValue.substring(1, colValue.length() - 1);

            // we have a special case where if the value is from a column with type 3, and the value doesn't have a decimal, then we need to add a .0 to the end of the value
            if(columnListWithNumTypes.get(colName.toLowerCase()) == 3 && (Double.parseDouble(colValue) == (int) Double.parseDouble(colValue))){
                colValue = String.valueOf((int) Double.parseDouble(colValue)) + ".0";
            }
            if(columnList.get(colName.toLowerCase()) != 0)
                colValue = preprocess(colValue);
            columnValues.add(colValue);
        }

        /*
        attribute = query.substring(0, query.indexOf("=")).strip();
        if (!columnList.containsKey(attribute))
            throw new QueryParseExceptions("Column name not found.");
        columnNames.add(attribute);
        query = query.substring(query.indexOf("=") + 1).stripLeading();
        endIndex = getColumnValue(query);
        value = query.substring(0, endIndex);
        value = preprocess(value);
        columnValues.add(value);
        query = query.substring(endIndex).stripLeading();
        if (query.startsWith(";")) {
            if (query.length() > 1)
                throw new QueryParseExceptions("Invalid query syntax: Content after ';'");
            return;
        }

        if (query.startsWith("and ")) {
            if (protocol.equals("single"))
                protocol = "and";
            else if (!protocol.equals("and"))
                throw new QueryParseExceptions("Unsupported protocol: only all 'and' or all 'or' protocols supported");
        } else if (query.startsWith("or ")) {
            if (protocol.equals("single"))
                protocol = "or";
            else if (!protocol.equals("or"))
                throw new QueryParseExceptions("Unsupported protocol: only all 'and' or all 'or' protocols supported");
        } else
            throw new QueryParseExceptions("Invalid query syntax: conjunctive or disjunctive required");

        extractPredicates(query.substring(protocol.length()).stripLeading());

        */
    }

    private static String preprocess(String data) throws QueryParseExceptions {

        //System.out.println(data);
        data = data.replaceAll("'", "");
        // CHANGE STRING REGEX TO SUPPORT ALL CHARACTERS
        String stringRegex = "[\\x00-\\x7F.]+";
        String dateRegex = "([0-9]{2})/([0-9]{2})/([0-9]{4})";
        String numberRegex = "[0-9]+";

        String value = "";

        if (Pattern.compile(stringRegex).matcher(data).matches()) {
            for (int i = 0; i < data.length(); i++) {
                char ch = data.charAt(i);
                if (ch == ' ') {
                    String temp = "032";
                    value = value + temp;
                } 
                else {
                    String temp = String.valueOf((int)ch);
                    String zerobuffer = "";
                    for(int j = temp.length(); j < 3; j++ ){ zerobuffer += "0"; }
                    value = value + zerobuffer + temp;
                }
            }
            int maxlength = convertCSV.getMaxLength();
            while (value.length() / 3 < maxlength) {
                value = value + "999";
            }
        } else if (Pattern.compile(dateRegex).matcher(data).matches()) {
            value = data.replace("/", "");
        } else if (Pattern.compile(numberRegex).matcher(data).matches()) {
            value = data;
        } else {
            throw new QueryParseExceptions("Invalid column value.");
        }

        //System.out.println(value);

        return value;
    }

    private static int getColumnValue(String query) throws QueryParseExceptions {
        int endIndex;
        if (query.startsWith("'")) {
            endIndex = query.indexOf("'", 1) + 1;
            if (endIndex == -1)
                throw new QueryParseExceptions("Invalid query syntax: Missing closing '.");
        } else {
            endIndex = query.indexOf(" ");
            if (endIndex == -1)
                endIndex = query.indexOf(";");
        }
        return endIndex;
    }

    private static void execute_query(String query,boolean info){
        Connection con = null;
        Statement stmt;
        try {
                con = Helper.getConnection();
        } catch (SQLException e) {
                e.printStackTrace();
        }
        try {
                stmt = con.createStatement();  
                stmt.executeUpdate("SET GLOBAL local_infile=1;");
                if(info){
                    ResultSet rs = stmt.executeQuery(query);
                    while(rs.next()){
                        writeToNumRows(rs.getString(1));
                    }
                }
                else{
                    stmt.executeUpdate(query);
                }

        } catch (Exception e) {
                e.printStackTrace();
        }     
        System.out.println("Success");
    }

    private static void writeResult(){
        try {
            FileWriter writer = new FileWriter("result/prompt.txt");
            writer.append("Done");
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    } 

    private static void writeToNumRows(String numrows){
        try {
            FileWriter writer = new FileWriter("result/numrows.txt");
            writer.append(numrows);
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    } 
    
    private static String removeDuplicates(String input) {
        Set<String> uniqueNumbers = new LinkedHashSet<>(Arrays.asList(input.split(",")));
        return uniqueNumbers.stream().collect(Collectors.joining(","));
    }

    private static void printFirst(String res){
        String[] resSplit = res.split("\\r?\\n");
        int numLines = Math.min(20, resSplit.length - 1);
        for(int i = 0; i < numLines + 1; i++){
            System.out.println(resSplit[i]);
        }
    }
    
    public static void main(String[] args) throws QueryParseExceptions, IOException, InterruptedException {

        String query = args[0];
        origQuery = query;
        query = query.strip().toLowerCase();

        if(query.contains("getdbtbinfo")){
            String new_query = "select count(*) from " + args[1] + "." + args[2] + ";";
            execute_query(new_query, true);
            return;
        }

        if(!query.contains("enc")){
            execute_query(query,false);
            writeResult();
            return;
        }

        tableName = Helper.getTableName();
        databaseName = Helper.getDatabaseName();
        noOfColumns = Helper.getNoOfColumns();
        numRows = Helper.getNumRows();
        columnList = Helper.getColumnList();  

        query = query.replace("enc ", "");

        int queryType = getQueryType(query);   

        if(queryType == 0){
            createTable(query);
            System.out.println("Table Created Sucessfully!");
            writeResult();
            return;
        }

        if(query.equals("select * from " + databaseName + "." + tableName + ";")){
            protocol = "NULL";
            type = "*";
        }
        else if(query.equals("select count(*) from " + databaseName + "." + tableName + ";")){
            protocol = "NULL";
            type = "count(*)";
        }
        else{
            query = basicChecks(query);
            query = extractprotocolType(query); // Might have some case issues, check later
            query = validateTableName(query);
            extractPredicates(query);
            String debug = "";
            debug += columnNames.toString() + "\n";
            debug += columnValues.toString() + "\n";
            debug += protocol + "\n";
            debug += type + "\n";
            //System.out.println(debug);
        }

        System.gc();


        String resultRows = "";

        // EXECUTE QUERY PROTOCOLS
        // All queries have an associated protocol. The result of the protocol is stored in resultRows
        // Final result is executed based on resultRows

        if(protocol.equals("single")){
            String[] clientArgs = new String[]{columnNames.get(0) + "," + columnValues.get(0)};
            if(columnList.get(columnNames.get(0).toLowerCase()) == 0){
                try {
                    String res = Client01.main(clientArgs);
                    res = res.replaceAll("\\[", "").replaceAll("\\]", "").replaceAll(" ", "");
                    resultRows = res;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            else{
                try {
                    String res = Client02.main(clientArgs);
                    res = res.replaceAll("\\[", "").replaceAll("\\]", "").replaceAll(" ", "");
                    resultRows = res;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        else if(protocol.equals("and")){
            String clientData = "";
            for(int i = 0; i < columnNames.size(); i++){
                clientData += columnNames.get(i) + "," + columnValues.get(i) + ",";
            }
            String[] clientArgs = new String[]{clientData};
            try {
                String res = Client03.main(clientArgs);
                res = res.replaceAll("\\[", "").replaceAll("\\]", "").replaceAll(" ", "");
                resultRows = res;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        else if(protocol.equals("or")){

            // split into int and non-int columns

            ArrayList<String> intColumns = new ArrayList<>();
            ArrayList<String> nonIntColumns = new ArrayList<>();

            for(int i = 0; i < columnNames.size(); i++){
                if(columnList.get(columnNames.get(i).toLowerCase()) == 0)
                    intColumns.add(columnNames.get(i) + "," + columnValues.get(i) + ",");
                else
                    nonIntColumns.add(columnNames.get(i) + "," + columnValues.get(i) + ",");
            }

            for(int num_executions = 0; num_executions < (int) Math.ceil((double)intColumns.size() / 2); num_executions++){
                String clientData = "";
                int upper_limit = Math.min(intColumns.size(), num_executions * 2 + 2);
                for(int i = num_executions * 2; i < upper_limit; i++){
                    clientData += intColumns.get(i);
                }
                String[] clientArgs = new String[]{clientData};
                try {
                    Client04 client = new Client04();
                    String res = client.main(clientArgs);
                    res = res.replaceAll("\\[", "").replaceAll("\\]", "").replaceAll(" ", "");
                    resultRows += res + ",";
                    System.out.println(res);
                    client = null;
                    System.gc();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                Thread.sleep(100);
            }           
            
            for(int num_executions = 0; num_executions < (int) Math.ceil((double)nonIntColumns.size() / 2); num_executions++){
                String clientData = "";
                int upper_limit = Math.min(nonIntColumns.size(), num_executions * 2 + 2);
                for(int i = num_executions * 2; i < upper_limit; i++){
                    clientData += nonIntColumns.get(i);
                }
                String[] clientArgs = new String[]{clientData};
                try {
                    Client04 client = new Client04();
                    String res = client.main(clientArgs);
                    res = res.replaceAll("\\[", "").replaceAll("\\]", "").replaceAll(" ", "");
                    resultRows += res + ",";
                    System.out.println(res);
                    client = null;
                    System.gc();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                Thread.sleep(100);
            }    
            

        }
        else if(protocol.equals("NULL")){
            resultRows = "";
            for(int i = 0; i < numRows; i++){
                resultRows += String.valueOf(i+1) + ",";
            }
        }

        resultRows = removeDuplicates(resultRows);
        //System.out.println("Result Rows: " + resultRows);

        // EXECUTE QUERY TYPE
        // Calculates *, count(*), and sum() based on the resultRows from the protocol

        if(resultRows.equals("")){
                System.out.println("No rows match the query");
                writeResult();
                return;
        }

        if(type.equals("count(*)")){
            // count number of values in resultRows
            String[] resultRowsSplit = resultRows.split(",");
            System.out.println(resultRowsSplit.length + " rows match the query");
        }

        else if(type.equals("*")){
            String[] clientArgs = new String[]{resultRows, "NONE/NULL/??"};
            try {
                String res = Client05.main(clientArgs);
                printFirst(res);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        else if (type.contains("sum") || type.contains("SUM")){
            String sum_col = "";

            sum_col = type.substring("sum(".length(), type.length() - 1);

            String[] clientArgs = new String[]{resultRows, sum_col};
            try {
                String res = Client05.main(clientArgs);
                System.out.println(res);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        writeResult();
    }
}
