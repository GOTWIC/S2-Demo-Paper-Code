package src;

import exceptions.QueryParseExceptions;
import src.convertCSV;
import utility.Helper;
import src._00_Database_Table_Creator.Database_Table_Creator;
import src._01_oneColumnNumberSearch.client.Client01;
import src._02_oneColumnStringSearch.client.Client02;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.regex.Pattern;

public class QueryParser {

    // To store column names, column values, protocol as OR/AND, type as */count/sum
    private static ArrayList<String> columnNames = new ArrayList<>();
    private static ArrayList<String> columnValues = new ArrayList<>();
    private static String protocol = "single";
    private static String type = "";
    private static String predicate = "";
    private static String aggregateAttribute;


    // to store metadata contents
    private static HashMap<String, String> columnList = new HashMap<>();
    private static String tableName;
    private static String databaseName;
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

        if (!query.startsWith("table "))
            throw new QueryParseExceptions("Invalid query syntax: Missing keyword 'table'.");

        query = query.substring("table".length()).stripLeading();

        if (!query.startsWith("from"))
            throw new QueryParseExceptions("Invalid query syntax: Missing keyword 'from'.");

        query = query.substring("from".length()).stripLeading();

        //if (!query.startsWith(tableName) && !query.startsWith(databaseName + "." + tableName))
        //    throw new QueryParseExceptions("Invalid query syntax: Incorrect table name.");

        // by this point the query is just database_name.table_name and number of rows

        String[] queryParts = query.split(" ");

        for (Map.Entry<String, String> entry : columnList.entrySet()) {
            String colName = entry.getKey();
            columns_to_split += colName + ",";
        }

        String db = queryParts[0].substring(0, query.indexOf("."));
        String table = queryParts[0].substring(query.indexOf(".") + 1);
        String rows = queryParts[1];   

        String[] arguments = new String[]{rows, db, table, columns_to_split};
        Database_Table_Creator.main(arguments);

        // Create server tables 
        for (int i = 0; i < 4; i++) {
            String createTableQuery = 
            """
                CREATE TABLE  """ +
                    " " + db + "." + table +
                """
                    _servertable """ + String.valueOf(i+1) + """
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
 + db + "." + table +
            """ 
_servertable """ + String.valueOf(i+1) + " " + 
            """    
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\\n'
            """;
            //IGNORE 1 ROWS;

            // Append additive columns for table definition
            for (Map.Entry<String, String> entry : columnList.entrySet()) {
                String colName = entry.getKey();
                String colType = entry.getValue();

                // basic check to prevent non-specific columns from being added
                if(!columns_to_split.contains(colName))
                    continue;
                
                // converts non-int columns to varchars
                if(!colType.contains("int"))
                    colType = "varchar(255)";
                
                if(colType.contains("char") || colType.contains("int"))
                    createTableQuery += "\n" + "A_" + colName + " " + colType + ", ";
            }

            // Append multiplicative columns for table definition
            for (Map.Entry<String, String> entry : columnList.entrySet()) {
                String colName = entry.getKey();
                String colType = entry.getValue();
 
                // basic check to prevent non-specific columns from being added
                if(!columns_to_split.contains(colName))
                    continue;

                // converts non-int columns to varchars
                if(!colType.contains("int"))
                    colType = "varchar(255)";

                if(colType.contains("char") || colType.contains("int"))
                    createTableQuery += "\n" + "M_" + colName + " " + colType + ", ";
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

            //System.out.println(createTableQuery);

            try {
                    stmt = con.createStatement();  
                    stmt.executeUpdate("SET GLOBAL local_infile=1;");
                    stmt.executeUpdate(createTableQuery);
                    stmt.executeUpdate(loadTableQuery);
            } catch (Exception e) {
                    e.printStackTrace();
            }     
        }
    }

    private static String basicChecks(String query) throws QueryParseExceptions {
        if (!query.startsWith("select "))
            throw new QueryParseExceptions("Invalid query syntax: Missing keyword 'select'.");
        //if (!query.endsWith(";"))
        //    throw new QueryParseExceptions("Invalid query syntax: Missing ';'.");
        return query.substring("select".length()).stripLeading();
    }

    private static String extractprotocolType(String query) throws QueryParseExceptions {
        if (query.isEmpty())
            throw new QueryParseExceptions("Invalid query syntax: Missing Type.");
        if (query.startsWith("* "))
            type = "*";
        if (query.startsWith("count(*) "))
            type = "count(*)";
        if(query.startsWith("sum(")){
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
        if (!query.startsWith("from "))
            throw new QueryParseExceptions("Invalid query syntax: Missing keyword 'from'.");
        query = query.substring("from".length()).stripLeading();
        if (!query.startsWith(tableName) && !query.startsWith(databaseName + "." + tableName))
            throw new QueryParseExceptions("Invalid query syntax: Incorrect table name.");
        if(query.startsWith(databaseName + "." + tableName))
            query = query.substring(databaseName.length() + 1 + tableName.length()).stripLeading();
        if(query.startsWith(tableName))
            query = query.substring(tableName.length()).stripLeading();
        if (!query.startsWith("where "))
            throw new QueryParseExceptions("Invalid query syntax: Missing keyword 'where'.");

        return query.substring("where".length()).stripLeading();
    }

    private static void extractPredicates(String query) throws QueryParseExceptions {
        String attribute, value;
        int endIndex;

        if (query.isEmpty() && columnValues.size() > 0)
            return;

        if (!query.contains("=") && columnValues.size() == 0)
            throw new QueryParseExceptions("Unsupported protocol: only '=' is supported");

        // Restore original query to preserve case
        query = origQuery.substring(origQuery.indexOf("where") + "where".length()).stripLeading();

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
    }

    private static String preprocess(String data) throws QueryParseExceptions {
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
                    String temp = "037";
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


    public static void runNormalQuery(String query) throws IOException{
        executeQuery(origQuery);
    }

    public static void executeQuery(String query){
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
                stmt.executeUpdate(query);
                writeResult("Success");
        } catch (Exception e) {
                e.printStackTrace();
        } 
    }

    public static void writeResult(String res) throws IOException {
        FileWriter writer = new FileWriter("result/prompt.txt");
        writer.append(res);
        writer.flush();
        writer.close();
    }

    
    
    public static void main(String[] args) throws QueryParseExceptions, IOException {

        columnList = Helper.getColumnList();

        Helper.getMetadata();
        tableName = Helper.getTableName();
        databaseName = Helper.getDatabaseName();
        noOfColumns = Helper.getNoOfColumns(); 


        String query = args[0];
        origQuery = query;
        query = query.strip().toLowerCase();

        if(!query.contains("enc")){
            runNormalQuery(query);
            return;
        }

        query = query.replace("enc ", "");

        int queryType = getQueryType(query); 
        
        if(queryType == 0){
            createTable(query);
            writeResult("Table created successfully");
        }

        else if (queryType == 1){
            // This is pretty useless, but I'm keeping it here for now
            query = basicChecks(query);
            query = extractprotocolType(query);
            query = validateTableName(query);
            extractPredicates(query);

            /*
            System.out.println(protocol);
            System.out.println(type);
            for (int i = 0; i < columnNames.size(); i++) {
                predicate += columnNames.get(i) + "," + columnValues.get(i) + ",";
            }
            System.out.println(predicate);
            */

            String enc_query_result = "";

            if (type == "*" || type == "count(*)"){
                String[] arguments = new String[]{columnNames.get(0) + "," + columnValues.get(0)};
                try {
                    List<Integer> res = Client01.main(arguments);
                    if(type == "count(*)"){
                        writeResult(String.valueOf(res.size()));
                    }
                    else{
                        for(int i = 0; i < res.size(); i++){
                            enc_query_result += res.get(i) + ",";
                        }
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            if (type == "*"){
                String[] arguments = new String[]{enc_query_result};

            }
        }
    }
}
