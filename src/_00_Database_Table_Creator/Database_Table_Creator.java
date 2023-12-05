package src._00_Database_Table_Creator;

import utility.Helper;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.sql.Date;
import src.convertCSV;
import java.math.BigInteger;

public class Database_Table_Creator {

    private static String database;
    private static String table;
    private static String[] column_names;

    // query to download tpch.lineitem table from database
    private static String query_base;

    // stores total number of rows of the table that need to be processed
    private static int totalRows;

    // stores the number threads and number of rows per thread that program should
    // run on. Currently, this code supports only single level threading
    private static int numThreads;
    private static int numRowsPerThread;

    // variables to stores output details
    private static boolean showDetails = false;
    private static final ArrayList<Long> threadTimes = new ArrayList<>();

    // stores database connection variable
    private static Connection con;

    // writers for writing into files
    private static FileWriter[] writers = new FileWriter[4];

    // Store column and type information for the table
    static Map<String, Integer> tableMetadata = new HashMap<String, Integer>();

    // constructor to initialize the object paramters
    public Database_Table_Creator(final int totalRows, final int numThreads, final boolean showDetails, String database,
            String table, String str_column_names) {
        Database_Table_Creator.totalRows = totalRows;
        Database_Table_Creator.numThreads = numThreads;
        Database_Table_Creator.numRowsPerThread = totalRows / numThreads;
        Database_Table_Creator.showDetails = showDetails;
        Database_Table_Creator.database = database;
        Database_Table_Creator.table = table;
        String[] col_names = str_column_names.split(",");
        Database_Table_Creator.column_names = col_names;
    }

    /**
     * Downloads Cleartext 'tpch.lineitem' table from database into 'data/cleartext
     * folder'. It then creates four
     * additive and multiplicative shares of the table stored into 'data/shares
     * folder'.
     *
     * @param args: takes number of rows to be processed as input
     */
    public static void main(String[] args) throws IOException {

        Database_Table_Creator DBShareCreation = new Database_Table_Creator(Integer.parseInt(args[0]), 1, false,
                args[1],
                args[2], args[3]);

        doPreWork();

        // doWork lcreates and starts all threads
        doWork();

        // Write to file
        doPostWork();
    }

    /**
     * Performs initialization tasks like connecting to the database, writer
     * objects.
     *
     * @throws IOException : throws during creation of directories
     */
    private static void doPreWork() throws IOException {

        // making connection to the database

        try {
            Database_Table_Creator.con = Helper.getConnection();
        } catch (SQLException ex) {
            System.out.println(ex);
        }

        // System.out.println("Successful - Connection to Database ");

        // creating directories to stores shares and cleartext files

        String diskPath = "data/shares/";

        Files.createDirectories(Paths.get(diskPath));

        // initializing writer object for file writes
        try {
            for (int i = 0; i < 4; i++) {
                writers[i] = new FileWriter(diskPath + "ServerTable" + (i + 1) + ".csv");
            }
        } catch (IOException ex) {
            Logger.getLogger(Database_Table_Creator.class.getName()).log(Level.SEVERE, null, ex);
        }

        // System.out.println("Successful - Preparing CSV Files");

        query_base = "select " + String.join(",", column_names) + " from " + database + "." + table + " LIMIT ";

        getTableMetadata();
        exportTableMetaData();
    }

    /**
     * Downloads the cleartext tpch.lineitem tables and creates four shares of the
     * same.
     * 
     * @return the time taken to download cleartext and create shares
     */
    private static long doWork() {

        // initializing thread list for storing all threads
        List<Thread> threadList = new ArrayList<>(); // The list containing all the threads

        long avgOperationTime = 0;

        // create threads and add them to thread list
        for (int i = 0; i < numThreads; i++) {
            int threadNum = i + 1;
            threadList.add(new Thread(new ParallelTask(numRowsPerThread, threadNum), "Thread" + threadNum));
        }

        // start all threads in the list
        for (int i = 0; i < numThreads; i++) {
            threadList.get(i).start();
        }

        // wait for all threads to finish
        for (Thread thread : threadList) {
            try {
                thread.join();
            } catch (InterruptedException ex) {
                System.out.println(ex.getMessage());
            }
        }

        /*
         * The Thread run times for each thread are stored in the array threadTimes.
         * This loop calculates the average
         * thread time across all the threads
         */
        for (int i = 0; i < threadTimes.size(); i++) {
            avgOperationTime += threadTimes.get(i);
        }

        return (avgOperationTime / numThreads);

    }

    // defines work performed by each thread
    private static class ParallelTask implements Runnable {

        private final int numRows;
        private final int threadNum;

        public ParallelTask(final int numRows, final int threadNum) {
            this.numRows = numRows;
            this.threadNum = threadNum;
        }

        @Override
        public void run() {

            Instant threadStartTime = Instant.now();

            int count = 0;

            // start count of the row
            int startRow = (threadNum - 1) * numRows;

            // random object for random number generation needed while shares creation
            Random rand = new Random(69);

            // stores the result returned by database
            ResultSet rs;

            Instant startTime = Instant.now();

            try {

                // downloads the tpch.lineitem table from the database
                Instant tableLoad = Instant.now();

                String query = query_base + startRow + ", " + numRows;
                Statement stmt = con.createStatement();
                rs = stmt.executeQuery(query);

                long tableLoadTime = Duration.between(tableLoad, Instant.now()).toMillis();

                // System.out.println("Successful - Cleartext Table Loading");

                Instant splittingStart = Instant.now();

                int rowNum = 1;

                // for each row, each column value's shares both additive and multiplicative is
                // created
                for (int i = startRow; i < startRow + numRows; i++) {
                    rs.next();
                    ArrayList<String> origVALS = new ArrayList<>();

                    int max_str_len = convertCSV.getMaxLength();

                    for (String column_name : column_names) {
                        if (getColumnType(column_name) == 0)
                            origVALS.add(String.valueOf(rs.getInt(column_name)));
                        else if(getColumnType(column_name) == 2) { // For Dates
                            Date temp_date = rs.getDate(column_name);
                            DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                            String date_str = dateFormat.format(temp_date);
                            origVALS.add(stringNumericalization(date_str, max_str_len));
                        }
                        else // For strings
                            origVALS.add(stringNumericalization(rs.getString(column_name), max_str_len));
                    }

                    // We need one list for additive shares
                    // and one list for multiplicative shares
                    // each list is a list of 4-value tuples, one for each part

                    ArrayList<String[]> additiveShares = new ArrayList<>();
                    ArrayList<String[]> multiplicativeShares = new ArrayList<>();

                    // For each value in the original table

                    for (int j = 0; j < column_names.length; j++) {
                        int col_type = getColumnType(column_names[j]);
                        if (col_type == 0) { // column is an int

                            // Additive Shares
                            int temp_mod = Helper.mod(rand.nextInt(1000000));
                            String part1_a = String.valueOf(temp_mod);
                            String part2_a = String.valueOf(Integer.parseInt(origVALS.get(j)) - temp_mod);
                            String part3_a = part1_a;
                            String part4_a = part2_a;

                            // Multiplicative Shares
                            int colVals[] = func(Integer.parseInt(origVALS.get(j)));
                            String part1_m = String.valueOf(colVals[0]);
                            String part2_m = String.valueOf(colVals[1]);
                            String part3_m = String.valueOf(colVals[2]);
                            String part4_m = String.valueOf(colVals[3]);

                            // Add to lists
                            additiveShares.add(new String[] { part1_a, part2_a, part3_a, part4_a });
                            multiplicativeShares.add(new String[] { part1_m, part2_m, part3_m, part4_m });
                        }

                        else if (col_type == 1 || col_type == 2) { // column is an string

                            // Additive Shares

                            String part1_a = "";
                            String part2_a = "";

                            for(int k = 0; k < max_str_len; k++) {
                                int temp_mod = Helper.mod(rand.nextInt(1000000));
                                String temp = String.valueOf(temp_mod);
                                part1_a += temp + "|";
                                int temp2 = Integer.parseInt(origVALS.get(j).substring(k*3, (k + 1)*3));
                                part2_a += String.valueOf(temp2 - temp_mod) + "|";
                            }

                            String part3_a = part1_a;
                            String part4_a = part2_a;

                            // Multiplicative Shares
                            BigInteger colVals[] = func(new BigInteger(origVALS.get(j)));
                            String part1_m = colVals[0].toString();
                            String part2_m = colVals[1].toString();
                            String part3_m = colVals[2].toString();
                            String part4_m = colVals[3].toString();

                            // Add to lists
                            additiveShares.add(new String[] { part1_a, part2_a, part3_a, part4_a });
                            multiplicativeShares.add(new String[] { part1_m, part2_m, part3_m, part4_m });
                        }
                    }

                    try {
                        // Write to file
                        for (int j = 0; j < 4; j++) {
                            for (int k = 0; k < column_names.length; k++) {
                                writers[j].append(additiveShares.get(k)[j]);
                                writers[j].append(",");
                            }

                            for (int k = 0; k < column_names.length; k++) {
                                writers[j].append(String.valueOf(multiplicativeShares.get(k)[j]));
                                writers[j].append(",");
                            }

                            writers[j].append(String.valueOf(rowNum) + "\n");
                        }

                    } catch (IOException ex) {
                        Logger.getLogger(Database_Table_Creator.class.getName())
                                .log(Level.SEVERE, null, ex);
                    }

                    if (i > 100 && i % (totalRows / 100) == 0) {
                        double percent = 100 * ((double) i / (double) totalRows);
                        // Helper.progressBar(percent,
                        // Duration.between(startTime, Instant.now()).toMillis());
                    }

                    rowNum++;

                }

                long tableSplittingTime = Duration.between(splittingStart, Instant.now()).toMillis();

                // System.out.println("Successful - Share Splitting");
                // System.out.println("Table Loading Time: " + tableLoadTime + " ms");
                // System.out.println("Table Splitting Time: " + tableSplittingTime + " ms");

            } catch (SQLException ex) {
                System.out.println(ex);
            }

            Duration totalThreadTime = Duration.between(threadStartTime, Instant.now());

            // depending upon the flag printing the details
            if (showDetails) {
                System.out.println("\n" + Thread.currentThread().getName().toUpperCase());
                System.out.println("Total Operations: " + count);
                System.out.println("Total Thread Time: " + totalThreadTime.toMillis() + " ms");
                System.out.println("Thread Start Time: " + DateTimeFormatter.ofPattern("hh:mm:ss.SSS")
                        .format(LocalDateTime.ofInstant(threadStartTime, ZoneOffset.UTC)));
                System.out.println("Thread Start Time: " + DateTimeFormatter.ofPattern("hh:mm:ss.SSS")
                        .format(LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC)));
            }
            threadTimes.add(totalThreadTime.toMillis());
        }
    }

    // performing post operations like closing, flushing the writers
    private static void doPostWork() {
        try {
            for (int i = 0; i < 4; i++) {
                writers[i].toString();
                writers[i].flush();
                writers[i].close();
            }

        } catch (IOException ex) {
            Logger.getLogger(Database_Table_Creator.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * The function performs multiplicative secret share creation using Shamir
     * Secret Sharing
     *
     * @param b : stores the value or secret whose Shamir Secret shares is to be
     *          created
     * @return array of four secret shares for the secret value
     */
    private static int[] func(int b) {
        Random rand = new Random(69);

        // stores random values generated for slope of the line
        int m = rand.nextInt(2000) - 1000;

        // as four shares are created for each value the value of x is 1,2,3,4 and
        // secret value b is the intercept
        int m1 = 1 * m + b;
        int m2 = 2 * m + b;
        int m3 = 3 * m + b;
        int m4 = 4 * m + b;

        // stores secret shares of the value b after performing modulus operation
        int vals[] = new int[] { Helper.mod(m1), Helper.mod(m2), Helper.mod(m3), Helper.mod(m4) };

        return vals;
    }

    private static BigInteger[] func(BigInteger b) {
        Random rand = new Random(69);

        // stores random values generated for slope of the line
        int m = rand.nextInt(2000) - 1000;

        // as four shares are created for each value the value of x is 1,2,3,4 and
        // secret value b is the intercept
        BigInteger m1 = BigInteger.valueOf(1 * m).add(b);
        BigInteger m2 = BigInteger.valueOf(2 * m).add(b);
        BigInteger m3 = BigInteger.valueOf(3 * m).add(b);
        BigInteger m4 = BigInteger.valueOf(4 * m).add(b);

        // stores secret shares of the value b after performing modulus operation
        // Cannot mod BigInteger or else row fetch returns wrong results
        //BigInteger vals[] = new BigInteger[] { Helper.mod(m1), Helper.mod(m2), Helper.mod(m3), Helper.mod(m4) }; 
        BigInteger vals[] = new BigInteger[] { m1, m2, m3, m4 };

        return vals;
    }

    private static int getColumnType(String col_name) {
        return tableMetadata.get(col_name);
    }

    private static void getTableMetadata() {
        Statement stmt;
        ResultSet rs;

        try {
            stmt = con.createStatement();
            rs = stmt.executeQuery("show COLUMNS from " + database + "." + table);

            while (rs.next()) {
                String col_type_str = rs.getString("Type");
                int col_type = 1;
                if (col_type_str.contains("int"))
                    col_type = 0;
                else if (col_type_str.contains("char"))
                    col_type = 1;
                else if (col_type_str.contains("date"))
                    col_type = 2;
                tableMetadata.put(rs.getString("Field"), col_type);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void exportTableMetaData() {
        try {
            FileWriter writer = new FileWriter("data/metadata/table_metadata.csv");
            for (Map.Entry<String, Integer> entry : tableMetadata.entrySet()) {
                writer.append(entry.getKey());
                writer.append(",");
                writer.append(String.valueOf(entry.getValue()));
                writer.append("\n");
            }
            writer.flush();
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String stringNumericalization(String str, int max_len) {
        // Insert error if exceeds max len?
        String numericals = "";

        if(str == null){
            for(int i = 0; i < max_len; i++)
                numericals += String.valueOf("999");
            return numericals;
        }

        char[] characters = str.toCharArray();

        int limit = Math.min(characters.length, max_len);

        for (int i = 0; i < limit; i++) {
            String buffer = "";
            for (int j = String.valueOf((int) characters[i]).length(); j < 3; j++)
                buffer += "0";
            numericals += buffer + String.valueOf((int) characters[i]);
        }
        for (int i = characters.length; i < max_len; i++) {
            numericals += String.valueOf("999");
        }

        return numericals;
    }

}
