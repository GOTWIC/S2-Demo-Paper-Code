package src;

import utility.Helper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.math.BigInteger;

import constant.*;

public class server4 {

    // ---------------- UNIVERSAL SERVER GLOBALS ---------------- \\

    // the number of row of tpch.lineitem considered
    private static int numRows;
    // the number of threads server program is running on
    private static int numThreads;
    // the number of row per thread
    private static int numRowsPerThread;
    private static ArrayList<Instant> timestamps = new ArrayList<>();
    // stores port for server
    private static int serverPort;
    // stores port for combiner
    private static int combinerPort;
    // stores IP for combiner
    private static String combinerIP;
    private static final int portIncrement = 0;

    // ---------------- 04 SERVER GLOBALS ---------------- \\

    // query string to get server data from database
    private static final String query_base1_04 = "select ";
    private static final String query_base2_04 = " from " + Helper.getDatabaseName() + "." + Helper.getTableName() + "_SERVERTABLE4 where rowID > ";
    // stores multiplicative share for search key values
    private static BigInteger[] multiplicativeShare_04;
    // stores seed value for client for random number generation
    private static int seedClient_04;
    // the list of name of the tpch.lineitem column to search over
    private static String[] columnName_04;
    // number of columns
    private static int columnCount_04;
    // stores result after server processing
    private static BigInteger[][] result_04;
    private static final Logger log_04 = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    // ---------------- 05 SERVER GLOBALS ---------------- \\

    // query string to get server data from database
    private static final String query_base_05 = " from " + Helper.getDatabaseName() + "." + Helper.getTableName() + "_SERVERTABLE4 where rowID > ";
    private static BigInteger[][][][] col_sum_05;
    private static BigInteger[][] total_sum_05;
    // the total number of row ids requested
    private static int querySize_05;
    // the size of filter based on number of rows considered which is sqrt(numRows)
    private static int filter_size_05;
    // stores the row filter for row ids value
    private static int[][] row_filter_05;
    // stores the column filter for row ids value
    private static int[][] col_filter_05;
    // stores seed value for client for random number generation
    private static int seedClient_05;
    // stores result after server processing
    private static BigInteger[][] result_05;
    private static final Logger log_05 = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    private static int numCols_05; 
    private static String columnNames_05 = "";
    private static String[] columnNamesArr_05;


    // ---------------- 04 SERVER CODE ---------------- \\

    // operation performed by each thread
    private static class ParallelTask_04 implements Runnable {

        private final int threadNum;

        public ParallelTask_04(int threadNum) {
            this.threadNum = threadNum;
        }

        @Override
        public void run() {
            // making connection to the database
            Connection con = null;

            try {
                con = Helper.getConnection();
            } catch (SQLException ex) {
                log_04.log(Level.SEVERE, ex.getMessage());
            }

            int startRow = (threadNum - 1) * numRowsPerThread;
            int endRow = startRow + numRowsPerThread;

            Random randClient = new Random(seedClient_04);
            String columns = Helper.strArrToStr(columnName_04);

            try {
                String query = query_base1_04 + columns + query_base2_04 + startRow + " LIMIT " + numRowsPerThread;
                Statement stmt = con.createStatement();
                ResultSet rs = stmt.executeQuery(query);

                // performing server operation on each row of the database
                for (int i = startRow; i < endRow; i++) {
                    rs.next();

                    int randSeedClient = randClient.nextInt(Constants.getMaxRandomBound() - Constants.getMinRandomBound())
                            + Constants.getMinRandomBound();
                    BigInteger product1 = new BigInteger("1"), product2 = new BigInteger("1");

                    if (columnCount_04 > 3) { // runs for server count =4
                        for (int j = 0; j < (columnName_04.length / 2); j++) {
                            product1 = Helper.mod(product1.multiply(Helper.mod(new BigInteger(rs.getString(columnName_04[j])).subtract(multiplicativeShare_04[j]))));
                            product2 = Helper.mod(product2.multiply(Helper.mod(new BigInteger(rs.getString(columnName_04[j + 2])).subtract(multiplicativeShare_04[j + 2]))));
                        }
                    } else { // runs for server count<= 3
                        for (int j = 0; j < columnName_04.length; j++) {
                            product1 = Helper.mod(product1.multiply(Helper.mod(new BigInteger(rs.getString(columnName_04[j])).subtract(multiplicativeShare_04[j]))));
                        }
                    }
                    result_04[0][i] = Helper.mod(product1.add(BigInteger.valueOf(randSeedClient)));
                    if (columnCount_04 > 3)
                        result_04[1][i] = Helper.mod(product2.add(BigInteger.valueOf(randSeedClient)));
                }
            } catch (SQLException ex) {
                log_04.log(Level.SEVERE, ex.getMessage());
            }
            try {
                con.close();
            } catch (SQLException ex) {
                log_04.log(Level.SEVERE, ex.getMessage());
            }
        }
    }

    // executing server operation over thread
    private static void doWork_04(String[] data) throws IOException {

        columnName_04 = Helper.strToStrArr(data[1]);
        multiplicativeShare_04 = Helper.strToBiArr(data[2]);
        columnCount_04 = columnName_04.length;
        seedClient_04 = Integer.parseInt(data[3]);

        int resultDim = 1;
        if (columnCount_04 > 3) {
            resultDim = 2;
        }

        result_04 = new BigInteger[resultDim + 1][numRows];

        // the list containing all the threads
        List<Thread> threadList = new ArrayList<>();

        // create threads and add them to threadlist
        int threadNum;
        for (int i = 0; i < numThreads; i++) {
            threadNum = i + 1;
            threadList.add(new Thread(new ParallelTask_04(threadNum), "Thread" + threadNum));
        }

        // start all threads
        for (int i = 0; i < numThreads; i++) {
            threadList.get(i).start();
        }

        // wait for all threads to finish
        for (Thread thread : threadList) {
            try {
                thread.join();
            } catch (InterruptedException ex) {
                log_04.log(Level.SEVERE, ex.getMessage());
            }
        }
        result_04[resultDim][0] = BigInteger.valueOf(4);
    }

    // ---------------- 05 SERVER CODE ---------------- \\

     // operation performed by each thread
     private static class ParallelTask_05 implements Runnable {

        private final int threadNum;

        public ParallelTask_05(int threadNum) {
            this.threadNum = threadNum;
        }

        @Override
        public void run() {
            // making connection to the database
            Connection con = null;

            try {
                con = Helper.getConnection();
            } catch (SQLException ex) {
                log_05.log(Level.SEVERE, ex.getMessage());
            }

            int startRow = (threadNum - 1) * numRowsPerThread;
            int endRow = startRow + numRowsPerThread;

            try {

                String query = "select " + columnNames_05  + query_base_05 + startRow + " LIMIT " + numRowsPerThread;                Statement stmt = con.createStatement();
                ResultSet rs = stmt.executeQuery(query);

                // performing server operation on each row of the database
                for (int i = startRow; i < endRow; i++) {
                    String st_val = "";
                    if(i < numRows)
                        rs.next();
                    // multiplication with the row filter for each column value
                    for (int j = 0; j < querySize_05; j++) {
                        BigInteger temp = BigInteger.valueOf(row_filter_05[j][i / filter_size_05]);
                        for(int k = 0; k < numCols_05; k++){
                            if(i < numRows)
                                st_val = rs.getString(columnNamesArr_05[k]);
                            else
                                st_val = "0";
                            col_sum_05[k][j][threadNum - 1][i % filter_size_05] = Helper.mod(col_sum_05[k][j][threadNum - 1][i % filter_size_05].add(Helper.mod(new BigInteger(st_val)).multiply(temp)));
                        }
                    }
                }

                for (int i = 0; i < querySize_05; i++) {
                    // multiplication with the col filter for each column value
                    for (int j = 0; j < filter_size_05; j++) {
                        for(int k = 0; k < numCols_05; k++){
                            col_sum_05[k][i][threadNum - 1][j] = Helper.mod(col_sum_05[k][i][threadNum - 1][j].multiply(BigInteger.valueOf(col_filter_05[i][j])));
                            total_sum_05[k][i] = Helper.mod(total_sum_05[k][i].add(col_sum_05[k][i][threadNum - 1][j]));
                        }
                    }
                }
            } catch (SQLException ex) {
                log_05.log(Level.SEVERE, ex.getMessage());
            }
            try {
                con.close();
            } catch (SQLException ex) {
                log_05.log(Level.SEVERE, ex.getMessage());
            }
        }
    }

    // executing server operation over threads
    private static void doWork_05(String[] data) {

        row_filter_05 = Helper.strToStrArr1(data[1]);
        col_filter_05 = Helper.strToStrArr1(data[2]);
        seedClient_05 = Integer.parseInt(data[3]);

        querySize_05 = row_filter_05.length;
        result_05 = new BigInteger[querySize_05 + 1][numCols_05];

        col_sum_05 = new BigInteger[numCols_05][querySize_05][numThreads][filter_size_05];
        total_sum_05 = new BigInteger[numCols_05][querySize_05];

        // initialize col_sum and total_sum to 0
        for(int i = 0; i < numCols_05; i++){
            for(int j = 0; j < querySize_05; j++){
                for(int k = 0; k < numThreads; k++){
                    for(int l = 0; l < filter_size_05; l++){
                        col_sum_05[i][j][k][l] = BigInteger.valueOf(0);
                    }
                }
                total_sum_05[i][j] = BigInteger.valueOf(0);
            }
        }

        // the list containing all the threads
        List<Thread> threadList = new ArrayList<>();

        // create threads and add them to threadlist
        int threadNum;
        for (int i = 0; i < numThreads; i++) {
            threadNum = i + 1;
            threadList.add(new Thread(new ParallelTask_05(threadNum), "Thread" + threadNum));
        }

        // start all threads
        for (int i = 0; i < numThreads; i++) {
            threadList.get(i).start();
        }

        // wait for all threads to finish
        for (Thread thread : threadList) {
            try {
                thread.join();
            } catch (InterruptedException ex) {
                log_05.log(Level.SEVERE, ex.getMessage());
            }
        }

        Random randSeedClient = new Random(seedClient_05);
        // adding random value before sending to Client
        for (int i = 0; i < querySize_05; i++) {
            BigInteger randClient = BigInteger.valueOf(randSeedClient.nextInt(Constants.getMaxRandomBound() - Constants.getMinRandomBound())
                    + Constants.getMinRandomBound());

            for(int j = 0; j < numCols_05; j++){
                result_05[i][j] = Helper.mod(total_sum_05[j][i].add(randClient));
            }
        }

        result_05[querySize_05][0] = BigInteger.valueOf(4);
    }


    private static void setEnv_05(){
        Map<String, Integer> tableMetadata = new HashMap<String, Integer>();
        // Open csv file at "data/metadata/table_metadata.csv" and load into tableMetaData hasmap
        String csvFile = "data/metadata/table_metadata.csv";
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";
        try {
            br = new BufferedReader(new FileReader(csvFile));
            while ((line = br.readLine()) != null) {
                String[] table_metadata = line.split(cvsSplitBy);
                tableMetadata.put(table_metadata[0], Integer.parseInt(table_metadata[1]));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        numCols_05 = tableMetadata.size();
        for (Map.Entry<String, Integer> entry : tableMetadata.entrySet()) {
            columnNames_05 += "M_" + entry.getKey() + ",";
        }
        columnNames_05 = columnNames_05.substring(0, columnNames_05.length() - 1);
        columnNamesArr_05 = columnNames_05.split(",");
    }



    // ---------------- UNIVERSAL CODE ---------------- \\


    // performing operations on data received over socket
    static class SocketCreation {

        private final Socket clientSocket;


        SocketCreation(Socket clientSocket) {
            this.clientSocket = clientSocket;
        }

        public void run() {
            ObjectInputStream inFromClient;
            Socket combinerSocket;
            ObjectOutputStream outToCombiner;
            String[] dataReceived;

            try {
                // reading the data sent by Client
                inFromClient = new ObjectInputStream(clientSocket.getInputStream());
                dataReceived = (String[]) inFromClient.readObject();
                String protocol = dataReceived[0];


                if(protocol.equals("or")){
                    doWork_04(dataReceived);
                    clientSocket.close();

                    // sending the processed data to Combiner
                    combinerSocket = new Socket(combinerIP, combinerPort + 10);
                    outToCombiner = new ObjectOutputStream(combinerSocket.getOutputStream());

                    // New array with an additional row
                    //BigInteger[][] newresult = new BigInteger[result_04.length + 1][result_04[0].length];

                    // Fill the first row of the new array with the dummy value 4
                    //Arrays.fill(newresult[0], BigInteger.valueOf(4));

                    // Copy the original array into the new array, starting from the second row
                    //System.arraycopy(result_04, 0, newresult, 1, result_04.length);

                    outToCombiner.writeObject(result_04);
                    combinerSocket.close();
                }

                else if(protocol.equals("row")){
                    doWork_05(dataReceived);
                    clientSocket.close();

                    // sending the processed data to Combiner
                    combinerSocket = new Socket(combinerIP, combinerPort);
                    outToCombiner = new ObjectOutputStream(combinerSocket.getOutputStream());

                    // New array with an additional row
                    BigInteger[][] newresult = new BigInteger[result_05.length + 1][result_05[0].length];

                    // Fill the first row of the new array with the dummy value 4
                    Arrays.fill(newresult[0], BigInteger.valueOf(5));

                    // Copy the original array into the new array, starting from the second row
                    System.arraycopy(result_05, 0, newresult, 1, result_05.length);

                    outToCombiner.writeObject(newresult);
                    combinerSocket.close();
                }

                // calculating timestamps
                timestamps.add(Instant.now());
//                System.out.println(Helper.getProgramTimes(timestamps));
//                log.log(Level.INFO, "Total Server4 time:" + Helper.getProgramTimes(timestamps));
            } catch (IOException | ClassNotFoundException ex) {
                log_04.log(Level.SEVERE, ex.getMessage());
            }
        }
    }

    // starting server to listening for incoming connection
    private void startServer() throws IOException {
        Socket socket;

        try {
            ServerSocket ss = new ServerSocket(serverPort);
            //System.out.println("Server4 Listening........");

            do {
                // listening over socket for connections
                socket = ss.accept();
                timestamps = new ArrayList<>();
                timestamps.add(Instant.now());
                new SocketCreation(socket).run();
            } while (true);
        } catch (IOException ex) {
            log_04.log(Level.SEVERE, ex.getMessage());
        }
    }

    // performs initialization tasks
    private static void doPreWork() {

        // reads configuration properties of the server
        String pathName = "config/Server4.properties";
        Properties properties = Helper.readPropertiesFile( pathName);

        numRows = Integer.parseInt(properties.getProperty("numRows"));
        numThreads = Integer.parseInt(properties.getProperty("numThreads"));
        numRowsPerThread = numRows / numThreads;

        serverPort = Integer.parseInt(properties.getProperty("serverPort")) + portIncrement;
        combinerPort = Integer.parseInt(properties.getProperty("combinerPort")) + portIncrement;
        combinerIP = properties.getProperty("combinerIP");

        filter_size_05 = (int) Math.ceil(Math.sqrt(numRows));

        setEnv_05();
    }

    // performs server task required to process client query
    public static void main(String[] args) throws IOException {

        doPreWork();

        server4 server4 = new server4();
        server4.startServer();

    }
}



