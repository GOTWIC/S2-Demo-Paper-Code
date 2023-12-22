package src._05_Multiplicative_Row_Fetch.server;

import constant.Constants;
import utility.Helper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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

public class Server2 {

    // query string to get server data from database
    private static final String query_base = " from " + Helper.getDatabaseName() + "." + Helper.getTableName() + "_SERVERTABLE2 where rowID > ";



    // the number of row of tpch.lineitem considered
    private static int numRows;
    // the number of threads server program is running on
    private static int numThreads;
    // the number of row per thread
    private static int numRowsPerThread;

    private static BigInteger[][][][] col_sum;
    private static BigInteger[][] total_sum;

    // the total number of row ids requested
    private static int querySize;
    // the size of filter based on number of rows considered which is sqrt(numRows)
    private static int filter_size;
    // stores the row filter for row ids value
    private static int[][] row_filter;
    // stores the column filter for row ids value
    private static int[][] col_filter;
    // stores seed value for client for random number generation
    private static int seedClient;

    // stores result after server processing
    private static BigInteger[][] result;
    private static ArrayList<Instant> timestamps = new ArrayList<>();
    private static final Logger log = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);


    // stores port for server
    private static int serverPort;
    // stores port for combiner
    private static int combinerPort;
    // stores IP for combiner
    private static String combinerIP;

    private static int numCols; 
    private static String columnNames = "";
    private static String[] columnNamesArr;

    private static final int portIncrement = 40;

    // operation performed by each thread
    private static class ParallelTask implements Runnable {

        private final int threadNum;

        public ParallelTask(int threadNum) {
            this.threadNum = threadNum;
        }

        @Override
        public void run() {
            // making connection to the database

            Connection con = null;

            try {
                con = Helper.getConnection();
            } catch (SQLException ex) {
                log.log(Level.SEVERE, ex.getMessage());
            }

            int startRow = (threadNum - 1) * numRowsPerThread;
            int endRow = startRow + numRowsPerThread;

            try {

                String query = "select " + columnNames  + query_base + startRow + " LIMIT " + numRowsPerThread;                Statement stmt = con.createStatement();
                ResultSet rs = stmt.executeQuery(query);

                // performing server operation on each row of the database
                for (int i = startRow; i < endRow; i++) {
                    rs.next();
                    // multiplication with the row filter for each column value
                    for (int j = 0; j < querySize; j++) {
                        BigInteger temp = BigInteger.valueOf(row_filter[j][i / filter_size]);
                        for(int k = 0; k < numCols; k++){
                            col_sum[k][j][threadNum - 1][i % filter_size] = Helper.mod(col_sum[k][j][threadNum - 1][i % filter_size].add(Helper.mod(new BigInteger(rs.getString(columnNamesArr[k]))).multiply(temp)));
                        }
                    }
                }

                for (int i = 0; i < querySize; i++) {
                    // multiplication with the col filter for each column value
                    for (int j = 0; j < filter_size; j++) {
                        for(int k = 0; k < numCols; k++){
                            col_sum[k][i][threadNum - 1][j] = Helper.mod(col_sum[k][i][threadNum - 1][j].multiply(BigInteger.valueOf(col_filter[i][j])));
                            total_sum[k][i] = Helper.mod(total_sum[k][i].add(col_sum[k][i][threadNum - 1][j]));
                        }
                    }
                }
            } catch (SQLException ex) {
                log.log(Level.SEVERE, ex.getMessage());
            }
            try {
                con.close();
            } catch (SQLException ex) {
                log.log(Level.SEVERE, ex.getMessage());
            }
        }
    }

    // executing server operation over threads
    private static void doWork(String[] data) {

        row_filter = Helper.strToStrArr1(data[0]);
        col_filter = Helper.strToStrArr1(data[1]);
        seedClient = Integer.parseInt(data[2]);

        querySize = row_filter.length;
        result = new BigInteger[querySize + 1][numCols];

        col_sum = new BigInteger[numCols][querySize][numThreads][filter_size];
        total_sum = new BigInteger[numCols][querySize];

        // initialize col_sum and total_sum to 0
        for(int i = 0; i < numCols; i++){
            for(int j = 0; j < querySize; j++){
                for(int k = 0; k < numThreads; k++){
                    for(int l = 0; l < filter_size; l++){
                        col_sum[i][j][k][l] = BigInteger.valueOf(0);
                    }
                }
                total_sum[i][j] = BigInteger.valueOf(0);
            }
        }

        // the list containing all the threads
        List<Thread> threadList = new ArrayList<>();

        // Create threads and add them to threadlist
        int threadNum;
        for (int i = 0; i < numThreads; i++) {
            threadNum = i + 1;
            threadList.add(new Thread(new ParallelTask(threadNum), "Thread" + threadNum));
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
                log.log(Level.SEVERE, ex.getMessage());
            }
        }

        Random randSeedClient = new Random(seedClient);
        // adding random value before sending to Client
        for (int i = 0; i < querySize; i++) {
            BigInteger randClient = BigInteger.valueOf(randSeedClient.nextInt(Constants.getMaxRandomBound() - Constants.getMinRandomBound())
                    + Constants.getMinRandomBound());

            for(int j = 0; j < numCols; j++){
                result[i][j] = Helper.mod(total_sum[j][i].add(randClient));
            }
        }

        result[querySize][0] = BigInteger.valueOf(2);
    }

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
                doWork(dataReceived);

                // sending the processed data to Combiner
                combinerSocket = new Socket(combinerIP, combinerPort);
                outToCombiner = new ObjectOutputStream(combinerSocket.getOutputStream());
                outToCombiner.writeObject(result);
                combinerSocket.close();

                // calculating timestamps
                timestamps.add(Instant.now());
//                System.out.println(Helper.getProgramTimes(timestamps));
//                log.log(Level.INFO, "Total Server2 time:" + Helper.getProgramTimes(timestamps));
            } catch (IOException | ClassNotFoundException ex) {
                log.log(Level.SEVERE, ex.getMessage());
            }
        }
    }

    // starting server to listening for incoming connection
    private void startServer() throws IOException {
        Socket socket;

        try {
            ServerSocket ss = new ServerSocket(serverPort);
            System.out.println("Server2 Listening........");

            do {
                // listening over socket for connections
                socket = ss.accept();
                timestamps = new ArrayList<>();
                timestamps.add(Instant.now());
                new SocketCreation(socket).run();
            } while (true);
        } catch (IOException ex) {
            log.log(Level.SEVERE, ex.getMessage());
        }
    }

    /**
     * It performs initialization tasks
     */
    private static void doPreWork() {

        // reads configuration properties of the server
        String pathName = "config/Server2.properties";
        Properties properties = Helper.readPropertiesFile(pathName);

        numRows = Integer.parseInt(properties.getProperty("numRows"));
        numThreads = Integer.parseInt(properties.getProperty("numThreads"));
        numRowsPerThread = numRows / numThreads;

        serverPort = Integer.parseInt(properties.getProperty("serverPort")) + portIncrement;
        combinerPort = Integer.parseInt(properties.getProperty("combinerPort")) + portIncrement;
        combinerIP = properties.getProperty("combinerIP");

        filter_size = (int) Math.sqrt(numRows);

        setEnv();
    }

    private static void setEnv(){
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
        numCols = tableMetadata.size();
        for (Map.Entry<String, Integer> entry : tableMetadata.entrySet()) {
            columnNames += "M_" + entry.getKey() + ",";
        }
        columnNames = columnNames.substring(0, columnNames.length() - 1);
        columnNamesArr = columnNames.split(",");
    }

    // performs server task required to process client query
    public static void main(String[] args) throws IOException {

        doPreWork();

        Server2 Server2 = new Server2();
        Server2.startServer();

    }
}


