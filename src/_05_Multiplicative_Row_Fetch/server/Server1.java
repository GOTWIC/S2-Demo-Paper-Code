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

public class Server1 {

    // query string to get server data from database
    private static final String query_base_05 = " from " + Helper.getDatabaseName() + "." + Helper.getTableName() + "_SERVERTABLE1 where rowID > ";

    // the number of row of tpch.lineitem considered
    private static int numRows;
    // the number of threads server program is running on
    private static int numThreads;
    // the number of row per thread
    private static int numRowsPerThread;

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
    private static ArrayList<Instant> timestamps = new ArrayList<>();
    private static final Logger log_05 = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);


    // stores port for server
    private static int serverPort;
    // stores port for combiner
    private static int combinerPort;
    // stores IP for combiner
    private static String combinerIP;

    private static int numCols_05; 
    private static String columnNames_05 = "";
    private static String[] columnNamesArr_05;

    private static final int portIncrement = 40;

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

                String query = "select " + columnNames_05  + query_base_05 + startRow + " LIMIT " + numRowsPerThread;
                Statement stmt = con.createStatement();
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

        row_filter_05 = Helper.strToStrArr1(data[0]);
        col_filter_05 = Helper.strToStrArr1(data[1]);
        seedClient_05 = Integer.parseInt(data[2]);

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

        result_05[querySize_05][0] = BigInteger.valueOf(1);

        //System.out.println(result[0][0] + " " + result[0][1] + " " + result[0][2] + " " + result[0][3]);
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
                doWork_05(dataReceived);

                // sending the processed data to Combiner
                combinerSocket = new Socket(combinerIP, combinerPort);
                outToCombiner = new ObjectOutputStream(combinerSocket.getOutputStream());
                outToCombiner.writeObject(result_05);
                combinerSocket.close();

                // calculating timestamps
                timestamps.add(Instant.now());
            } catch (IOException | ClassNotFoundException ex) {
                log_05.log(Level.SEVERE, ex.getMessage());
            }
        }
    }

    // starting server to listening for incoming connection
    private void startServer() throws IOException {
        Socket socket;

        try {
            ServerSocket ss = new ServerSocket(serverPort);
            //System.out.println("Server1 Listening........");

            do {
                // listening over socket for connections
                socket = ss.accept();
                timestamps = new ArrayList<>();
                timestamps.add(Instant.now());
                new SocketCreation(socket).run();
            } while (true);
        } catch (IOException ex) {
            log_05.log(Level.SEVERE, ex.getMessage());
        }
    }

    /**
     * It performs initialization tasks
     */
    private static void doPreWork() {

        // reads configuration properties of the server
        String pathName = "config/Server1.properties";
        Properties properties = Helper.readPropertiesFile(pathName);

        numRows = Integer.parseInt(properties.getProperty("numRows"));
        numThreads = Integer.parseInt(properties.getProperty("numThreads"));
        // phantom rows includes padding
        int numRowsPhantom = (int) Math.pow((int) Math.ceil(Math.sqrt(numRows)), 2); 
        numRowsPerThread = numRowsPhantom / numThreads;

        serverPort = Integer.parseInt(properties.getProperty("serverPort")) + portIncrement;
        combinerPort = Integer.parseInt(properties.getProperty("combinerPort")) + portIncrement;
        combinerIP = properties.getProperty("combinerIP");

        filter_size_05 = (int) Math.ceil(Math.sqrt(numRows));

        setEnv_05();
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

    


    // performs server task required to process client query
    public static void main(String[] args) throws IOException {
        doPreWork();
        Server1 server1 = new Server1();
        server1.startServer();
    }
}


