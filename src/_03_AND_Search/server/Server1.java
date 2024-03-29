package src._03_AND_Search.server;

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

import constant.*;

public class Server1 {

    // query string to get server data from database
    private static final String query_base1_03 = "select ";
    private static final String query_base2_03 = " from " + Helper.getDatabaseName() + "." + Helper.getTableName() + "_SERVERTABLE1 where rowID > ";

    // the number of row of tpch.lineitem considered
    private static int numRows;
    // the number of threads server program is running on
    private static int numThreads;
    // the number of row per thread
    private static int numRowsPerThread;

    // the fingerprintPrimeNumber value i.e value of r which is taken as 43 in our case
    private static int fingerprintPrimeNumber;
    // the fingerprint value generated for server1
    private static int fingerprint1_03;
    // stores seed value for server for random number generation
    private static int seedServer;
    // stores seed value for client for random number generation
    private static int seedClient;
    // the list of name of the tpch.lineitem column to search over
    private static String[] columnName_03;

    // stores result after server processing
    private static int[] result_03;
    private static HashMap<Integer, Long> hashMap_03 = new HashMap<>();

    private static ArrayList<Instant> timestamps = new ArrayList<>();
    private static final Logger log_03 = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    // stores port for server
    private static int serverPort;
    // stores port for combiner
    private static int combinerPort;
    // stores IP for combiner
    private static String combinerIP;

    static Map<String, Integer> tableMetadata_03 = null;

    private static final int portIncrement = 0;

    // operation performed by each thread
    private static class ParallelTask_03 implements Runnable {

        private final int threadNum;

        public ParallelTask_03(int threadNum) {
            this.threadNum = threadNum;
        }

        @Override
        public void run() {
            // making connection to the database
            Connection con = null;
            try {
                con = Helper.getConnection();
            } catch (SQLException ex) {
                log_03.log(Level.SEVERE, ex.getMessage());
            }
            int startRow = (threadNum - 1) * numRowsPerThread;
            int endRow = startRow + numRowsPerThread;

            try {
                Random randSeedServer = new Random(seedServer);
                Random randSeedClient = new Random(seedClient);

                // Make a copy of the column names and add the "A_" prefix to the column names
                String [] columnNameCopy = new String[columnName_03.length];
                for (int i = 0; i < columnName_03.length; i++) {
                    columnNameCopy[i] = "A_" + columnName_03[i];
                }


                String columns = Helper.strArrToStr(columnNameCopy);

                //System.out.println(columns);
                //System.out.println("\n");


                String query = query_base1_03 + columns + query_base2_03 + startRow + " LIMIT " + numRowsPerThread;
                Statement stmt = con.createStatement();
                ResultSet rs = stmt.executeQuery(query);
                int prgServer, prgClient;

                String[] rowSplit;
                // performing server operation on each row of the database
                for (int i = startRow; i < endRow; i++) {
                    int start = 1;
                    rs.next();
                    prgServer = randSeedServer.nextInt(Constants.getMaxRandomBound() -
                            Constants.getMinRandomBound()) + Constants.getMinRandomBound();
                    prgClient = randSeedClient.nextInt(Constants.getMaxRandomBound() -
                            Constants.getMinRandomBound()) + Constants.getMinRandomBound();

                    // process for each column of string or numeric type
                    for (int k = 0; k < columnName_03.length; k++) {

                        int col_type = getColumnType_03(columnName_03[k]);

                        if (col_type == 0) { // int columns

                            if (!hashMap_03.containsKey(start)) {
                                hashMap_03.put(start, Helper.mod((long) Math.pow(fingerprintPrimeNumber, start)));
                            }
                            result_03[i] = (int) Helper.mod(result_03[i] +
                                    Helper.mod(hashMap_03.get(start) * rs.getLong("A_" + columnName_03[k])));
                            start++;
                        } else { // string columns
                            rowSplit = rs.getString("A_" + columnName_03[k]).split("\\|");

                            for (int j = 0; j < rowSplit.length; j++) {
                                if (!hashMap_03.containsKey(start)) {
                                    hashMap_03.put(start, Helper.mod((long) Math.pow(fingerprintPrimeNumber, start)));
                                }
                                result_03[i] = (int) Helper.mod(result_03[i] +
                                        Helper.mod(hashMap_03.get(start) * Integer.parseInt(rowSplit[j])));
                                start++;
                            }     
                        }
                    }
                    result_03[i] = (int) Helper.mod(Helper.mod(Helper.mod((long) result_03[i] - fingerprint1_03)
                            * prgServer) + prgClient);
                }
            } catch (SQLException ex) {
                log_03.log(Level.SEVERE, ex.getMessage());
            }
        }
    }

    // executing server operation over threads
    private static void doWork_03(String[] data) {

        columnName_03 = Helper.strToStrArr(data[0]);
        fingerprint1_03 = Integer.parseInt(data[1]);
        seedClient = Integer.parseInt(data[2]);
        result_03 = new int[numRows];
        hashMap_03 = new HashMap<>();


        // the list containing all the threads
        List<Thread> threadList = new ArrayList<>();

        // create threads and add them to threadlist
        int threadNum;
        for (int i = 0; i < numThreads; i++) {
            threadNum = i + 1;
            threadList.add(new Thread(new ParallelTask_03(threadNum), "Thread" + threadNum));
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
                log_03.log(Level.SEVERE, ex.getMessage());
            }
        }
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
                doWork_03(dataReceived);

                // sending the processed data to Combiner
                combinerSocket = new Socket(combinerIP, combinerPort);
                outToCombiner = new ObjectOutputStream(combinerSocket.getOutputStream());
                outToCombiner.writeObject(result_03);
                combinerSocket.close();

                // calculating timestamps
                timestamps.add(Instant.now());
//                System.out.println(Helper.getProgramTimes(timestamps));
//                log.log(Level.INFO, "Total Server1 time:" + Helper.getProgramTimes(timestamps));
            } catch (IOException | ClassNotFoundException ex) {
                log_03.log(Level.SEVERE, ex.getMessage());
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
            log_03.log(Level.SEVERE, ex.getMessage());
        }
    }

    /**
     * It performs initialization tasks
     */
    private static void doPreWork() {

        // reads configuration properties of the server
        String pathName = "config/Server1.properties";
        Properties properties = Helper.readPropertiesFile( pathName);

        seedServer = Integer.parseInt(properties.getProperty("seedServer"));
        fingerprintPrimeNumber = Integer.parseInt(properties.getProperty("fingerprintPrimeNumber"));

        numRows = Integer.parseInt(properties.getProperty("numRows"));
        numThreads = Integer.parseInt(properties.getProperty("numThreads"));
        numRowsPerThread = numRows / numThreads;

        serverPort = Integer.parseInt(properties.getProperty("serverPort")) + portIncrement;
        combinerPort = Integer.parseInt(properties.getProperty("combinerPort")) + portIncrement;
        combinerIP = properties.getProperty("combinerIP");
    }

    
    private static int getColumnType_03(String col_name){
        if(tableMetadata_03 == null)
            tableMetadata_03 = Helper.getColumnList();
        return tableMetadata_03.get(col_name.toLowerCase());
    }

    // performs server task required to process client query
    public static void main(String[] args) throws IOException {

        doPreWork();

        Server1 server1 = new Server1();
        server1.startServer();

    }
}


