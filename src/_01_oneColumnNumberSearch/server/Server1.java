package src._01_oneColumnNumberSearch.server;

import constant.*;
import utility.Helper;

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

public class Server1 {

    // query string to get server data from database
    private static final String query_base1_01 = "select ";
    private static final String query_base2_01 = " from " + Helper.getDatabaseName() + "." + Helper.getTableName() + "_SERVERTABLE1 where rowID > ";

    // the number of row of tpch.lineitem considered
    private static int numRows;
    // the number of threads server program is running on
    private static int numThreads;
    // the number of row per thread
    private static int numRowsPerThread;

    // the fingerprintPrimeNumber value i.e value of r which is taken as 43 in our case
    private static int fingerprintPrimeNumber_01;
    // the fingerprint value generated for server1
    private static int addShare1_01;
    // stores seed value for server for random number generation
    private static int seedServer_01;
    // stores seed value for client for random number generation
    private static int seedClient_01;
    // the name of the tpch.lineitem column to search over
    private static String columnName_01;

    // stores result after server processing
    private static int[] result_01;
    private static HashMap<Integer, Long> hashMap_01 = new HashMap<>();

    private static ArrayList<Instant> timestamps_01 = new ArrayList<>();
    private static final Logger log = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    private static final int portIncrement = 0;


    // stores port for server
    private static int serverPort_01;
    // stores port for combiner
    private static int combinerPort_01;
    // stores IP for combiner
    private static String combinerIP_01;

    // operation performed by each thread
    private static class ParallelTask_01 implements Runnable {

        private final int threadNum;

        public ParallelTask_01(int threadNum) {
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
                String query = query_base1_01 + columnName_01 + query_base2_01 + startRow + " LIMIT " + numRowsPerThread;
                Statement stmt = con.createStatement();
                ResultSet rs = stmt.executeQuery(query);

                String row;
                String[] rowSplit;
                int prgServer, prgClient;
                Random randSeedServer = new Random(seedServer_01);
                Random randSeedClient = new Random(seedClient_01);

                // performing server operation on each row of the database
                for (int i = startRow; i < endRow; i++) {
                    rs.next();
                    prgServer = randSeedServer.nextInt(Constants.getMaxRandomBound() -
                            Constants.getMinRandomBound()) + Constants.getMinRandomBound();
                    prgClient = randSeedClient.nextInt(Constants.getMaxRandomBound() -
                            Constants.getMinRandomBound()) + Constants.getMinRandomBound();

                    result_01[i] = (int) Helper.mod(Helper.mod((rs.getLong(columnName_01) - addShare1_01) *
                            prgServer) + prgClient);

                    //System.out.println("Row Val: " + rs.getLong(columnName) + " prgServer: " + prgServer + " prgClient: " + prgClient
                    //        + " addShare1: " + addShare1 + " result: " + result[i]);
                }
            } catch (SQLException ex) {
                log.log(Level.SEVERE, ex.getMessage());
            }
        }
    }

    // executing server operation over threads
    private static void doWork_01(String[] data) {

        columnName_01 = "A_" + data[0];
        addShare1_01 = Integer.parseInt(data[1]);
        seedClient_01 = Integer.parseInt(data[2]);
        result_01 = new int[numRows];

        // the list containing all the threads
        List<Thread> threadList = new ArrayList<>();

        // create threads and add them to threadlist
        int threadNum;
        for (int i = 0; i < numThreads; i++) {
            threadNum = i + 1;
            threadList.add(new Thread(new ParallelTask_01(threadNum), "Thread" + threadNum));
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
    }

    // performing operations on data received over socket
    static class SocketCreation_01 {

        private final Socket clientSocket;


        SocketCreation_01(Socket clientSocket) {
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
                doWork_01(dataReceived);

                // sending the processed data to Combiner
                combinerSocket = new Socket(combinerIP_01, combinerPort_01);
                outToCombiner = new ObjectOutputStream(combinerSocket.getOutputStream());
                // Print result
                //System.out.println("Server1 Result: " + Arrays.toString(result));
                outToCombiner.writeObject(result_01);
                combinerSocket.close();

                // calculating timestamps
                timestamps_01.add(Instant.now());
//                System.out.println(Helper.getProgramTimes(timestamps));
//                log.log(Level.INFO, "Total Server1 time:" + Helper.getProgramTimes(timestamps));
            } catch (IOException | ClassNotFoundException ex) {
                log.log(Level.SEVERE, ex.getMessage());
            }
        }
    }

    // starting server to listening for incoming connection
    private void startServer_01() throws IOException {
        Socket socket;

        try {
            ServerSocket ss = new ServerSocket(serverPort_01);
            //System.out.println("Server1 Listening........");

            do {
                // listening over socket for connections
                socket = ss.accept();
                timestamps_01 = new ArrayList<>();
                timestamps_01.add(Instant.now());
                new SocketCreation_01(socket).run();
            } while (true);
        } catch (IOException ex) {
            log.log(Level.SEVERE, ex.getMessage());
        }
    }

    // Performs initialization tasks
    private static void doPreWork_01() {

        // reads configuration properties of the server
        String pathName = "config/Server1.properties";
        Properties properties = Helper.readPropertiesFile(pathName);

        seedServer_01 = Integer.parseInt(properties.getProperty("seedServer"));
        fingerprintPrimeNumber_01 = Integer.parseInt(properties.getProperty("fingerprintPrimeNumber"));

        numRows = Integer.parseInt(properties.getProperty("numRows"));
        numThreads = Integer.parseInt(properties.getProperty("numThreads"));
        numRowsPerThread = numRows / numThreads;

        serverPort_01 = Integer.parseInt(properties.getProperty("serverPort")) + portIncrement;
        combinerPort_01 = Integer.parseInt(properties.getProperty("combinerPort")) + portIncrement;
        combinerIP_01 = properties.getProperty("combinerIP");
    }

    // performs server task required to process client query
    public static void main(String[] args) throws IOException {
        doPreWork_01();
        Server1 server1 = new Server1();
        server1.startServer_01();
    }
}


