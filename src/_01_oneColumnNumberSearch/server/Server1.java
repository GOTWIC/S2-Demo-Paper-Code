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
    private static final String query_base1 = "select ";
    private static final String query_base2 = " from " + Helper.getDatabaseName() + "." + Helper.getTableName() + "_SERVERTABLE1 where rowID > ";

    // the number of row of tpch.lineitem considered
    private static int numRows;
    // the number of threads server program is running on
    private static int numThreads;
    // the number of row per thread
    private static int numRowsPerThread;

    // the fingerprintPrimeNumber value i.e value of r which is taken as 43 in our case
    private static int fingerprintPrimeNumber;
    // the fingerprint value generated for server1
    private static int addShare1;
    // stores seed value for server for random number generation
    private static int seedServer;
    // stores seed value for client for random number generation
    private static int seedClient;
    // the name of the tpch.lineitem column to search over
    private static String columnName;

    // stores result after server processing
    private static int[] result;
    private static HashMap<Integer, Long> hashMap = new HashMap<>();

    private static ArrayList<Instant> timestamps = new ArrayList<>();
    private static final Logger log = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    private static final int portIncrement = 0;


    // stores port for server
    private static int serverPort;
    // stores port for combiner
    private static int combinerPort;
    // stores IP for combiner
    private static String combinerIP;

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
                String query = query_base1 + columnName + query_base2 + startRow + " LIMIT " + numRowsPerThread;
                Statement stmt = con.createStatement();
                ResultSet rs = stmt.executeQuery(query);

                String row;
                String[] rowSplit;
                int prgServer, prgClient;
                Random randSeedServer = new Random(seedServer);
                Random randSeedClient = new Random(seedClient);

                // performing server operation on each row of the database
                for (int i = startRow; i < endRow; i++) {
                    rs.next();
                    prgServer = randSeedServer.nextInt(Constants.getMaxRandomBound() -
                            Constants.getMinRandomBound()) + Constants.getMinRandomBound();
                    prgClient = randSeedClient.nextInt(Constants.getMaxRandomBound() -
                            Constants.getMinRandomBound()) + Constants.getMinRandomBound();

                    result[i] = (int) Helper.mod(Helper.mod((rs.getLong(columnName) - addShare1) *
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
    private static void doWork(String[] data) {

        columnName = "A_" + data[0];
        addShare1 = Integer.parseInt(data[1]);
        seedClient = Integer.parseInt(data[2]);
        result = new int[numRows];

        // the list containing all the threads
        List<Thread> threadList = new ArrayList<>();

        // create threads and add them to threadlist
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
                // Print result
                //System.out.println("Server1 Result: " + Arrays.toString(result));
                outToCombiner.writeObject(result);
                combinerSocket.close();

                // calculating timestamps
                timestamps.add(Instant.now());
//                System.out.println(Helper.getProgramTimes(timestamps));
//                log.log(Level.INFO, "Total Server1 time:" + Helper.getProgramTimes(timestamps));
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
            //System.out.println("Server1 Listening........");

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
        String pathName = "config/Server1.properties";
        Properties properties = Helper.readPropertiesFile(pathName);

        seedServer = Integer.parseInt(properties.getProperty("seedServer"));
        fingerprintPrimeNumber = Integer.parseInt(properties.getProperty("fingerprintPrimeNumber"));

        numRows = Integer.parseInt(properties.getProperty("numRows"));
        numThreads = Integer.parseInt(properties.getProperty("numThreads"));
        numRowsPerThread = numRows / numThreads;

        serverPort = Integer.parseInt(properties.getProperty("serverPort")) + portIncrement;
        combinerPort = Integer.parseInt(properties.getProperty("combinerPort")) + portIncrement;
        combinerIP = properties.getProperty("combinerIP");
    }

    // performs server task required to process client query
    public static void main(String[] args) throws IOException {
        doPreWork();
        Server1 server1 = new Server1();
        server1.startServer();
    }
}


