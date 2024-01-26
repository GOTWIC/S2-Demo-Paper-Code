package src._04_OR_Search.server;

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
import java.math.BigInteger;

import constant.*;

public class Server2 {

    // query string to get server data from database
    private static final String query_base1_04 = "select ";
    private static final String query_base2_04 = " from " + Helper.getDatabaseName() + "." + Helper.getTableName() + "_SERVERTABLE2 where rowID > ";

    // the number of row of tpch.lineitem considered
    private static int numRows;
    // the number of threads server program is running on
    private static int numThreads;
    // the number of row per thread
    private static int numRowsPerThread;


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

    private static ArrayList<Instant> timestamps = new ArrayList<>();
    private static final Logger log_04 = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);


    // stores port for server
    private static int serverPort;
    // stores port for combiner
    private static int combinerPort;
    // stores IP for combiner
    private static String combinerIP;

    private static final int portIncrement = 30;

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

    // executing server operation over threads
    private static void doWork_04(String[] data) throws IOException {

        columnName_04 = Helper.strToStrArr(data[0]);
        multiplicativeShare_04 = Helper.strToBiArr(data[1]);
        columnCount_04 = columnName_04.length;
        seedClient_04 = Integer.parseInt(data[2]);

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

        // Wait for all threads to finish
        for (Thread thread : threadList) {
            try {
                thread.join();
            } catch (InterruptedException ex) {
                log_04.log(Level.SEVERE, ex.getMessage());
            }
        }
        result_04[resultDim][0] = BigInteger.valueOf(2);
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
                doWork_04(dataReceived);
                clientSocket.close();

                // sending the processed data to Combiner
                combinerSocket = new Socket(combinerIP, combinerPort);
                outToCombiner = new ObjectOutputStream(combinerSocket.getOutputStream());
                outToCombiner.writeObject(result_04);
                combinerSocket.close();

                // calculating timestamps
                timestamps.add(Instant.now());
//                System.out.println(Helper.getProgramTimes(timestamps));
//                log.log(Level.INFO, "Total Server2 time:" + Helper.getProgramTimes(timestamps));
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
            //System.out.println("Server2 Listening........");

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
    }

    // performs server task required to process client query
    public static void main(String[] args) throws IOException {

        doPreWork();

        Server2 Server2 = new Server2();
        Server2.startServer();

    }
}


