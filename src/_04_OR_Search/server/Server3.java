package src._04_OR_Search.server;

import utility.Helper;

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

public class Server3 {

    // query string to get server data from database
    private static final String query_base1 = "select ";
    private static final String query_base2 = " from " + Helper.getDatabaseName() + "." + Helper.getTableName() + "_SERVERTABLE3 where rowID > ";

    // the number of row of tpch.lineitem considered
    private static int numRows;
    // the number of threads server program is running on
    private static int numThreads;
    // the number of row per thread
    private static int numRowsPerThread;


    // stores multiplicative share for search key values
    private static BigInteger[] multiplicativeShare;
    // stores seed value for client for random number generation
    private static int seedClient;
    // the list of name of the tpch.lineitem column to search over
    private static String[] columnName;
    // number of columns
    private static int columnCount;

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

    private static final int portIncrement = 30;

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

            Random randClient = new Random(seedClient);
            String columns = Helper.strArrToStr(columnName);

            try {
                String query = query_base1 + columns + query_base2 + startRow + " LIMIT " + numRowsPerThread;
                Statement stmt = con.createStatement();
                ResultSet rs = stmt.executeQuery(query);

                // performing server operation on each row of the database
                for (int i = startRow; i < endRow; i++) {
                    rs.next();

                    int randSeedClient = randClient.nextInt(Constants.getMaxRandomBound() - Constants.getMinRandomBound())
                            + Constants.getMinRandomBound();
                    BigInteger product1 = new BigInteger("1"), product2 = new BigInteger("1");

                    if (columnCount > 3) { // runs for server count =4
                        for (int j = 0; j < (columnName.length / 2); j++) {
                            product1 = Helper.mod(product1.multiply(Helper.mod(new BigInteger(rs.getString(columnName[j])).subtract(multiplicativeShare[j]))));
                            product2 = Helper.mod(product2.multiply(Helper.mod(new BigInteger(rs.getString(columnName[j + 2])).subtract(multiplicativeShare[j + 2]))));
                        }
                    } else { // runs for server count<= 3
                        for (int j = 0; j < columnName.length; j++) {
                            product1 = Helper.mod(product1.multiply(Helper.mod(new BigInteger(rs.getString(columnName[j])).subtract(multiplicativeShare[j]))));
                        }
                    }
                    result[0][i] = Helper.mod(product1.add(BigInteger.valueOf(randSeedClient)));
                    if (columnCount > 3)
                        result[1][i] = Helper.mod(product2.add(BigInteger.valueOf(randSeedClient)));
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

    // executing server operation over thread
    private static void doWork(String[] data) throws IOException {

        columnName = Helper.strToStrArr(data[0]);
        multiplicativeShare = Helper.strToBiArr(data[1]);
        columnCount = columnName.length;
        seedClient = Integer.parseInt(data[2]);

        int resultDim = 1;
        if (columnCount > 3) {
            resultDim = 2;
        }

        result = new BigInteger[resultDim + 1][numRows];

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
        result[resultDim][0] = BigInteger.valueOf(3);
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
                clientSocket.close();

                // sending the processed data to Combiner
                combinerSocket = new Socket(combinerIP, combinerPort);
                outToCombiner = new ObjectOutputStream(combinerSocket.getOutputStream());
                outToCombiner.writeObject(result);
                combinerSocket.close();

                // calculating timestamps
                timestamps.add(Instant.now());
//                System.out.println(Helper.getProgramTimes(timestamps));
//                log.log(Level.INFO, "Total Server3 time:" + Helper.getProgramTimes(timestamps));
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
            //System.out.println("Server3 Listening........");

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
        String pathName = "config/Server3.properties";
        Properties properties = Helper.readPropertiesFile( pathName);

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

        Server3 Server3 = new Server3();
        Server3.startServer();

    }
}


