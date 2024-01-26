package src._02_oneColumnStringSearch.server;

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

public class Server2 {

    // query string to get server data from database
    private static final String query_base1_02 = "select ";
    private static final String query_base2_02 = " from " + Helper.getDatabaseName() + "." + Helper.getTableName()+  "_SERVERTABLE2 where rowID > ";

    // the number of row of tpch.lineitem considered
    private static int numRows;
    // the number of threads server program is running on
    private static int numThreads;
    // the number of row per thread
    private static int numRowsPerThread;

    // the fingerprintPrimeNumber value i.e value of r which is taken as 43 in our case
    private static int fingerprintPrimeNumber_02;
    // the fingerprint value generated for server2
    private static int fingerprint2_02;
    // stores seed value for server for random number generation
    private static int seedServer_02;
    // the name of the tpch.lineitem column to search over
    private static String columnName_02;

    // stores result after server processing
    private static int[] result_02;
    private static HashMap<Integer, Long> hashMap_02 = new HashMap<>();

    private static ArrayList<Instant> timestamps_02 = new ArrayList<>();
    private static final Logger log_02 = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);


    private static int serverPort_02;
    private static int combinerPort_02;
    private static String combinerIP_02;

    private static final int portIncrement = 10;

    // operation performed by each thread
    private static class ParallelTask_02 implements Runnable {

        private final int threadNum;

        public ParallelTask_02(int threadNum) {
            this.threadNum = threadNum;
        }

        @Override
        public void run() {
            // making connection to the database
            Connection con = null;
            try {
                con = Helper.getConnection();
            } catch (SQLException ex) {
                log_02.log(Level.SEVERE, ex.getMessage());
            }
            int startRow = (threadNum - 1) * numRowsPerThread;
            int endRow = startRow + numRowsPerThread;

            try {
                String query = query_base1_02 + columnName_02 + query_base2_02 + startRow + " LIMIT " + numRowsPerThread;
                Statement stmt = con.createStatement();
                ResultSet rs = stmt.executeQuery(query);

                String row;
                String[] rowSplit;
                int prgServer;
                Random randSeedServer = new Random(seedServer_02);

                // performing server operation on each row of the database
                for (int i = startRow; i < endRow; i++) {
                    rs.next();
                    row = rs.getString(columnName_02);
                    rowSplit = row.split("\\|");

                    long temp = 0;
                    prgServer = randSeedServer.nextInt(Constants.getMaxRandomBound() -
                            Constants.getMinRandomBound()) + Constants.getMinRandomBound();

                    for (int j = rowSplit.length - 1; j >= 0; j--) {
                        if (!hashMap_02.containsKey(j + 1)) {
                            hashMap_02.put(j + 1, Helper.mod((long) Math.pow(fingerprintPrimeNumber_02, j + 1)));
                        }
                        temp = Helper.mod(temp + Helper.mod(hashMap_02.get(j + 1) * Integer.parseInt(rowSplit[j])));
                        //System.out.println("val: " + rowSplit[j] +  " temp: " + temp + " hashmapvalue: " + hashMap.get(j + 1));
                    }
                    result_02[i] = (int) Helper.mod((temp - fingerprint2_02) * prgServer);
                    //System.out.println("prgServer: " + prgServer + " fingerprint: " + fingerprint2 + " result: " + result[i]);
                    //System.out.println("\n------------------------------------------\n");
                }
            } catch (SQLException ex) {
                log_02.log(Level.SEVERE, ex.getMessage());
            }
        }
    }

    // executing server operation over threads
    private static void doWork_02(String[] data) {

        columnName_02 = "A_" + data[0];
        fingerprint2_02 = Integer.parseInt(data[1]);
        result_02 = new int[numRows];

        // the list containing all the threads
        List<Thread> threadList = new ArrayList<>();

        // create threads and add them to threadlist
        int threadNum;
        for (int i = 0; i < numThreads; i++) {
            threadNum = i + 1;
            threadList.add(new Thread(new ParallelTask_02(threadNum), "Thread" + threadNum));
        }

        // ctart all threads
        for (int i = 0; i < numThreads; i++) {
            threadList.get(i).start();
        }

        // wait for all threads to finish
        for (Thread thread : threadList) {
            try {
                thread.join();
            } catch (InterruptedException ex) {
                log_02.log(Level.SEVERE, ex.getMessage());
            }
        }
    }

    // performing operations on data received over socket
    static class SocketCreation_02 {

        private final Socket clientSocket;


        SocketCreation_02(Socket clientSocket) {
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
                doWork_02(dataReceived);

                // sending the processed data to Combiner
                combinerSocket = new Socket(combinerIP_02, combinerPort_02);
                outToCombiner = new ObjectOutputStream(combinerSocket.getOutputStream());
                outToCombiner.writeObject(result_02);
                combinerSocket.close();

                // calculating timestamps
                timestamps_02.add(Instant.now());
                //System.out.println(Helper.getProgramTimes(timestamps));
                //log.log(Level.INFO, "Total Server2 time:" + Helper.getProgramTimes(timestamps));
            } catch (IOException | ClassNotFoundException ex) {
                log_02.log(Level.SEVERE, ex.getMessage());
            }
        }
    }

    // starting server to listening for incoming connection
    private void startServer_02() throws IOException {
        Socket socket;

        try {
            ServerSocket ss = new ServerSocket(serverPort_02);
            //System.out.println("Server2 Listening........");

            do {
                // listening over socket for connections
                socket = ss.accept();
                timestamps_02 = new ArrayList<>();
                timestamps_02.add(Instant.now());
                new SocketCreation_02(socket).run();
            } while (true);
        } catch (IOException ex) {
            log_02.log(Level.SEVERE, ex.getMessage());
        }
    }

    // performs initialization tasks
    private static void doPreWork_02() {

        // reads configuration properties of the server
        String pathName = "config/Server2.properties";
        Properties properties = Helper.readPropertiesFile(pathName);

        seedServer_02 = Integer.parseInt(properties.getProperty("seedServer"));
        fingerprintPrimeNumber_02 = Integer.parseInt(properties.getProperty("fingerprintPrimeNumber"));

        numRows = Integer.parseInt(properties.getProperty("numRows"));
        numThreads = Integer.parseInt(properties.getProperty("numThreads"));
        numRowsPerThread = numRows / numThreads;

        serverPort_02 = Integer.parseInt(properties.getProperty("serverPort")) + portIncrement;
        combinerPort_02 = Integer.parseInt(properties.getProperty("combinerPort")) + portIncrement;
        combinerIP_02 = properties.getProperty("combinerIP");
    }

    // performs server task required to process client query
    public static void main(String[] args) throws IOException {

        doPreWork_02();

        Server2 Server2 = new Server2();
        Server2.startServer_02();

    }
}


