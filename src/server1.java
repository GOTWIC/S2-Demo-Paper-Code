package src;

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

public class server1 {

    // ---------------- UNIVERSAL SERVER GLOBALS ---------------- \\

    // the number of row of tpch.lineitem considered
    private static int numRows;
    // the number of threads server program is running on
    private static int numThreads;
    // the number of row per thread
    private static int numRowsPerThread;
    private static final int portIncrement = 0;
    // stores port for server
    private static int serverPort;
    // stores port for combiner
    private static int combinerPort;
    // stores IP for combiner
    private static String combinerIP;
    // stores seed value for client for random number generation
    private static int seedClient;
    // the fingerprintPrimeNumber value i.e value of r which is taken as 43 in our case
    private static int fingerprintPrimeNumber;
    // stores seed value for server for random number generation
    private static int seedServer;
    // the name of the tpch.lineitem column to search over
    private static String columnName;
    private static ArrayList<Instant> timestamps = new ArrayList<>();


    // ---------------- 01 SERVER GLOBALS ---------------- \\

    // the fingerprint value generated for server1
    private static int addShare1_01;
    // stores result after server processing
    private static int[] result_01;
    private static HashMap<Integer, Long> hashMap_01 = new HashMap<>();
    private static final Logger log = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    // query string to get server data from database
    private static final String query_base1_01 = "select ";
    private static final String query_base2_01 = " from " + Helper.getDatabaseName() + "." + Helper.getTableName() + "_SERVERTABLE1 where rowID > ";

    // ---------------- 02 SERVER GLOBALS ---------------- \\

    // the fingerprint value generated for server1
    private static int fingerprint1_02;
    // stores result_02 after server processing
    private static int[] result_02;
    private static HashMap<Integer, Long> hashMap_02 = new HashMap<>();
    private static final Logger log_02 = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    private static final String query_base1_02 = "select ";
    private static final String query_base2_02 = " from " + Helper.getDatabaseName() + "." + Helper.getTableName()+  "_SERVERTABLE1 where rowID > ";


    // ---------------- 01 SERVER CODE ---------------- \\

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
                String query = query_base1_01 + columnName + query_base2_01 + startRow + " LIMIT " + numRowsPerThread;
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

                    result_01[i] = (int) Helper.mod(Helper.mod((rs.getLong(columnName) - addShare1_01) *
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

        columnName = "A_" + data[1];
        addShare1_01 = Integer.parseInt(data[2]);
        seedClient = Integer.parseInt(data[3]);
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

    // ---------------- 02 SERVER CODE ---------------- \\

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
                String query = query_base1_02 + columnName + query_base2_02 + startRow + " LIMIT " + numRowsPerThread;
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
                    row = rs.getString(columnName);
                    rowSplit = row.split("\\|");

                    long temp = 0;
                    prgServer = randSeedServer.nextInt(Constants.getMaxRandomBound() -
                            Constants.getMinRandomBound()) + Constants.getMinRandomBound();
                    prgClient = randSeedClient.nextInt(Constants.getMaxRandomBound() -
                            Constants.getMinRandomBound()) + Constants.getMinRandomBound();

                    for (int j = rowSplit.length - 1; j >= 0; j--) {
                        if (!hashMap_02.containsKey(j + 1)) {
                            hashMap_02.put(j + 1, Helper.mod((long) Math.pow(fingerprintPrimeNumber, j + 1)));
                        }
                        temp = Helper.mod(temp + Helper.mod(hashMap_02.get(j + 1)
                                * Integer.parseInt(rowSplit[j])));

                        //System.out.println("val: " + rowSplit[j] +  " temp: " + temp + " hashmapvalue: " + hashMap.get(j + 1));
                    }
                    result_02[i] = (int) Helper.mod(Helper.mod((temp - fingerprint1_02) * prgServer) + prgClient);
                    //System.out.println("prgServer: " + prgServer + " prgClient: " + prgClient + " fingerprint: " + fingerprint1 + " result_02: " + result_02[i]);
                    //System.out.println("\n------------------------------------------\n");
                }
            } catch (SQLException ex) {
                log_02.log(Level.SEVERE, ex.getMessage());
            }
        }
    }

    // executing server operation over threads
    private static void doWork_02(String[] data) {

        columnName = "A_" + data[1];
        fingerprint1_02 = Integer.parseInt(data[2]);
        seedClient = Integer.parseInt(data[3]);
        result_02 = new int[numRows];

        // the list containing all the threads
        List<Thread> threadList = new ArrayList<>();

        // create threads and add them to threadlist
        int threadNum;
        for (int i = 0; i < numThreads; i++) {
            threadNum = i + 1;
            threadList.add(new Thread(new ParallelTask_02(threadNum), "Thread" + threadNum));
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
                log_02.log(Level.SEVERE, ex.getMessage());
            }
        }
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

                //System.out.println("Protocol: " + protocol);

                if(protocol.equals("num")){
                    doWork_01(dataReceived);

                    // sending the processed data to Combiner
                    combinerSocket = new Socket(combinerIP, combinerPort);
                    outToCombiner = new ObjectOutputStream(combinerSocket.getOutputStream());

                    // to help the combiner know which version to run
                    int[] newresult = new int[result_01.length + 1];
                    newresult[0] = 1; 
                    for (int i = 0; i < result_01.length; i++) {
                        newresult[i + 1] = result_01[i];
                    }
                    outToCombiner.writeObject(newresult);
                    combinerSocket.close();
                }

                else if(protocol.equals("str")){
                    doWork_02(dataReceived);

                    // sending the processed data to Combiner
                    combinerSocket = new Socket(combinerIP, combinerPort);
                    outToCombiner = new ObjectOutputStream(combinerSocket.getOutputStream());

                    // to help the combiner know which version to run
                    int[] newresult = new int[result_02.length + 1];
                    newresult[0] = 2; 
                    for (int i = 0; i < result_02.length; i++) {
                        newresult[i + 1] = result_02[i];
                    }
                    outToCombiner.writeObject(newresult);
                    combinerSocket.close();
                }


                // calculating timestamps
                timestamps.add(Instant.now());


                
                
//                System.out.println(Helper.getProgramTimes(timestamps));
//                log.log(Level.INFO, "Total Server1 time:" + Helper.getProgramTimes(timestamps));
            } catch (IOException | ClassNotFoundException ex) {
                log_02.log(Level.SEVERE, ex.getMessage());
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
            log_02.log(Level.SEVERE, ex.getMessage());
        }
    }

    // performs initialization tasks
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

    public static void main(String[] args) throws IOException {

        doPreWork();

        server1 server1 = new server1();
        server1.startServer();

    }

   
}
