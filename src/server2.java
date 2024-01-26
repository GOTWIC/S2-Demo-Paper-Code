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

public class server2 {

    // ALL SERVER GLOBALS

    // the number of row of tpch.lineitem considered
    private static int numRows;
    // the number of threads server program is running on
    private static int numThreads;
    // the number of row per thread
    private static int numRowsPerThread;
    private static final int portIncrement = 0;
    // stores seed value for server for random number generation
    private static int seedServer;
    // the name of the tpch.lineitem column to search over
    private static String columnName;
    private static int serverPort;
    private static int combinerPort;
    private static String combinerIP;
    // the fingerprintPrimeNumber value i.e value of r which is taken as 43 in our case
    private static int fingerprintPrimeNumber;
    private static ArrayList<Instant> timestamps = new ArrayList<>();


    // ---------------- 01 SERVER GLOBALS ---------------- \\

    // query string to get server data from database
    private static final String query_base1_01 = "select ";
    private static final String query_base2_01 = " from " + Helper.getDatabaseName() + "." + Helper.getTableName() + "_SERVERTABLE2 where rowID > ";
    // the fingerprint value generated for server2
    private static int addShare2_01;
    // stores result after server processing
    private static int[] result_01;
    private static HashMap<Integer, Long> hashMap_01 = new HashMap<>();
    private static final Logger log_01 = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    

    // ---------------- 02 SERVER GLOBALS ---------------- \\

    // query string to get server data from database
    private static final String query_base1_02 = "select ";
    private static final String query_base2_02 = " from " + Helper.getDatabaseName() + "." + Helper.getTableName()+  "_SERVERTABLE2 where rowID > ";
    // the fingerprint value generated for server2
    private static int fingerprint2_02;
    // stores result after server processing
    private static int[] result_02;
    private static HashMap<Integer, Long> hashMap_02 = new HashMap<>();
    private static final Logger log_02 = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    // ---------------- 03 SERVER GLOBALS ---------------- \\
    // query string to get server data from database
    private static final String query_base1_03 = "select ";
    private static final String query_base2_03 = " from " + Helper.getDatabaseName() + "." + Helper.getTableName() + "_SERVERTABLE2 where rowID > ";
    // the fingerprint value generated for server2
    private static int fingerprint2_03;
    // the list of name of the tpch.lineitem column to search over
    private static String[] columnName_03;
    // stores result after server processing
    private static int[] result_03;
    private static HashMap<Integer, Long> hashMap_03 = new HashMap<>();
    private static final Logger log_03 = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    static Map<String, Integer> tableMetadata_03 = null;


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
                log_01.log(Level.SEVERE, ex.getMessage());
            }
            int startRow = (threadNum - 1) * numRowsPerThread;
            int endRow = startRow + numRowsPerThread;

            try {
                String query = query_base1_01 + columnName + query_base2_01 + startRow + " LIMIT " + numRowsPerThread;
                Statement stmt = con.createStatement();
                ResultSet rs = stmt.executeQuery(query);

                String row;
                String[] rowSplit;
                int prgServer;
                Random randSeedServer = new Random(seedServer);

                // performing server operation on each row of the database
                for (int i = startRow; i < endRow; i++) {
                    rs.next();
                    prgServer = randSeedServer.nextInt(Constants.getMaxRandomBound() -
                            Constants.getMinRandomBound()) + Constants.getMinRandomBound();

                    result_01[i] = (int) Helper.mod(Helper.mod((rs.getLong(columnName) - addShare2_01) *
                            prgServer));

                    //System.out.println("Row Val: " + rs.getLong(columnName) + " prgServer: " + prgServer + " addShare2: " + addShare2 + " result: " + result[i]);
                }
            } catch (SQLException ex) {
                log_01.log(Level.SEVERE, ex.getMessage());
            }
        }
    }

    // executing server operation over threads
    private static void doWork_01(String[] data) {

        columnName = "A_" + data[1];
        addShare2_01 = Integer.parseInt(data[2]);
        result_01 = new int[numRows];

        // the list containing all the threads
        List<Thread> threadList = new ArrayList<>();

        // create threads and add them to threadlist
        int threadNum;
        for (int i = 0; i < numThreads; i++) {
            threadNum = i + 1;
            threadList.add(new Thread(new ParallelTask_01(threadNum), "Thread" + threadNum));
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
                log_01.log(Level.SEVERE, ex.getMessage());
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
                int prgServer;
                Random randSeedServer = new Random(seedServer);

                // performing server operation on each row of the database
                for (int i = startRow; i < endRow; i++) {
                    rs.next();
                    row = rs.getString(columnName);
                    rowSplit = row.split("\\|");

                    long temp = 0;
                    prgServer = randSeedServer.nextInt(Constants.getMaxRandomBound() -
                            Constants.getMinRandomBound()) + Constants.getMinRandomBound();

                    for (int j = rowSplit.length - 1; j >= 0; j--) {
                        if (!hashMap_02.containsKey(j + 1)) {
                            hashMap_02.put(j + 1, Helper.mod((long) Math.pow(fingerprintPrimeNumber, j + 1)));
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

        columnName = "A_" + data[1];
        fingerprint2_02 = Integer.parseInt(data[2]);
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

    // ---------------- 03 SERVER CODE ---------------- \\

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

                // Make a copy of the column names and add the "A_" prefix to the column names
                String [] columnNameCopy = new String[columnName_03.length];
                for (int i = 0; i < columnName_03.length; i++) {
                    columnNameCopy[i] = "A_" + columnName_03[i];
                }

                String columns = Helper.strArrToStr(columnNameCopy);

                String query = query_base1_03 + columns + query_base2_03 + startRow + " LIMIT " + numRowsPerThread;
                Statement stmt = con.createStatement();
                ResultSet rs = stmt.executeQuery(query);
                int prgServer;

                String[] rowSplit;
                // performing server operation on each row of the database
                for (int i = startRow; i < endRow; i++) {
                    int start = 1;
                    rs.next();
                    prgServer = randSeedServer.nextInt(Constants.getMaxRandomBound() -
                            Constants.getMinRandomBound()) + Constants.getMinRandomBound();

                    // process for each column of string or numeric type
                    for (int k = 0; k < columnName_03.length; k++) {

                        int col_type = getColumnType_03(columnName_03[k]);

                        if (col_type == 0) { // int column
                            if (!hashMap_03.containsKey(start)) {
                                hashMap_03.put(start, Helper.mod((long) Math.pow(fingerprintPrimeNumber, start)));
                            }
                            result_03[i] = (int) Helper.mod(result_03[i] +
                                    Helper.mod(hashMap_03.get(start) * rs.getLong("A_" + columnName_03[k])));
                            start++;
                        } else { // string column
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
                    result_03[i] = (int) Helper.mod(Helper.mod((long) result_03[i] - fingerprint2_03) * prgServer);
                }
            } catch (SQLException ex) {
                log_03.log(Level.SEVERE, ex.getMessage());
            }
        }
    }

    // executing server operation over threads
    private static void doWork_03(String[] data) {

        columnName_03 = Helper.strToStrArr(data[1]);
        fingerprint2_03 = Integer.parseInt(data[2]);
        result_03 = new int[numRows];


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

    private static int getColumnType_03(String col_name){
        if(tableMetadata_03 == null)
            tableMetadata_03 = Helper.getColumnList();
        return tableMetadata_03.get(col_name.toLowerCase());
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

                else if(protocol.equals("and")){

                    doWork_03(dataReceived);

                    // sending the processed data to Combiner
                    combinerSocket = new Socket(combinerIP, combinerPort);
                    outToCombiner = new ObjectOutputStream(combinerSocket.getOutputStream());
                    // to help the combiner know which version to run
                    int[] newresult = new int[result_03.length + 1];
                    newresult[0] = 3; 
                    for (int i = 0; i < result_03.length; i++) {
                        newresult[i + 1] = result_03[i];
                    }
                    outToCombiner.writeObject(newresult);
                    combinerSocket.close();
                }
                                   

                // calculating timestamps
                timestamps.add(Instant.now());
                //System.out.println(Helper.getProgramTimes(timestamps));
                //log.log(Level.INFO, "Total Server2 time:" + Helper.getProgramTimes(timestamps));
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
            //System.out.println("Server2 Listening........");

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
        String pathName = "config/Server2.properties";
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

        server2 server2 = new server2();
        server2.startServer();

    }
    
}
