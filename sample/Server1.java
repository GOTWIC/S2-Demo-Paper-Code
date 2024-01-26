package src._03_AND_Search.server;

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

import constant.*;

public class Server1 {

    private static String query_base1;
    private static String query_base2;
    private static String query_base3;

    private static int numRows;
    private static int numThreads;
    private static int numRowsPerThread;

    private static int fingerprintPrimeNumber;
    private static int fingerprint1;
    private static int seedServer;
    private static int seedClient;
    private static String[] columnName;

    private static int[] multiplicativeShare;
    private static int columnCount;


    private static int[][] resultAnd;
    private static int[][] resultOr;


    private static HashMap<Integer, Long> hashMap = new HashMap<>();

    private static ArrayList<Instant> timestamps = new ArrayList<>();
    private static final Logger log = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);


    private static int serverPort;
    private static int combinerPort;
    private static String combinerIP;
    private static String protocol;

    private static String tableName;
    private static int noOfColumns;
    private static LinkedHashMap<String, String> columnhash = new LinkedHashMap<>();


    private static int querySize;
    private static int filter_size;
    private static int[][] row_filter;
    private static int[][] col_filter;

    private static int sum[][][][];
    private static int total[][];
    private static int sumString[][][][][];
    private static int totalString[][][];
    private static int countString;
    private static int countNumber;
    private static int[][][] resultRow;
    private static List<String> columList;
    private static int blocks;

    private static int[][] resultSum;
    private static int[] shareRowId;
    private static String aggregateAttribute;

    private static class ParallelTask implements Runnable {

        private final int threadNum;

        public ParallelTask(int threadNum) {
            this.threadNum = threadNum;
        }

        @Override
        public void run() {
            Connection con = null;
            try {
                con = Helper.getConnection();
            } catch (SQLException ex) {
                log.log(Level.SEVERE, ex.getMessage());
            }
            int startRow = (threadNum - 1) * numRowsPerThread;
            int endRow = startRow + numRowsPerThread;

            try {
                Random randSeedServer = new Random(seedServer);
                Random randSeedClient = new Random(seedClient);

                String columns = Helper.strArrToStr(columnName);

                String query = query_base1 + columns + query_base2 + startRow + " LIMIT " + numRowsPerThread;
                Statement stmt = con.createStatement();
                ResultSet rs = stmt.executeQuery(query);
                int prgServer, prgClient;

                String[] rowSplit;
                for (int i = startRow; i < endRow; i++) {
                    int start = 1;
                    rs.next();
                    prgServer = randSeedServer.nextInt(Constants.getMaxRandomBound() -
                            Constants.getMinRandomBound()) + Constants.getMinRandomBound();
                    prgClient = randSeedClient.nextInt(Constants.getMaxRandomBound() -
                            Constants.getMinRandomBound()) + Constants.getMinRandomBound();


                    for (int k = 0; k < columnName.length; k++) {
                        if (columnhash.get(columnName[k]).equalsIgnoreCase("string")) {
                            rowSplit = rs.getString(columnName[k]).split("\\|");

                            for (int j = 0; j < rowSplit.length; j++) {
                                if (!hashMap.containsKey(start)) {
                                    hashMap.put(start, Helper.mod((long) Math.pow(fingerprintPrimeNumber, start)));
                                }
                                resultAnd[1][i] = (int) Helper.mod(resultAnd[1][i] +
                                        Helper.mod(hashMap.get(start) * Integer.parseInt(rowSplit[j])));
                                start++;
                            }
                        } else if (!columnhash.get(columnName[k]).equalsIgnoreCase("string")) {
                            if (!hashMap.containsKey(start)) {
                                hashMap.put(start, Helper.mod((long) Math.pow(fingerprintPrimeNumber, start)));
                            }
                            resultAnd[1][i] = (int) Helper.mod(resultAnd[1][i] +
                                    Helper.mod(hashMap.get(start) * rs.getLong(columnName[k])));
                            start++;
                        }
                    }
                    resultAnd[1][i] = (int) Helper.mod(Helper.mod(Helper.mod((long) resultAnd[1][i] - fingerprint1)
                            * prgServer) + prgClient);
                }
            } catch (SQLException ex) {
                log.log(Level.SEVERE, ex.getMessage());
            }
        }
    }

    private static void doWorkAnd(String[] data) {
        columnName = Helper.strToStrArr(data[1]);
        fingerprint1 = Integer.parseInt(data[2]);
        seedClient = Integer.parseInt(data[3]);
        resultAnd = new int[2][numRows];
        resultAnd[0][0] = 0;
        hashMap = new HashMap<>();


        // The list containing all the threads
        List<Thread> threadList = new ArrayList<>();

        // Create threads and add them to threadlist
        int threadNum;
        for (int i = 0; i < numThreads; i++) {
            threadNum = i + 1;
            threadList.add(new Thread(new ParallelTask(threadNum), "Thread" + threadNum));
        }

        // Start all threads
        for (int i = 0; i < numThreads; i++) {
            threadList.get(i).start();
        }

        // Wait for all threads to finish
        for (Thread thread : threadList) {
            try {
                thread.join();
            } catch (InterruptedException ex) {
                log.log(Level.SEVERE, ex.getMessage());
            }
        }
    }

    private static class ParallelTaskOr implements Runnable {

        private final int threadNum;

        public ParallelTaskOr(int threadNum) {
            this.threadNum = threadNum;
        }

        @Override
        public void run() {
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

                for (int i = startRow; i < endRow; i++) {
                    rs.next();

                    int randSeedClient = randClient.nextInt(Constants.getMaxRandomBound() - Constants.getMinRandomBound())
                            + Constants.getMinRandomBound();
                    int product1 = 1, product2 = 1;

                    if (columnCount > 3) {
                        for (int j = 0; j < (columnName.length / 2); j++) {
                            product1 = (int) Helper.mod(product1 * Helper.mod(rs.getLong(columnName[j]) - multiplicativeShare[j]));
                            product2 = (int) Helper.mod(product2 * Helper.mod(rs.getLong(columnName[j + 2]) - multiplicativeShare[j + 2]));
                        }
                    } else {
                        for (int j = 0; j < columnName.length; j++) {
                            product1 = (int) Helper.mod(product1 * Helper.mod(rs.getLong(columnName[j]) - multiplicativeShare[j]));
                        }
                    }
                    resultOr[1][i] = Helper.mod(product1 + randSeedClient);
                    if (columnCount > 3)
                        resultOr[2][i] = Helper.mod(product2 + randSeedClient);
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

    private static void doWorkOr(String[] data) throws IOException {

        columnName = Helper.strToStrArr(data[1]);
        multiplicativeShare = Helper.strToArr(data[2]);
        columnCount = columnName.length;
        seedClient = Integer.parseInt(data[3]);

        int resultDim = 1;
        if (columnCount > 3) {
            resultDim = 2;
        }

        resultOr = new int[resultDim + 2][numRows];
        resultOr[0][0] = 1;

        // The list containing all the threads
        List<Thread> threadList = new ArrayList<>();

        // Create threads and add them to threadlist
        int threadNum;
        for (int i = 0; i < numThreads; i++) {
            threadNum = i + 1;
            threadList.add(new Thread(new ParallelTaskOr(threadNum), "Thread" + threadNum));
        }

        // Start all threads
        for (int i = 0; i < numThreads; i++) {
            threadList.get(i).start();
        }

        // Wait for all threads to finish
        for (Thread thread : threadList) {
            try {
                thread.join();
            } catch (InterruptedException ex) {
                log.log(Level.SEVERE, ex.getMessage());
            }
        }
        resultOr[resultOr.length - 1][0] = 1;
    }

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
                //Reading the data sent by Client
                inFromClient = new ObjectInputStream(clientSocket.getInputStream());
                dataReceived = (String[]) inFromClient.readObject();
                protocol = dataReceived[0];
                if (protocol.equals("and") || protocol.equals("single")) {
                    doWorkAnd(dataReceived);
                    clientSocket.close();

                    //Sending the processed data to Combiner
                    combinerSocket = new Socket(combinerIP, combinerPort);
                    outToCombiner = new ObjectOutputStream(combinerSocket.getOutputStream());

                    outToCombiner.writeObject((resultAnd));
                    combinerSocket.close();

                    //Calculating timestamps
                    timestamps.add(Instant.now());
//                    System.out.println("Total time taken at Server1:" + Helper.getProgramTimes(timestamps)+" ms.");
                } else if (protocol.equals("or")) {
                    doWorkOr(dataReceived);
                    clientSocket.close();

                    //Sending the processed data to Combiner
                    combinerSocket = new Socket(combinerIP, combinerPort);
                    outToCombiner = new ObjectOutputStream(combinerSocket.getOutputStream());
                    outToCombiner.writeObject(resultOr);
                    combinerSocket.close();

                    //Calculating timestamps
                    timestamps.add(Instant.now());
//                    System.out.println("Total time taken at Server1:"+Helper.getProgramTimes(timestamps)+" ms.");
                } else if (protocol.equals("*")) {
                    doWorkRow(dataReceived);

                    //Sending the processed data to Combiner
                    combinerSocket = new Socket(combinerIP, combinerPort);
                    outToCombiner = new ObjectOutputStream(combinerSocket.getOutputStream());
                    outToCombiner.writeObject(resultRow);
                    combinerSocket.close();

                    //Calculating timestamps
                    timestamps.add(Instant.now());
//                    System.out.println("Total time taken at Server1:"+Helper.getProgramTimes(timestamps)+" ms.");

                } else if (protocol.contains("sum")) {
                    doWorkSum(dataReceived);

                    //Sending the processed data to Combiner
                    combinerSocket = new Socket(combinerIP, combinerPort);
                    outToCombiner = new ObjectOutputStream(combinerSocket.getOutputStream());
                    outToCombiner.writeObject(resultSum);
                    combinerSocket.close();

                    //Calculating timestamps
                    timestamps.add(Instant.now());
//                    System.out.println("Total time taken at Server1:"+Helper.getProgramTimes(timestamps));

                }

//                System.out.println("Completed data processing.");
//                log.log(Level.INFO, "Total Server1 time:" + Helper.getProgramTimes(timestamps));
            } catch (IOException | ClassNotFoundException ex) {
                log.log(Level.SEVERE, ex.getMessage());
            }
        }
    }

    private static void doWorkSum(String[] data) {

        shareRowId = Helper.strToArr(data[1]);
        seedClient = Integer.parseInt(data[2]);
        aggregateAttribute = data[3];

        resultSum = new int[3][numRows];
        resultSum[0][0] = 3;


        // The list containing all the threads
        List<Thread> threadList = new ArrayList<>();

        // Create threads and add them to threadlist
        int threadNum;
        for (int i = 0; i < numThreads; i++) {
            threadNum = i + 1;
            threadList.add(new Thread(new ParallelTaskSum(threadNum), "Thread" + threadNum));
        }

        // Start all threads
        for (int i = 0; i < numThreads; i++) {
            threadList.get(i).start();
        }

        // Wait for all threads to finish
        for (Thread thread : threadList) {
            try {
                thread.join();
            } catch (InterruptedException ex) {
                log.log(Level.SEVERE, ex.getMessage());
            }
        }

        resultSum[resultSum.length - 1][0] = 1;
    }

    private static class ParallelTaskSum implements Runnable {

        private final int threadNum;

        public ParallelTaskSum(int threadNum) {
            this.threadNum = threadNum;
        }

        @Override
        public void run() {
            Connection con = null;

            try {
                con = Helper.getConnection();
            } catch (SQLException ex) {
                log.log(Level.SEVERE, ex.getMessage());
            }

            int startRow = (threadNum - 1) * numRowsPerThread;
            int endRow = startRow + numRowsPerThread;

            try {

                String query = query_base1 + "m_" + aggregateAttribute + query_base2 + startRow + " LIMIT " + numRowsPerThread;
                Statement stmt = con.createStatement();
                ResultSet rs = stmt.executeQuery(query);

                Random randSeedClient = new Random(seedClient);
                int prgClient;

                for (int i = startRow; i < endRow; i++) {
                    rs.next();
                    prgClient = randSeedClient.nextInt(Constants.getMaxRandomBound() -
                            Constants.getMinRandomBound()) + Constants.getMinRandomBound();
                    resultSum[1][i] = (int) Helper.mod((Helper.mod(rs.getLong("m_" + aggregateAttribute) * (long) shareRowId[i])) + prgClient);
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

    private static void doWorkRow(String[] data) {

        row_filter = Helper.strToStrArr1(data[1]);
        col_filter = Helper.strToStrArr1(data[2]);
        seedClient = Integer.parseInt(data[3]);

        querySize = row_filter.length;
        resultRow = new int[querySize + 2][noOfColumns][];


        sum = new int[querySize][numThreads][filter_size][countNumber];
        total = new int[querySize][countNumber];
        sumString = new int[querySize][numThreads][filter_size][countString][];
        totalString = new int[querySize][countString][];

        // The list containing all the threads
        List<Thread> threadList = new ArrayList<>();

        // Create threads and add them to threadlist
        int threadNum;
        for (int i = 0; i < numThreads; i++) {
            threadNum = i + 1;
            threadList.add(new Thread(new ParallelTaskRow(threadNum), "Thread" + threadNum));
        }

        // Start all threads
        for (int i = 0; i < numThreads; i++) {
            threadList.get(i).start();
        }

        // Wait for all threads to finish
        for (Thread thread : threadList) {
            try {
                thread.join();
            } catch (InterruptedException ex) {
                log.log(Level.SEVERE, ex.getMessage());
            }
        }

        Random randSeedClient = new Random(seedClient);

        for (int i = 0; i < querySize; i++) {
            int randClient = randSeedClient.nextInt(Constants.getMaxRandomBound() - Constants.getMinRandomBound())
                    + Constants.getMinRandomBound();
            int colN = 0, colS = 0;
            for (int l = 0; l < noOfColumns; l++) {
                resultRow[i + 1][l] = new int[blocks];
                if (!columnhash.get(columList.get(l)).equals("string")) {
                    resultRow[i + 1][l][0] = (int) Helper.mod(total[i][colN] + (long) randClient);
                    colN++;
                } else if (columnhash.get(columList.get(l)).equals("string")) {
                    for (int m = 0; m < blocks; m++) {
                        resultRow[i + 1][l][m] = (int) Helper.mod(totalString[i][colS][m] + (long) randClient);
                    }
                    colS++;
                }
            }
        }


        resultRow[0][0] = new int[blocks];
        resultRow[0][0][0] = 2;
        resultRow[resultRow.length - 1][0] = new int[blocks];
        resultRow[resultRow.length - 1][0][0] = 1;
    }

    private static class ParallelTaskRow implements Runnable {

        private final int threadNum;

        public ParallelTaskRow(int threadNum) {
            this.threadNum = threadNum;
        }

        @Override
        public void run() {
            Connection con = null;

            try {
                con = Helper.getConnection();
            } catch (SQLException ex) {
                log.log(Level.SEVERE, ex.getMessage());
            }

            int startRow = (threadNum - 1) * numRowsPerThread;
            int endRow = startRow + numRowsPerThread;

            try {

                String query = query_base3 + startRow + " LIMIT " + numRowsPerThread;
                Statement stmt = con.createStatement();
                ResultSet rs = stmt.executeQuery(query);


                String[] rowSplit;
                int colN, colS;
                for (int i = startRow; i < endRow; i++) {
                    rs.next();
                    for (int k = 0; k < querySize; k++) {
                        colN = 0;
                        colS = 0;
                        for (int l = 0; l < noOfColumns; l++) {
                            if (!columnhash.get(columList.get(l)).equals("string")) {
                                sum[k][threadNum - 1][i % filter_size][colN] = (int) Helper.mod(sum[k][threadNum - 1][i % filter_size][colN] + Helper.mod(rs.getLong("m_" + columList.get(l)) * row_filter[k][i / filter_size]));
                                colN++;
                            } else if (columnhash.get(columList.get(l)).equals("string")) {
//
                                rowSplit = rs.getString("m_" + columList.get(l)).split("\\|");

                                if (sumString[k][threadNum - 1][i % filter_size][colS] == null) {
                                    blocks = rowSplit.length;
                                    sumString[k][threadNum - 1][i % filter_size][colS] = new int[blocks];
                                }

                                for (int m = 0; m < rowSplit.length; m++) {
                                    sumString[k][threadNum - 1][i % filter_size][colS][m] = (int) Helper.mod(sumString[k][threadNum - 1][i % filter_size][colS][m] + Helper.mod(Long.parseLong(rowSplit[m]) * row_filter[k][i / filter_size]));
                                }
                                colS++;
                            }
                        }
                    }
                }

                for (int i = 0; i < querySize; i++) {
                    for (int k = 0; k < filter_size; k++) {
                        colN = 0;
                        colS = 0;
                        for (int l = 0; l < noOfColumns; l++) {
                            if (!columnhash.get(columList.get(l)).equals("string")) {
                                sum[i][threadNum - 1][k][colN] = (int) Helper.mod(sum[i][threadNum - 1][k][colN] * (long) col_filter[i][k]);
                                total[i][colN] = (int) Helper.mod(total[i][colN] + (long) sum[i][threadNum - 1][k][colN]);
                                colN++;
                            } else if (columnhash.get(columList.get(l)).equals("string")) {
                                if (totalString[i][colS] == null) {
                                    totalString[i][colS] = new int[blocks];
                                }

                                for (int m = 0; m < blocks; m++) {
                                    sumString[i][threadNum - 1][k][colS][m] = (int) Helper.mod(sumString[i][threadNum - 1][k][colS][m] * (long) col_filter[i][k]);
                                    totalString[i][colS][m] = (int) Helper.mod(totalString[i][colS][m] + (long) sumString[i][threadNum - 1][k][colS][m]);
                                }
                                colS++;
                            }
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


    private void startServer() throws IOException {
        Socket socket;

        try {
            ServerSocket ss = new ServerSocket(serverPort);
            System.out.println("Server1 Listening........");

            do {
                socket = ss.accept();
                timestamps = new ArrayList<>();
                timestamps.add(Instant.now());
                new SocketCreation(socket).run();
            } while (true);
        } catch (IOException ex) {
            log.log(Level.SEVERE, ex.getMessage());
        }
    }

    private static void doPreWork() {

        //TODO: remove hardcoding of query.
        String pathName = "config/Server1.properties";
        Properties properties = Helper.readPropertiesFile(pathName);

        seedServer = Integer.parseInt(properties.getProperty("seedServer"));
        fingerprintPrimeNumber = Integer.parseInt(properties.getProperty("fingerprintPrimeNumber"));

        numRows = Integer.parseInt(properties.getProperty("numRows"));
        numThreads = Integer.parseInt(properties.getProperty("numThreads"));
        numRowsPerThread = numRows / numThreads;

        serverPort = Integer.parseInt(properties.getProperty("serverPort"));
        combinerPort = Integer.parseInt(properties.getProperty("combinerPort"));
        combinerIP = properties.getProperty("combinerIP");

        Helper.readMeta();
        tableName = Helper.getTableName();
        noOfColumns = Helper.getNoOfColumns();
        columnhash = Helper.getColumnList();
        for (Map.Entry<String, String> mapElement : columnhash.entrySet()) {
            if (mapElement.getValue().equals("string"))
                countString++;
        }
        countNumber = noOfColumns - countString;
        columList = new ArrayList<>(columnhash.keySet());
        query_base1 = "select ";
//        query_base2 = " from " + Helper.getTablePrefix() + " "+ tableName +" where rowID > ";
        query_base2 = " from " + Helper.getTablePrefix() + "SERVERTABLE1 where rowID > ";
        query_base3 = "select " + "m_" + String.join(",m_", columnhash.keySet()) + " from "
                + Helper.getTablePrefix() + "SERVERTABLE1 where rowID > ";

        filter_size = (int) Math.sqrt(numRows);
    }

    public static void main(String[] args) throws IOException {

        doPreWork();

        Server1 server1 = new Server1();
        server1.startServer();

    }
}


