package src._03_AND_Search.combiner;

import constant.*;
import utility.Helper;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Combiner extends Thread {
    private static List<int[][]> serverResult = Collections.synchronizedList(new ArrayList<>());
    private static final ExecutorService threadPool = Executors.newFixedThreadPool(Constants.getThreadPoolSize());
    private static List<int[][][]> serverResultRow = Collections.synchronizedList(new ArrayList<>());
    private static List<SocketCreationRow> socketCreationsRow = new ArrayList<>();
    private static List<SocketCreation> socketCreations = new ArrayList<>();
    private static int[] resultAnd;
    private static int[][] resultOr;

    private static int numRows;
    private static int numThreads;
    private static int numRowsPerThread;

    private static int combinerPort;
    private static int clientPort;
    private static String clientIP;
    private static int serverCount;


    private static int[][] server1;
    private static int[][] server2;
    private static int[][] server3;
    private static int[][] server4;

    private static int[][][] serverRow1;
    private static int[][][] serverRow2;
    private static int[][][] serverRow3;
    private static int[][][] serverRow4;


    private static final Logger log = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    private static ArrayList<Instant> timestamps = new ArrayList<>();

    private static boolean flag = true;
    private static boolean flag1 = false;

    private static int querySize;
    private static int[][][] resultRow;

    private static int noOfColumns;
    private static LinkedHashMap<String, String> columnhash = new LinkedHashMap<>();
    private static List<String> columList;

    private static int[] resultSum;

    private static class ParallelTask implements Runnable {
        private int threadNum;

        public ParallelTask(int threadNum) {
            this.threadNum = threadNum;
        }

        @Override
        public void run() {
            int startRow = (threadNum - 1) * numRowsPerThread;
            int endRow = startRow + numRowsPerThread;
            for (int i = startRow; i < endRow; i++) {

                resultAnd[i] = ((int) Helper.mod((long) server1[1][i] + (long) server2[1][i]));
            }
        }
    }

    private static void doWorkAnd() {
        // The list containing all the threads

        server1 = serverResult.get(0);
        server2 = serverResult.get(1);
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
                ex.printStackTrace();
            }
        }
    }

    class SocketCreation implements Runnable {

        private final Socket serverSocket;

        SocketCreation(Socket serverSocket) {
            this.serverSocket = serverSocket;
        }

        @Override
        public void run() {
            ObjectInputStream inFromServer;
            try {
                inFromServer = new ObjectInputStream(serverSocket.getInputStream());
                serverResult.add((int[][]) inFromServer.readObject());
            } catch (IOException ex) {
                Logger.getLogger(Combiner.class.getName()).log(Level.SEVERE, null, ex);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }

    class SocketCreationRow implements Runnable {

        private final Socket serverSocket;

        SocketCreationRow(Socket serverSocket) {
            this.serverSocket = serverSocket;
        }

        @Override
        public void run() {
            ObjectInputStream inFromServer;
            try {
                inFromServer = new ObjectInputStream(serverSocket.getInputStream());
                serverResultRow.add((int[][][]) inFromServer.readObject());
            } catch (IOException ex) {
                Logger.getLogger(Combiner.class.getName()).log(Level.SEVERE, null, ex);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void run() {
        startCombiner();
        super.run();
    }

    private void startCombiner() {
        Socket serverSocket;
        Socket clientSocket;
        ArrayList<Future> serverJobs = new ArrayList<>();

        try {
            ServerSocket ss = new ServerSocket(combinerPort);
            System.out.println("Combiner Listening........");

            while (true) {
                //Reading data from the server
                serverSocket = ss.accept();

                if (flag) {
                    ObjectInputStream inFromServer = new ObjectInputStream(serverSocket.getInputStream());
                    String[] combinerData = (String[]) inFromServer.readObject();
                    serverCount = Integer.parseInt(combinerData[0]);
                    flag1 = Boolean.parseBoolean(combinerData[1]);
                    flag = false;
                    serverSocket.close();
                } else {
                    if (!flag1) {
                        socketCreations.add(new SocketCreation(serverSocket));
                    } else {
                        socketCreationsRow.add(new SocketCreationRow(serverSocket));
                    }
                }

                //Processing data received from both the servers
                if (socketCreations.size() == serverCount) {
                    timestamps = new ArrayList<>();
                    timestamps.add(Instant.now());
                    for (SocketCreation socketCreation : socketCreations) {
                        serverJobs.add(threadPool.submit(socketCreation));
                    }
                    for (Future<?> future : serverJobs)
                        future.get();

                    if (serverResult.get(0)[0][0] == 0) {
                        doWorkAnd();
                        //Sending data from the client
                        clientSocket = new Socket(clientIP, clientPort);
                        ObjectOutputStream outToClient = new ObjectOutputStream(clientSocket.getOutputStream());
                        outToClient.writeObject(resultAnd);
                        clientSocket.close();
                        //Resetting storage variables
                        resultAnd = new int[numRows];
                        serverJobs = new ArrayList<>();
                        serverResult = Collections.synchronizedList(new ArrayList<>());
                        socketCreations = new ArrayList<>();
                        flag = true;

                        //Calculating the time spent
                        timestamps.add(Instant.now());
//                        System.out.println(Helper.getProgramTimes(timestamps));
//                    log.log(Level.INFO, "Total Combiner time:" + Helper.getProgramTimes(timestamps));
                    } else if (serverResult.get(0)[0][0] == 1) {
                        doWorkOr();
                        //Sending data from the client

                        clientSocket = new Socket(clientIP, clientPort);
                        ObjectOutputStream outToClient = new ObjectOutputStream(clientSocket.getOutputStream());
                        outToClient.writeObject(resultOr);
                        clientSocket.close();

                        //Resetting storage variables
                        serverJobs = new ArrayList<>();
                        serverResult = Collections.synchronizedList(new ArrayList<>());
                        socketCreations = new ArrayList<>();
                        flag = true;

                        //Calculating the time spent
                        timestamps.add(Instant.now());
//                        System.out.println(Helper.getProgramTimes(timestamps));
                    } else if (serverResult.get(0)[0][0] == 3){
                        doWorkSum();
                        //Sending data from the client

                        clientSocket = new Socket(clientIP, clientPort);
                        ObjectOutputStream outToClient = new ObjectOutputStream(clientSocket.getOutputStream());
                        outToClient.writeObject(resultSum);
                        clientSocket.close();

                        //Resetting storage variables
                        serverJobs = new ArrayList<>();
                        serverResult = Collections.synchronizedList(new ArrayList<>());
                        socketCreations = new ArrayList<>();
                        flag = true;

                        //Calculating the time spent
                        timestamps.add(Instant.now());
//                        System.out.println(Helper.getProgramTimes(timestamps));
                    }
                } else if (socketCreationsRow.size() == serverCount) {
                    timestamps = new ArrayList<>();
                    timestamps.add(Instant.now());
                    for (SocketCreationRow socketCreation : socketCreationsRow) {
                        serverJobs.add(threadPool.submit(socketCreation));
                    }
                    for (Future<?> future : serverJobs)
                        future.get();

                    if (serverResultRow.get(0)[0][0][0] == 2) {
                        doWorkRow();
                        //Sending data from the client
                        clientSocket = new Socket(clientIP, clientPort);
                        ObjectOutputStream outToClient = new ObjectOutputStream(clientSocket.getOutputStream());
                        outToClient.writeObject(resultRow);
                        clientSocket.close();
                        //Resetting storage variables
                        resultRow = new int[querySize][noOfColumns][];
                        serverJobs = new ArrayList<>();
                        serverResultRow = Collections.synchronizedList(new ArrayList<>());
                        socketCreationsRow = new ArrayList<>();
                        flag = true;
                        flag1 = false;

                        //Calculating the time spent
                        timestamps.add(Instant.now());
//                        System.out.println(Helper.getProgramTimes(timestamps));
                    }
                }
            }
        } catch (IOException | ExecutionException | InterruptedException ex) {
            log.log(Level.SEVERE, ex.getMessage());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private static void doWorkRow() {
        // The list containing all the threads

        for (int i = 0; i < serverResultRow.size(); i++) {
            switch (serverResultRow.get(i)[serverResultRow.get(i).length - 1][0][0]) {
                case 1 -> serverRow1 = serverResultRow.get(i);
                case 2 -> serverRow2 = serverResultRow.get(i);
                case 3 -> serverRow3 = serverResultRow.get(i);
                case 4 -> serverRow4 = serverResultRow.get(i);
            }
        }

        querySize = serverRow1.length - 2;
        resultRow = new int[querySize][noOfColumns][];

        int[] share;
        for (int i = 1; i <= querySize; i++) {

            for (int l = 0; l < noOfColumns; l++) {
                resultRow[i - 1][l] = new int[serverRow1[i][l].length];
                if(!columnhash.get(columList.get(l)).equals("string")){
                    share = new int[]{serverRow1[i][l][0], serverRow2[i][l][0], serverRow3[i][l][0], serverRow4[i][l][0]};
                    resultRow[i - 1][l][0] = (langrangesInterpolatation(share));
                }else if(columnhash.get(columList.get(l)).equals("string")){
                    for(int m=0;m<serverRow1[i][l].length;m++){
                        share = new int[]{serverRow1[i][l][m], serverRow2[i][l][m], serverRow3[i][l][m], serverRow4[i][l][m]};
                        resultRow[i - 1][l][m] = (langrangesInterpolatation(share));
                    }
                }
            }
        }
    }

    private static void doWorkSum() {
        // The list containing all the threads

        for (int i = 0; i < serverResult.size(); i++) {
            switch (serverResult.get(i)[serverResult.get(i).length - 1][0]) {
                case 1 -> server1 = serverResult.get(i);
                case 2 -> server2 = serverResult.get(i);
                case 3 -> server3 = serverResult.get(i);
                case 4 -> server4 = serverResult.get(i);
            }
        }

        resultSum = new int[numRows];

        int[] share;
        for (int i = 0; i <numRows; i++) {
            share = new int[]{server1[1][i], server2[1][i], server3[1][i], server4[1][i]};
            resultSum[i] = (langrangesInterpolatation(share));
        }
    }


    private static void doWorkOr() {
        // The list containing all the threads


        for (int i = 0; i < serverResult.size(); i++) {
            switch ((int) serverResult.get(i)[serverResult.get(i).length - 1][0]) {
                case 1 -> server1 = serverResult.get(i);
                case 2 -> server2 = serverResult.get(i);
                case 3 -> server3 = serverResult.get(i);
                case 4 -> server4 = serverResult.get(i);
            }
        }

        int resultDim = 1;
        if (server1.length > 2)
            resultDim = 2;
        resultOr = new int[resultDim][numRows];

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
                ex.printStackTrace();
            }
        }
    }

    private static class ParallelTaskOr implements Runnable {
        private int threadNum;

        public ParallelTaskOr(int threadNum) {
            this.threadNum = threadNum;
        }

        @Override
        public void run() {
            int startRow = (threadNum - 1) * numRowsPerThread;
            int endRow = startRow + numRowsPerThread;

            // The operation of adding each of the M1 and M2 values

            int[] share1 = null, share2;
            for (int i = startRow; i < endRow; i++) {
                if (server1.length > 3) {
                    share1 = new int[]{server1[1][i], server2[1][i], server3[1][i]};
                    share2 = new int[]{server1[2][i], server2[2][i], server3[2][i]};
                    resultOr[0][i] = langrangesInterpolatation(share1);
                    resultOr[1][i] = langrangesInterpolatation(share2);
                } else {
                    switch (serverCount) {
                        case 2 -> share1 = new int[]{server1[1][i], server2[1][i]};
                        case 3 -> share1 = new int[]{server1[1][i], server2[1][i], server3[1][i]};
                        case 4 ->
                                share1 = new int[]{server1[1][i], server2[1][i], server3[1][i], server4[1][i]};
                    }
                    resultOr[0][i] = langrangesInterpolatation(share1);
                }
            }
        }
    }

    private static int langrangesInterpolatation(int share[]) {
        return switch (share.length) {
            case 2 -> (int) Helper.mod(Helper.mod((long) 2 * share[0]) - share[1]);
            case 3 -> (int) Helper.mod(Helper.mod((long) 3 * share[0]) - Helper.mod((long) 3 * share[1]) + share[2]);
            case 4 ->
                    (int) Helper.mod(Helper.mod(Helper.mod(((long) 4 * share[0])) - (Helper.mod((long) 6 * share[1])) + (Helper.mod((long) 4 * share[2])) - (Helper.mod(share[3]))));
            default -> 0;
        };
    }

    private static void doPreWork(String[] args) {
        //Reading Combiner property file
        String pathName = "config/Combiner.properties";
        Properties properties = Helper.readPropertiesFile(pathName);

        numRows = Integer.parseInt(properties.getProperty("numRows"));
        numThreads = Integer.parseInt(properties.getProperty("numThreads"));
        numRowsPerThread = numRows / numThreads;

        clientPort = Integer.parseInt(properties.getProperty("clientPort"));
        clientIP = properties.getProperty("clientIP");
        combinerPort = Integer.parseInt(properties.getProperty("combinerPort"));

        resultAnd = new int[numRows];

        Helper.readMeta();
        noOfColumns = Helper.getNoOfColumns();
        columnhash = Helper.getColumnList();
        columList = new ArrayList<>(columnhash.keySet());
    }

    public static void main(String args[]) {

        doPreWork(args);

        Combiner combiner = new Combiner();
        combiner.startCombiner();

    }
}