package src._04_OR_Search.combiner;

import constant.*;
import utility.Helper;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.math.BigInteger;

public class Combiner extends Thread {
    // stores result received from servers
    private static List<BigInteger[][]> serverResult_04 = Collections.synchronizedList(new ArrayList<>());
    private static final ExecutorService threadPool = Executors.newFixedThreadPool(Constants.getThreadPoolSize());
    private static List<SocketCreation> socketCreations = new ArrayList<>();
    private static BigInteger[][] result_04;

    // the number of row of tpch.lineitem considered
    private static int numRows;
    // the number of threads combiner program is running on
    private static int numThreads;
    // the number of row per thread
    private static int numRowsPerThread;

    // stores port value for combiner
    private static int combinerPort;
    // stores port value for client
    private static int clientPort;
    // stores IP value for client
    private static String clientIP;

    // stores server data
    private static BigInteger[][] server1_04;
    private static BigInteger[][] server2_04;
    private static BigInteger[][] server3_04;
    private static BigInteger[][] server4_04;
    private static int serverCount_04;
    private static boolean flag_04 = true;

    private static final Logger log_04 = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    private static ArrayList<Instant> timestamps = new ArrayList<>();

    private static final int portIncrement = 0;

    // shamir secret share data interpolation
    private static BigInteger langrangesInterpolatation_04(BigInteger share[]) {
        return switch (share.length) {
            case 2 -> Helper.mod(Helper.mod(BigInteger.valueOf(2).multiply(share[0])).subtract(share[1]));
            case 3 -> Helper.mod(Helper.mod(BigInteger.valueOf(3).multiply(share[0])).subtract(Helper.mod(BigInteger.valueOf(3).multiply(share[1]))).add(share[2]));
            case 4 -> 
                    Helper.mod(
                        Helper.mod(
                            Helper.mod(BigInteger.valueOf(4).multiply(share[0]))
                                .subtract(Helper.mod(BigInteger.valueOf(6).multiply(share[1])))
                                .add(Helper.mod(BigInteger.valueOf(4).multiply(share[2])))
                                .subtract(Helper.mod(share[3]))
                        )
                    );
            default -> BigInteger.valueOf(0);
        };
    }

    // operation performed by each thread
    private static class ParallelTask_04 implements Runnable {
        private int threadNum;

        public ParallelTask_04(int threadNum) {
            this.threadNum = threadNum;
        }

        @Override
        public void run() {
            int startRow = (threadNum - 1) * numRowsPerThread;
            int endRow = startRow + numRowsPerThread;

            // adding data received from the server

            BigInteger[] share1 = null, share2;
            for (int i = startRow; i < endRow; i++) {
                if (server1_04.length > 2) {
                    share1 = new BigInteger[]{server1_04[0][i], server2_04[0][i], server3_04[0][i]};
                    share2 = new BigInteger[]{server1_04[1][i], server2_04[1][i], server3_04[1][i]};
                    result_04[0][i] = langrangesInterpolatation_04(share1);
                    result_04[1][i] = langrangesInterpolatation_04(share2);
                } else { 
                    switch (serverCount_04) {
                        case 2 -> share1 = new BigInteger[]{server1_04[0][i], server2_04[0][i]};
                        case 3 -> share1 = new BigInteger[]{server1_04[0][i], server2_04[0][i], server3_04[0][i]};
                        case 4 -> share1 = new BigInteger[]{server1_04[0][i], server2_04[0][i], server3_04[0][i], server4_04[0][i]};
                    }
                    result_04[0][i] = langrangesInterpolatation_04(share1);
                }
            }
        }
    }

    // working on server data to process for client
    private static void doWork_04() {
        // The list containing all the threads

        // extracting each server data from data received
        for (int i = 0; i < serverResult_04.size(); i++) {
            switch (serverResult_04.get(i)[serverResult_04.get(i).length - 1][0].intValue()) {
                case 1 -> server1_04 = serverResult_04.get(i);
                case 2 -> server2_04 = serverResult_04.get(i);
                case 3 -> server3_04 = serverResult_04.get(i);
                case 4 -> server4_04 = serverResult_04.get(i);
            }
        }

        int resultDim = 1;
        if (server1_04.length > 2)
            resultDim = 2;
        result_04 = new BigInteger[resultDim][numRows];

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

        // wait for all threads to finish
        for (Thread thread : threadList) {
            try {
                thread.join();
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }
    }





    // socket to read data from servers
    class SocketCreation implements Runnable {

        private final Socket serverSocket;

        SocketCreation(Socket serverSocket) {
            this.serverSocket = serverSocket;
        }

        @Override
        public void run() {
            ObjectInputStream inFromServer;
            try {
                // initializing input stream for reading the data
                inFromServer = new ObjectInputStream(serverSocket.getInputStream());
                serverResult_04.add((BigInteger[][]) inFromServer.readObject());
                // socket closed
                serverSocket.close();
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

    // starting combiner to process server data
    private void startCombiner() {
        Socket serverSocket;
        Socket clientSocket;
        ArrayList<Future> serverJobs = new ArrayList<>();

        try {
            ServerSocket ss = new ServerSocket(combinerPort);
            //System.out.println("Combiner Listening........");

            while (true) {
                // reading data from the server
                serverSocket = ss.accept();

                if (flag_04) { // when client communicates with combiner
                    ObjectInputStream inFromServer = new ObjectInputStream(serverSocket.getInputStream());
                    serverCount_04= Integer.parseInt(((String[])inFromServer.readObject())[0]);
                    flag_04 = false;
                    serverSocket.close();
                } else { // when sever communicates with combiner
                    socketCreations.add(new SocketCreation(serverSocket));
                }

                // processing data received from both the servers
                if (socketCreations.size() == serverCount_04) {
                    timestamps = new ArrayList<>();
                    timestamps.add(Instant.now());
                    for (SocketCreation socketCreation : socketCreations) {
                        serverJobs.add(threadPool.submit(socketCreation));
                    }
                    for (Future<?> future : serverJobs)
                        future.get();
                    doWork_04();
                    // sending data from the client

                    clientSocket = new Socket(clientIP, clientPort);
                    ObjectOutputStream outToClient = new ObjectOutputStream(clientSocket.getOutputStream());
                    outToClient.writeObject(result_04);
                    clientSocket.close();

                    // resetting storage variables
                    serverJobs = new ArrayList<>();
                    serverResult_04 = Collections.synchronizedList(new ArrayList<>());
                    socketCreations = new ArrayList<>();
                    flag_04 = true;

                    // calculating the time spent
                    timestamps.add(Instant.now());
//                    System.out.println(Helper.getProgramTimes(timestamps));
//                  log.log(Level.INFO, "Total Combiner time:" + Helper.getProgramTimes(timestamps));
                }
            }
        } catch (IOException | ExecutionException | InterruptedException | ClassNotFoundException ex) {
            log_04.log(Level.SEVERE, ex.getMessage());
        }
    }




        
    /**
     * It performs initialization tasks
     */
    private static void doPreWork(String[] args) {
        // reading Combiner property file
        String pathName = "config/Combiner.properties";
        Properties properties = Helper.readPropertiesFile(pathName);

        numRows = Integer.parseInt(properties.getProperty("numRows"));
        numThreads = Integer.parseInt(properties.getProperty("numThreads"));
        numRowsPerThread = numRows / numThreads;

        clientPort = Integer.parseInt(properties.getProperty("clientPort")) + portIncrement;
        clientIP = properties.getProperty("clientIP");
        combinerPort = Integer.parseInt(properties.getProperty("combinerPort")) + portIncrement + 10;
    }

    /**
     * combiner process the data received from server before sending to client
     */
    public static void main(String args[]) {

        doPreWork(args);

        Combiner combiner = new Combiner();
        combiner.startCombiner();

    }
}