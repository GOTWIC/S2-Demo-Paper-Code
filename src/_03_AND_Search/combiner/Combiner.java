package src._03_AND_Search.combiner;

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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Combiner extends Thread {
    // stores result received from servers
    private static List<int[]> serverResult_03 = Collections.synchronizedList(new ArrayList<>());
    private static final ExecutorService threadPool = Executors.newFixedThreadPool(Constants.getThreadPoolSize());
    private static List<SocketCreation> socketCreations = new ArrayList<>();
    private static int[] result_03;

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
    private static int[] server1_03;
    private static int[] server2_03;

    private static final Logger log_03 = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    private static ArrayList<Instant> timestamps = new ArrayList<>();

    private static final int portIncrement = 0;

    // operation performed by each thread
    private static class ParallelTask_03 implements Runnable {
        private int threadNum;

        public ParallelTask_03(int threadNum) {
            this.threadNum = threadNum;
        }

        @Override
        public void run() {
            int startRow = (threadNum - 1) * numRowsPerThread;
            int endRow = startRow + numRowsPerThread;
            // adding data received from the server
            for (int i = startRow; i < endRow; i++) {
                result_03[i] = (int) Helper.mod((long) server1_03[i] + (long) server2_03[i]);
            }
        }
    }

    // working on server data to process for client
    private static void doWork_03() {
        // the list containing all the threads

        server1_03 = serverResult_03.get(0);
        server2_03 = serverResult_03.get(1);
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
                serverResult_03.add((int[]) inFromServer.readObject());
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
                // listening over the socket for connections
                serverSocket = ss.accept();
                // reading data from the server
                socketCreations.add(new SocketCreation(serverSocket));

                // processing data after receiving data from both the servers
                if (socketCreations.size() == 2) {
                    timestamps = new ArrayList<>();
                    timestamps.add(Instant.now());
                    for (SocketCreation socketCreation : socketCreations) {
                        serverJobs.add(threadPool.submit(socketCreation));
                    }
                    for (Future<?> future : serverJobs)
                        future.get();
                    doWork_03();
                    // sending data from the client
                    clientSocket = new Socket(clientIP, clientPort);
                    ObjectOutputStream outToClient = new ObjectOutputStream(clientSocket.getOutputStream());
                    outToClient.writeObject(result_03);
                    clientSocket.close();

                    // resetting storage variables
                    result_03 = new int[numRows];
                    serverJobs = new ArrayList<>();
                    serverResult_03 = Collections.synchronizedList(new ArrayList<>());
                    socketCreations = new ArrayList<>();

                    // calculating the time spent
                    timestamps.add(Instant.now());
//                    System.out.println(Helper.getProgramTimes(timestamps));
//                    log.log(Level.INFO, "Total Combiner time:" + Helper.getProgramTimes(timestamps));
                }
            }
        } catch (IOException | ExecutionException | InterruptedException ex) {
            log_03.log(Level.SEVERE, ex.getMessage());
        }
    }

    /**
     * It performs initialization tasks
     */
    private static void doPreWork(String[] args) {
        // reading combiner configuration file
        String pathName = "config/Combiner.properties";
        Properties properties = Helper.readPropertiesFile(pathName);

        numRows = Integer.parseInt(properties.getProperty("numRows"));
        numThreads = Integer.parseInt(properties.getProperty("numThreads"));
        numRowsPerThread = numRows / numThreads;

        clientPort = Integer.parseInt(properties.getProperty("clientPort")) + portIncrement;
        clientIP = properties.getProperty("clientIP");
        combinerPort = Integer.parseInt(properties.getProperty("combinerPort")) + portIncrement;

        result_03 = new int[numRows];
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