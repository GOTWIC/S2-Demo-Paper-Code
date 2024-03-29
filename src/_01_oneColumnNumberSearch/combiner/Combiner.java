package src._01_oneColumnNumberSearch.combiner;

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
    private static List<int[]> serverResult_01 = Collections.synchronizedList(new ArrayList<>());
    private static final ExecutorService threadPool_01 = Executors.newFixedThreadPool(Constants.getThreadPoolSize());
    private static List<SocketCreation> socketCreations_01 = new ArrayList<>();
    private static int[] result_01;

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
    private static int[] server1_01;
    private static int[] server2_01;

    private static final Logger log_01 = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    private static ArrayList<Instant> timestamps = new ArrayList<>();

    private static final int portIncrement = 0;

    // operation performed by each thread
    private static class ParallelTask_01 implements Runnable {
        private int threadNum;

        public ParallelTask_01(int threadNum) {
            this.threadNum = threadNum;
        }

        @Override
        public void run() {
            int startRow = (threadNum - 1) * numRowsPerThread;
            int endRow = startRow + numRowsPerThread;

            // adding data received from the server
            for (int i = startRow; i < endRow; i++) {
                result_01[i] = (int) Helper.mod((long) server1_01[i] + (long) server2_01[i]);
                //System.out.println("result[" + i + "]:" + result[i] + " = server1[" + i + "]:" + server1[i] + " + server2[" + i + "]:" + server2[i]);
            }
        }
    }

    // working on server data to process for client
    private static void doWork_01() {
        // the list containing all the threads

        server1_01 = serverResult_01.get(0);
        server2_01 = serverResult_01.get(1);
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
                log_01.log(Level.SEVERE, ex.getMessage());
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
                int[] data=(int[]) inFromServer.readObject();
                serverResult_01.add(data);
                //socket closed
                serverSocket.close();
            } catch (IOException | ClassNotFoundException ex) {
                log_01.log(Level.SEVERE, ex.getMessage());
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
                socketCreations_01.add(new SocketCreation(serverSocket));
                // processing data after receiving data from both the servers
                if (socketCreations_01.size() == 2) {
                    timestamps = new ArrayList<>();
                    timestamps.add(Instant.now());
                    for (SocketCreation socketCreation : socketCreations_01) {
                        serverJobs.add(threadPool_01.submit(socketCreation));
                    }
                    for (Future<?> future : serverJobs)
                        future.get();
                    doWork_01();

                    // sending data from the client
                    clientSocket = new Socket(clientIP, clientPort);
                    ObjectOutputStream outToClient = new ObjectOutputStream(clientSocket.getOutputStream());
                    outToClient.writeObject(result_01);
                    clientSocket.close();

                    // resetting storage variables
                    result_01 = new int[numRows];
                    serverJobs = new ArrayList<>();
                    serverResult_01 = Collections.synchronizedList(new ArrayList<>());
                    socketCreations_01 = new ArrayList<>();

                    // calculating the time spent
                    timestamps.add(Instant.now());
//                    System.out.println(Helper.getProgramTimes(timestamps));
//                    log.log(Level.INFO, "Total Combiner time:" + Helper.getProgramTimes(timestamps));
                }
            }
        } catch (IOException | ExecutionException | InterruptedException ex) {
            log_01.log(Level.SEVERE, ex.getMessage());
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

        result_01 = new int[numRows];
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