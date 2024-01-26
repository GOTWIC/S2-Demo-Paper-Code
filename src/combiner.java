package src;

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

public class combiner extends Thread {

    // ---------------- UNIVERSAL SERVER GLOBALS ---------------- \\

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
    private static final int portIncrement = 0;
    private static ArrayList<Instant> timestamps = new ArrayList<>();
    private static List<SocketCreation> socketCreations = new ArrayList<>();
    private static final ExecutorService threadPool = Executors.newFixedThreadPool(Constants.getThreadPoolSize());

    private int protocol = 0;


    // ---------------- 01 COMBINER GLOBALS ---------------- \\

         // stores result received from servers
        private static List<int[]> serverResult_01 = Collections.synchronizedList(new ArrayList<>());
        private static int[] result_01;

        // stores server data
        private static int[] server1_01;
        private static int[] server2_01;
    
        private static final Logger log_01 = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    // ---------------- 02 COMBINER GLOBALS ---------------- \\

    // stores result received from servers
    private static List<int[]> serverResult_02 = Collections.synchronizedList(new ArrayList<>());
    private static int[] result_02;

    // stores server data
    private static int[] server1_02;
    private static int[] server2_02;

    private static final Logger log_02 = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);


    // ---------------- 03 COMBINER GLOBALS ---------------- \\

    private static List<int[]> serverResult_03 = Collections.synchronizedList(new ArrayList<>());
    private static int[] result_03;
    // stores server data
    private static int[] server1_03;
    private static int[] server2_03;

    private static final Logger log_03 = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    // ---------------- 01 COMBINER CODE ---------------- \\

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


    // ---------------- 02 COMBINER CODE ---------------- \\

    // operation performed by each thread
    private static class ParallelTask_02 implements Runnable {
        private int threadNum;

        public ParallelTask_02(int threadNum) {
            this.threadNum = threadNum;
        }

        @Override
        public void run() {
            int startRow = (threadNum - 1) * numRowsPerThread;
            int endRow = startRow + numRowsPerThread;

            // adding data received from the server
            for (int i = startRow; i < endRow; i++) {
                result_02[i] = (int) Helper.mod((long) server1_02[i] + (long) server2_02[i]);
            }
        }
    }

    // working on server data to process for client
    private static void doWork_02() {
        // the list containing all the threads

        server1_02 = serverResult_02.get(0);
        server2_02 = serverResult_02.get(1);
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

    // ---------------- 03 COMBINER CODE ---------------- \\

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
   

    // ---------------- UNIVERSAL CODE ---------------- \\


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

                protocol = data[0];
                //System.out.println("Protocol: " + protocol);

                // remove the first element
                int[] temp = new int[data.length - 1];
                System.arraycopy(data, 1, temp, 0, temp.length);

                if(protocol == 1){
                    serverResult_01.add(temp);
                }
                else if (protocol == 2){
                    serverResult_02.add(temp);
                }
                else if (protocol == 3){
                    serverResult_03.add(temp);
                }

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

                    if(protocol == 1){
                        doWork_01();

                        // sending data from the client
                        clientSocket = new Socket(clientIP, clientPort);
                        ObjectOutputStream outToClient = new ObjectOutputStream(clientSocket.getOutputStream());
                        outToClient.writeObject(result_01);
                        clientSocket.close();

                        // resetting storage variables
                        result_01 = new int[numRows];
                        serverResult_01 = Collections.synchronizedList(new ArrayList<>());
                    }

                    else if (protocol == 2){
                        doWork_02();

                        // sending data from the client
                        clientSocket = new Socket(clientIP, clientPort);
                        ObjectOutputStream outToClient = new ObjectOutputStream(clientSocket.getOutputStream());
                        outToClient.writeObject(result_02);
                        clientSocket.close();

                        // resetting storage variables
                        result_02 = new int[numRows];
                        serverResult_02 = Collections.synchronizedList(new ArrayList<>());
                    }

                    else if (protocol == 3){
                        doWork_03();

                        // sending data from the client
                        clientSocket = new Socket(clientIP, clientPort);
                        ObjectOutputStream outToClient = new ObjectOutputStream(clientSocket.getOutputStream());
                        outToClient.writeObject(result_03);
                        clientSocket.close();

                        // resetting storage variables
                        result_03 = new int[numRows];
                        serverResult_03 = Collections.synchronizedList(new ArrayList<>());
                    }


                    serverJobs = new ArrayList<>();
                    socketCreations = new ArrayList<>();

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

        // TODO: These may not be needed, check later
        result_01 = new int[numRows];
        result_02 = new int[numRows];
        result_03 = new int[numRows];
    }


    public static void main(String args[]) {

        doPreWork(args);

        combiner combiner = new combiner();
        combiner.startCombiner();

    }
    
}
