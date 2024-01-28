package src;

import constant.*;
import utility.Helper;
import java.io.*;
import java.math.BigInteger;
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


    // ---------------- 04 COMBINER GLOBALS ---------------- \\

    private static List<BigInteger[][]> serverResult_04 = Collections.synchronizedList(new ArrayList<>());
    private static BigInteger[][] result_04;// stores server data
    private static BigInteger[][] server1_04;
    private static BigInteger[][] server2_04;
    private static BigInteger[][] server3_04;
    private static BigInteger[][] server4_04;
    private static int serverCount_04;
    private static boolean flag_04 = true;

    private static final Logger log_04 = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    // ---------------- 05 COMBINER GLOBALS ---------------- \\

        // stores result received from servers
        private static List<BigInteger[][]> serverResult_05 = Collections.synchronizedList(new ArrayList<>());
        private static BigInteger[][] result_05;
        // stores server data
        private static BigInteger[][] server1_05;
        private static BigInteger[][] server2_05;
        private static BigInteger[][] server3_05;
        private static BigInteger[][] server4_05;
        private static int querySize_05;
        private static int numCols_05;
        private static final Logger log_05 = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

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
   

    // ---------------- 04 COMBINER CODE ---------------- \\

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


    // ---------------- 05 COMBINER CODE ---------------- \\

        // shamir secret share data interpolation
        private static BigInteger langrangesInterpolatation_05(BigInteger share[]) {
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
    
        // working on server data to process for client
        private static void doWork_05() {
    
            numCols_05 = serverResult_05.get(0)[0].length;
            // extracting server based information
    
            // TODO: what is serverResult.size();
            for (int i = 0; i < serverResult_05.size(); i++) {
                switch (serverResult_05.get(i)[serverResult_05.get(i).length - 1][0].intValue()) {
                    case 1 -> server1_05 = serverResult_05.get(i);
                    case 2 -> server2_05 = serverResult_05.get(i);
                    case 3 -> server3_05 = serverResult_05.get(i);
                    case 4 -> server4_05 = serverResult_05.get(i);
                }
            }
    
            querySize_05 = server1_05.length - 1;
            result_05 = new BigInteger[querySize_05][numCols_05];
    
            // interpolating values from shares
            BigInteger[] share;
            for (int i = 0; i < querySize_05; i++) {
    
                ////System.out.println("Server Length 2: " + server1[0].length);
    
                for(int j = 0; j < server1_05[0].length; j++){
                    share = new BigInteger[]{server1_05[i][j], server2_05[i][j], server3_05[i][j], server4_05[i][j]};
                    result_05[i][j] = (langrangesInterpolatation_05(share));
                }
    
    
                //share = new BigInteger[]{server1[i][0], server2[i][0], server3[i][0], server4[i][0]};
                //result[i][0] = (langrangesInterpolatation(share));
                //share = new BigInteger[]{server1[i][1], server2[i][1], server3[i][1], server4[i][1]};
                //result[i][1] = (langrangesInterpolatation(share));
                //share = new BigInteger[]{server1[i][2], server2[i][2], server3[i][2], server4[i][2]};
                //result[i][2] = (langrangesInterpolatation(share));
                //share = new BigInteger[]{server1[i][3], server2[i][3], server3[i][3], server4[i][3]};
                //result[i][3] = (langrangesInterpolatation(share));
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

                Object tempObj = inFromServer.readObject();

                try{
                    int[] data=(int[]) tempObj;
                    protocol = data[0];

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
                }
                catch (ClassCastException e1) {

                    //System.out.println("Failed to Cast as Int[], trying as BigInt[][]");

                    BigInteger[][] data=(BigInteger[][]) tempObj;
                    protocol = data[0][0].intValue();

                    //System.out.println("Raw OBJ: " + Helper.arrToStr(data));

                    

                    // remove the first row
                    BigInteger[][] temp = new BigInteger[data.length - 1][data[0].length];
                    System.arraycopy(data, 1, temp, 0, temp.length);

                    //System.out.println("Protocol: " + protocol);

                    serverResult_05.add(temp);
                }

                //socket closed
                serverSocket.close();
            } catch (IOException | ClassNotFoundException ex) {
                log_01.log(Level.SEVERE, ex.getMessage());
            }
        }
    }

/*
    @Override
    public void run() {
        startCombiner();
        super.run();
    }
*/  
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

                //System.out.println("Protocol: " + protocol);

                //  && (protocol == 1 || protocol == 2 || protocol == 3)
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

                    else if(protocol == 5){
                        // we might not be done yet, continue listening for data
                        continue;
                    }
                    serverJobs = new ArrayList<>();
                    socketCreations = new ArrayList<>();

                    // calculating the time spent
                    timestamps.add(Instant.now());
//                    System.out.println(Helper.getProgramTimes(timestamps));
//                    log.log(Level.INFO, "Total Combiner time:" + Helper.getProgramTimes(timestamps));
                }

                if (socketCreations.size() == 4) {
                    timestamps = new ArrayList<>();
                    timestamps.add(Instant.now());
                    for (SocketCreation socketCreation : socketCreations) {
                        serverJobs.add(threadPool.submit(socketCreation));
                    }
                    for (Future<?> future : serverJobs)
                        future.get();


                    doWork_05();
                    // sending data from the client
                    clientSocket = new Socket(clientIP, clientPort);
                    ObjectOutputStream outToClient = new ObjectOutputStream(clientSocket.getOutputStream());
                    outToClient.writeObject(result_05);
                    clientSocket.close();
                    // resetting storage variables
                    
                    // CHECK: is serverResult correct?
                    result_05 = new BigInteger[querySize_05][numCols_05];
                    serverJobs = new ArrayList<>();
                    serverResult_05 = Collections.synchronizedList(new ArrayList<>());
                    socketCreations = new ArrayList<>();

                    // calculating the time spent
                    timestamps.add(Instant.now());
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
