package src._05_Multiplicative_Row_Fetch.combiner;

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
import java.math.BigInteger;

public class Combiner extends Thread {
    // stores result received from servers
    private static List<BigInteger[][]> serverResult_05 = Collections.synchronizedList(new ArrayList<>());
    private static final ExecutorService threadPool = Executors.newFixedThreadPool(Constants.getThreadPoolSize());
    private static List<SocketCreation> socketCreations = new ArrayList<>();
    private static BigInteger[][] result_05;

    // stores port value for combiner
    private static int combinerPort;
    // stores port value for client
    private static int clientPort;
    // stores IP value for client
    private static String clientIP;

    // stores server data
    private static BigInteger[][] server1_05;
    private static BigInteger[][] server2_05;
    private static BigInteger[][] server3_05;
    private static BigInteger[][] server4_05;
    private static int querySize_05;

    private static int numCols_05;

    private static final Logger log_05 = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    private static ArrayList<Instant> timestamps = new ArrayList<>();

    private static final int portIncrement = 40;

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
                serverResult_05.add((BigInteger[][]) inFromServer.readObject());
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
                socketCreations.add(new SocketCreation(serverSocket));

                // processing data received from both the servers
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
//                    System.out.println(Helper.getProgramTimes(timestamps));
//                    log.log(Level.INFO, "Total Combiner time:" + Helper.getProgramTimes(timestamps));
                }
            }
        } catch (IOException | ExecutionException | InterruptedException ex) {
            log_05.log(Level.SEVERE, ex.getMessage());
        }
    }

    /**
     * It performs initialization tasks
     */
    private static void doPreWork(String[] args) {
        // reading Combiner property file
        String pathName = "config/Combiner.properties";
        Properties properties = Helper.readPropertiesFile(pathName);

        clientPort = Integer.parseInt(properties.getProperty("clientPort")) + portIncrement;
        clientIP = properties.getProperty("clientIP");
        combinerPort = Integer.parseInt(properties.getProperty("combinerPort")) + portIncrement;

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