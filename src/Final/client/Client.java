package src.Final.client;

import constant.*;
import utility.Helper;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Instant;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Client extends Thread {

    // stores IP value of desired server/client
    public String IP;
    // stores port value of desired server/client
    public int port;
    // stores data send out by the client to servers
    private String[] data_01;

    // stores IP for server1
    private static String server1IP;
    // stores port for server1
    private static int server1Port;
    // stores IP for server2
    private static String server2IP;
    // stores IP for server2
    private static int server2Port;
    // stores port for client
    private static int clientPort;

    // the name of the tpch.lineitem column to search over
    private static String columnName_01;
    // the value of the tpch.lineitem column to search over
    private static int columnValue_01;
    // the fingerprintPrimeNumber value i.e value of r which is taken as 43 in our case
    private static int fingerprintPrimeNumber_01;
    // the fingerprint value generated for server1
    private static int fingerprint1_01;
    // the fingerprint value generated for server2
    private static int fingerprint2_01;
    // stores seed value for client for random number generation
    private static int seedClient_01;

    // the number of row of tpch.lineitem considered
    private static int numRows_01;
    // the number of threads client program is running on
    private static int numThreads_01;
    // the number of row per thread
    private static int numRowsPerThread_01;

    // stores the result received/sent from/to combiner
    private static int[] resultCombiner_01;
    // stores result received from servers
    private static final List<Integer> result_01 = Collections.synchronizedList(new ArrayList<>());

    // used to calculate the time taken by client program
    private static final ArrayList<Instant> timestamps1_01 = new ArrayList<>();
    private static final ArrayList<Instant> timestamps2_01 = new ArrayList<>();
    private static final Logger log_01 = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    // the name of file storing the query result under result/ folder
    private static final String resultFileName_01 = "_01_oneColumnStringSearch";


    // default constructor
    private Client() {
    }

    // parametrised constructor
    public Client(String IP, int port, String[] data) {
        this.IP = IP;
        this.port = port;
        this.data_01 = data;
    }

    // operation performed by each thread
    private static class ParallelTask_01 implements Runnable {
        private final int threadNum;

        public ParallelTask_01(int threadNum) {
            this.threadNum = threadNum;
        }

        @Override
        public void run() {
            int startRow = (threadNum - 1) * numRowsPerThread_01;
            int endRow = startRow + numRowsPerThread_01;
            Random randSeedClient = new Random(seedClient_01);
            int prg;

            // evaluating which rows matches the requested query and storing row ids in result list
            for (int i = startRow; i < endRow; i++) {
                prg = randSeedClient.nextInt(Constants.getMaxRandomBound() -
                        Constants.getMinRandomBound()) + Constants.getMinRandomBound();
                if (resultCombiner_01[i] == prg) {
                    result_01.add(i + 1);
                }
            }
        }
    }

    // to interpolate the data received from the server
    private static void interpolation_01() {
        // the list containing all the threads
        List<Thread> threadList = new ArrayList<>();

        // wreate threads and add them to threadlist
        int threadNum;
        for (int i = 0; i < numThreads_01; i++) {
            threadNum = i + 1;
            threadList.add(new Thread(new ParallelTask_01(threadNum), "Thread" + threadNum));
        }

        // wtart all threads
        for (int i = 0; i < numThreads_01; i++) {
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

    // receiving server data over the socket
    static class ReceiverSocket_01 {

        private Socket socket;

        ReceiverSocket_01(Socket socket) {
            this.socket = socket;
        }

        @SuppressWarnings("unchecked")
        public void run() {
            try {
                // receiving the data from the Combiner
                ObjectInputStream inFromServer = new ObjectInputStream(socket.getInputStream());
                resultCombiner_01 = (int[]) inFromServer.readObject();
                // interpolating data to get results
                interpolation_01();
                // socket closed
                socket.close();
            } catch (IOException | ClassNotFoundException ex) {
                log_01.log(Level.SEVERE, ex.getMessage());
            }
        }
    }

    // starting to listen for incoming responses from servers
    private List<Integer> startAsReceiver_01() {
        Socket socket;

        try {
            ServerSocket ss = new ServerSocket(clientPort);
            // listening over socket for incoming connections
            socket = ss.accept();
            timestamps2_01.add(Instant.now());

            // processing data received from server
            new ReceiverSocket_01(socket).run();

            // printing result of the query
            //Helper.printResult(result, resultFileName);

            timestamps2_01.add(Instant.now());
            int totalTime = Math.toIntExact(Helper.getProgramTimes(timestamps1_01).get(0)) +
                    Math.toIntExact(Helper.getProgramTimes(timestamps2_01).get(0));
            return result_01;
        } catch (IOException ex) {
            log_01.log(Level.SEVERE, ex.getMessage());
        }

        return result_01;
    }

    // to send client data to servers
    private void startAsSender_01() {
        Socket socket;
        ObjectOutputStream outToServer;
        try {
            // socket creation and initialising output stream to write data
            socket = new Socket(IP, port);
            outToServer = new ObjectOutputStream(socket.getOutputStream());
            // writing data to stream
            outToServer.writeObject(data_01);
            // socket closed
            socket.close();
        } catch (IOException ex) {
            log_01.log(Level.SEVERE, ex.getMessage());
        }
    }

    @Override
    public void run() {
        startAsSender_01();
        super.run();
    }

    // prepares data to send to server and starts listening to target servers
    private static List<Integer> doPostWork_01() {

        // server data preparation
        String[] data;
        data = new String[]{columnName_01, String.valueOf(fingerprint1_01), String.valueOf(seedClient_01)};
        Client server1 = new Client(server1IP, server1Port, data);

        data = new String[]{columnName_01, String.valueOf(fingerprint2_01)};
        Client server2 = new Client(server2IP, server2Port, data);

        // sending data to each server
        server1.start();
        server2.start();

        // started to listen for incoming responses from servers
        timestamps1_01.add(Instant.now());
        Client client = new Client();
        List<Integer> returnResult = client.startAsReceiver_01();
        return returnResult;
    }

    // creates additive shares of the search keyword value and generated fingerprint for each server
    private static void doWork_01() {
        Random random = new Random();

        int additiveShare1, additiveShare2, multiplier;

        // extracts each digit/letter of string value
        int[] valueSplit = Helper.stringToIntArray(String.valueOf(columnValue_01));

        // loops over each digit/letter to generate fingerprint value for server
        for (int j = valueSplit.length - 1; j >= 0; j--) {
            // additive share for digit/letter
            additiveShare1 = random.nextInt(Constants.getMaxRandomBound() - Constants.getMinRandomBound())
                    + Constants.getMinRandomBound();
            additiveShare2 = valueSplit[j] - additiveShare1;

            // fingerprint generation
            multiplier = (int) Helper.mod((long) Math.pow(fingerprintPrimeNumber_01, j + 1));

            fingerprint1_01 = (int) Helper.mod(fingerprint1_01 +
                    Helper.mod((long) multiplier * (long) additiveShare1));
            fingerprint2_01 = (int) Helper.mod(fingerprint2_01 +
                    Helper.mod((long) multiplier * (long) additiveShare2));
        }
    }

    /**
     * It performs initialization tasks
     * @param args takes as string a column name and column value e.g. "suppkey,145"
     */

    private static void doPreWork_01(String[] args) {

        String query = args[0];

        // splitting the argument value to extract column name and value to be searched
        String[] querySplit = query.split(",");
        columnName_01 = querySplit[0];
        columnValue_01 = Integer.parseInt(querySplit[1]);

        // reads configuration properties of the client
        String pathName = "config/Client.properties";
        Properties properties = Helper.readPropertiesFile(pathName);

        seedClient_01 = Integer.parseInt(properties.getProperty("seedClient"));
        fingerprintPrimeNumber_01 = Integer.parseInt(properties.getProperty("fingerprintPrimeNumber"));

        numRows_01 = Integer.parseInt(properties.getProperty("numRows"));
        numThreads_01 = Integer.parseInt(properties.getProperty("numThreads"));
        numRowsPerThread_01 = numRows_01 / numThreads_01;

        clientPort = Integer.parseInt(properties.getProperty("clientPort"));
        server1IP = properties.getProperty("server1IP");
        server1Port = Integer.parseInt(properties.getProperty("server1Port"));
        server2IP = properties.getProperty("server2IP");
        server2Port = Integer.parseInt(properties.getProperty("server2Port"));

        resultCombiner_01 = new int[numRows_01];
    }


    public static List<Integer> main(String[] args) throws InterruptedException {
        timestamps1_01.add(Instant.now());

        doPreWork_01(args);

        doWork_01();

        List<Integer> returnResult = doPostWork_01();

        return returnResult;
    }
}


