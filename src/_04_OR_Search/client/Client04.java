package src._04_OR_Search.client;

import constant.Constants;
import utility.Helper;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Instant;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.math.BigInteger;

public class Client04 extends Thread {

    // stores IP value of desired server/client
    public String IP;
    // stores port value of desired server/client
    public int port;
    // stores data send out by the client to servers
    private String[] data;



    // stores IP for server1
    private static String server1IP;
    // stores port for server1
    private static int server1Port;
    // stores IP for server2
    private static String server2IP;
    // stores port for server2
    private static int server2Port;
    // stores IP for server3
    private static String server3IP;
    // stores port for server3
    private static int server3Port;
    // stores IP for server4
    private static String server4IP;
    // stores port for server4
    private static int server4Port;
    // stores IP for client
    private static int clientPort;
    // stores IP for combiner
    private static String combinerIP;
    // stores port for combiner
    private static int combinerPort;

    // the list of name of the tpch.lineitem column to search over
    private static String[] columnName;
    // the list of value of the tpch.lineitem column to search over
    private static String[] columnValue;
    // the number of columns in the search query
    private static int columnCount;
    // the multiplicative shares for each column value
    private static String[][] multiplicativeShares;
    // stores seed value for client for random number generation
    private static int seedClient;

    // the number of row of tpch.lineitem considered
    private static int numRows;
    // the number of threads client program is running on
    private static int numThreads;
    // the number of row per thread
    private static int numRowsPerThread;

    // stores the result received/sent from/to combiner
    private static BigInteger[][] resultCombiner;
    // stores result received from servers
    private static final Set<Integer> result = Collections.synchronizedSet(new HashSet<Integer>());

    // used to calculate the time taken by client program
    private static final ArrayList<Instant> timestamps1 = new ArrayList<>();
    private static final ArrayList<Instant> timestamps2 = new ArrayList<>();
    private static final Logger log = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    // the name of file storing the query result under result/ folder
    private static final String resultFileName = "_04_OR_Search";

    static Map<String, Integer> tableMetadata = new HashMap<String, Integer>();


    // default constructor
    private Client04() {
    }

    // parametrised constructor
    public Client04(String IP, int port, String[] data) {
        this.IP = IP;
        this.port = port;
        this.data = data;
    }

    // operation performed by each thread
    private static class ParallelTask implements Runnable {
        private final int threadNum;

        public ParallelTask(int threadNum) {
            this.threadNum = threadNum;
        }

        @Override
        public void run() {
            int startRow = (threadNum - 1) * numRowsPerThread;
            int endRow = startRow + numRowsPerThread;
            Random randClient = new Random(seedClient);

            // evaluating which rows matches the requested query and storing row ids in result list
            for (int i = startRow; i < endRow; i++) {
                int randSeedClient = randClient.nextInt(Constants.getMaxRandomBound() - Constants.getMinRandomBound())
                        + Constants.getMinRandomBound();
                if (resultCombiner[0][i].intValue() == randSeedClient) { // of number of search columns is less than 3
                    result.add(i + 1);
                }
                if (resultCombiner.length > 1) { // of number of columns is equal to 4
                    if (resultCombiner[1][i].intValue() == randSeedClient) {
                        result.add(i + 1);
                    }
                }
            }
        }
    }

    // to interpolate the data received from the server
    private static void interpolation() {
        // the list containing all the threads
        List<Thread> threadList = new ArrayList<>();

        // create threads and add them to threadlist
        int threadNum;
        for (int i = 0; i < numThreads; i++) {
            threadNum = i + 1;
            threadList.add(new Thread(new ParallelTask(threadNum), "Thread" + threadNum));
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
                log.log(Level.SEVERE, ex.getMessage());
            }
        }
    }

    // receiving server data over the socket
    static class ReceiverSocket {

        private Socket socket;

        ReceiverSocket(Socket socket) {
            this.socket = socket;
        }

        @SuppressWarnings("unchecked")
        public void run() {
            try {
                // receiving the data from the Combiner
                ObjectInputStream inFromServer = new ObjectInputStream(socket.getInputStream());
                resultCombiner = (BigInteger[][]) inFromServer.readObject();
                // interpolating data to get results
                interpolation();
            } catch (IOException | ClassNotFoundException ex) {
                log.log(Level.SEVERE, ex.getMessage());
            }
        }
    }

    // starting to listen for incoming responses from servers
    private void startAsReceiver() {
        Socket socket;

        try {
            ServerSocket ss = new ServerSocket(clientPort);
            System.out.println("Client Listening........");
            // listening over socket for incoming connections
            socket = ss.accept();
            timestamps2.add(Instant.now());

            // processing data received from server
            new ReceiverSocket(socket).run();
            // printing result of the query
            Helper.printResult(result, resultFileName);

            timestamps2.add(Instant.now());
            int totalTime = Math.toIntExact(Helper.getProgramTimes(timestamps1).get(0)) +
                    Math.toIntExact(Helper.getProgramTimes(timestamps2).get(0));
//            System.out.println(totalTime);
            socket.close();
//            log.log(Level.INFO, "Total Client time:" + totalTime);
        } catch (IOException ex) {
            log.log(Level.SEVERE, ex.getMessage());
        }
    }

    // to send client data to servers
    private void startAsSender() {
        Socket socket;
        ObjectOutputStream outToServer;
        try {
            // socket creation and initialising output stream to write data
            socket = new Socket(IP, port);
            outToServer = new ObjectOutputStream(socket.getOutputStream());
            // writing data to stream
            outToServer.writeObject(data);
            // socket closed
            socket.close();
        } catch (IOException ex) {
            log.log(Level.SEVERE, ex.getMessage());
        }
    }

    @Override
    public void run() {
        startAsSender();
        super.run();
    }

    // extracts requires shares for a particular server
    private static String[] helper(int index) {
        String[] data = new String[columnCount];
        for (int j = 0; j < columnCount; j++) {
            data[j] = multiplicativeShares[j][index];
        }
        return data;
    }

    // prepares data to send to server and starts listening to target servers
    private static void doPostWork() {
        int numServers;
        Client04 server1, server2, server3 = null, server4 = null, combiner;

        // based on number of columns the number of server are chosen
        if (columnCount > 3) {
            numServers = 3;
        } else {
            numServers = columnCount + 1;
        }

        // server data preparation
        String[] data;

        data = new String[]{String.valueOf(numServers)};
        combiner = new Client04(combinerIP, combinerPort, data);

        data = new String[]{Helper.strArrToStr(columnName), Helper.strArrToStr(helper(0)), String.valueOf(seedClient)};
        server1 = new Client04(server1IP, server1Port, data);

        data = new String[]{Helper.strArrToStr(columnName), Helper.strArrToStr(helper(1)), String.valueOf(seedClient)};
        server2 = new Client04(server2IP, server2Port, data);

        if (numServers > 2) {
            data = new String[]{Helper.strArrToStr(columnName), Helper.strArrToStr(helper(2)), String.valueOf(seedClient)};
            server3 = new Client04(server3IP, server3Port, data);
        }

        if (numServers > 3) {
            data = new String[]{Helper.strArrToStr(columnName), Helper.strArrToStr(helper(3)), String.valueOf(seedClient)};
            server4 = new Client04(server4IP, server4Port, data);
        }

        // sending data to each server
        combiner.start();
        server1.start();
        server2.start();

        if (numServers > 2) {
            server3.start();
        }

        if (numServers > 3) {
            server4.start();
        }

        // started to listen for incoming responses from servers
        timestamps1.add(Instant.now());
        Client04 client = new Client04();
        client.startAsReceiver();
    }

    /**
     * The function creates multiplicative secret share using Shamir Secret Sharing
     *
     * @param value : the secret whose share is to be created
     * @param serverCount: the number of shares that is to be created based on number of servers
     * @return a list of shares of secret of length 'serverCount'
     */
   
    private static String[] shamirSecretSharingString(String value, int serverCount) {
        Random rand = new Random(1);
        // storing the slope value for the line
        int coefficient = rand.nextInt(2);
        // stores shares of the secret
        String[] share = new String[serverCount];
        // for value of x starting from 1, evaluates the share for  'value'
        for (int i = 0; i < serverCount; i++) {
            share[i] = BigInteger.valueOf((i + 1) * coefficient).add(new BigInteger(value)).toString();
        }
        return share;
    }

    // based on number of columns searched over creates shares of the column values
    private static void doWork() {
        if (columnCount > 3) {
            for (int i = 0; i < columnCount; i++)
                if (getColumnType(columnName[i]) == 0)
                    multiplicativeShares[i] = shamirSecretSharingString(columnValue[i], columnCount - 1);
                else
                    multiplicativeShares[i] = shamirSecretSharingString(columnValue[i], columnCount - 1);
        } 
        else { 
            for (int i = 0; i < columnCount; i++)
                if (getColumnType(columnName[i]) == 0)
                    multiplicativeShares[i] = shamirSecretSharingString(columnValue[i], columnCount + 1);
                else
                    multiplicativeShares[i] = shamirSecretSharingString(columnValue[i], columnCount + 1);

                for(int j = 0; j < multiplicativeShares[0].length; j++){
                }
        }
    }

    /**
     * It performs initialization tasks
     * @param args takes as string a list of column name and column value e.g. "suppkey,145,linenumber,1,partkey,12"
     */
    private static void doPreWork(String[] args) {

        String query = args[0];

        // splitting the argument value to extract column names and values to be searched
        String[] querySplit = query.split(",");
        columnCount = querySplit.length / 2;
        columnName = new String[columnCount];
        columnValue = new String[columnCount];
        for (int i = 0; i < columnCount; i++) {
            columnName[i] = "M_" + querySplit[2 * i];
            columnValue[i] = querySplit[2 * i + 1];
        }

        // reads configuration properties of the client
        String pathName = "config/Client.properties";
        Properties properties = Helper.readPropertiesFile(pathName);

        seedClient = Integer.parseInt(properties.getProperty("seedClient"));

        numRows = Integer.parseInt(properties.getProperty("numRows"));
        numThreads = Integer.parseInt(properties.getProperty("numThreads"));
        numRowsPerThread = numRows / numThreads;

        clientPort = Integer.parseInt(properties.getProperty("clientPort"));
        server1IP = properties.getProperty("server1IP");
        server1Port = Integer.parseInt(properties.getProperty("server1Port"));
        server2IP = properties.getProperty("server2IP");
        server2Port = Integer.parseInt(properties.getProperty("server2Port"));
        server3IP = properties.getProperty("server3IP");
        server3Port = Integer.parseInt(properties.getProperty("server3Port"));
        server4IP = properties.getProperty("server4IP");
        server4Port = Integer.parseInt(properties.getProperty("server4Port"));
        combinerIP = properties.getProperty("combinerIP");
        combinerPort = Integer.parseInt(properties.getProperty("combinerPort"));

        int resultDim = 1;
        if (columnCount > 3)
            resultDim = 2;
        resultCombiner = new BigInteger[resultDim][numRows];
        multiplicativeShares = new String[columnCount][columnCount + 1];
    }


    private static void loadTableMetaData(){
        // Open csv file at "data/metadata/table_metadata.csv" and load into tableMetaData hasmap
        String csvFile = "data/metadata/table_metadata.csv";
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";
        try {
            br = new BufferedReader(new FileReader(csvFile));
            while ((line = br.readLine()) != null) {
                String[] table_metadata = line.split(cvsSplitBy);
                tableMetadata.put("M_" + table_metadata[0], Integer.parseInt(table_metadata[1]));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static int getColumnType(String col_name){
        if(tableMetadata.isEmpty()){
            loadTableMetaData();
        }
        return tableMetadata.get(col_name);
    }

    /**
     * This program is used to perform 'or' operation over search keys belonging to multiple columns.
     *
     * @param args takes as string a list of column name and column value e.g. "suppkey,145,linenumber,1,partkey,12"
     * @throws InterruptedException
     */
    public static void main(String[] args) throws InterruptedException {
        timestamps1.add(Instant.now());

        doPreWork(args);

        doWork();

        doPostWork();
    }
}


