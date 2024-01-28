package src._05_Multiplicative_Row_Fetch.client;

import constant.Constants;
import utility.Helper;

import java.io.*;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Instant;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class Client05 extends Thread {

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

    // list of all row ids requested
    private static int[] queryList;
    // number of  row ids requested
    private static int querySize;
    // stores seed value for client for random number generation
    private static int seedClient;
    // in square matrix form the row to which row ids belong
    private static BigInteger[][] row_filter;
    // in square matrix form the column to which row ids belong
    private static BigInteger[][] col_filter;
    // for each server the row shares for row ids values
    private static BigInteger[][] server1RowShare;
    private static BigInteger[][] server2RowShare;
    private static BigInteger[][] server3RowShare;
    private static BigInteger[][] server4RowShare;
    // for each server the column shares for row ids values
    private static BigInteger[][] server1ColShare;
    private static BigInteger[][] server2ColShare;
    private static BigInteger[][] server3ColShare;
    private static BigInteger[][] server4ColShare;

    // the number of row of tpch.lineitem considered
    private static int numRows;
    // stores the result received/sent from/to combiner
    private static BigInteger[][] resultCombiner;

    // used to calculate the time taken by client program
    private static final ArrayList<Instant> timestamps1 = new ArrayList<>();
    private static final ArrayList<Instant> timestamps2 = new ArrayList<>();
    private static final Logger log = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    private static ArrayList<String> colNames = new ArrayList<>(); 
    private static ArrayList<Integer> colTypes = new ArrayList<>(); 

    // the name of file storing the query result under result/ folder
    private static final String resultFileName = "_05_Multiplicative_Row_Fetch";

    private static final int portIncrement = 0;

    private static String sum_col = "def";


    // default constructor
    private Client05() {
    }

    // parametrised constructor
    public Client05(String IP, int port, String[] data) {
        this.IP = IP;
        this.port = port;
        this.data = data;
    }

    // operation performed by each thread
    private static void interpolation() throws IOException {

        get_col_types();

        querySize = resultCombiner.length;
        Random randSeedClient = new Random(seedClient);

        // evaluating which rows matches the requested query and storing row ids in result list
        for (int i = 0; i < querySize; i++) {
            BigInteger randClient = BigInteger.valueOf(randSeedClient.nextInt(Constants.getMaxRandomBound() - Constants.getMinRandomBound())
                    + Constants.getMinRandomBound());

            for(int j = 0; j < resultCombiner[i].length; j++) {
                resultCombiner[i][j] = Helper.mod(resultCombiner[i][j].subtract(randClient));
            }
        }

        // we return the string equivalent of this function in the main function
        //Helper.printResult(resultCombiner, queryList, resultFileName, colNames, colTypes);

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
                // Receiving the data from the Combiner
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
            
            // listening over socket for incoming connections
            socket = ss.accept();
            timestamps2.add(Instant.now());

            // processing data received from server
            new ReceiverSocket(socket).run();

            timestamps2.add(Instant.now());
            int totalTime = Math.toIntExact(Helper.getProgramTimes(timestamps1).get(0)) +
                    Math.toIntExact(Helper.getProgramTimes(timestamps2).get(0));
//            System.out.println(totalTime);
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

    // prepares data to send to server and starts listening to target servers
    private static void doPostWork() {
        Client05 server1, server2, server3, server4;

        // server data preparation
        String[] data;

        data = new String[]{"row", Helper.arrToStr(server1RowShare), Helper.arrToStr(server1ColShare), String.valueOf(seedClient)};
        server1 = new Client05(server1IP, server1Port, data);

        data = new String[]{"row", Helper.arrToStr(server2RowShare), Helper.arrToStr(server2ColShare), String.valueOf(seedClient)};
        server2 = new Client05(server2IP, server2Port, data);

        data = new String[]{"row", Helper.arrToStr(server3RowShare), Helper.arrToStr(server3ColShare), String.valueOf(seedClient)};
        server3 = new Client05(server3IP, server3Port, data);

        data = new String[]{"row", Helper.arrToStr(server4RowShare), Helper.arrToStr(server4ColShare), String.valueOf(seedClient)};
        server4 = new Client05(server4IP, server4Port, data);

        // print server1RowShare and server1ColShare
        //System.out.println(Arrays.deepToString(server1RowShare));
        //System.out.println(Arrays.deepToString(server1ColShare));



        // sending data to each server
        server1.start();
        server2.start();
        server3.start();
        server4.start();

        // started to listen for incoming responses from servers
        timestamps1.add(Instant.now());
        Client05 client = new Client05();
        client.startAsReceiver();
    }

    /**
     * The function creates multiplicative secret share using Shamir Secret Sharing
     *
     * @param value : the secret whose share is to be created
     * @param serverCount: the number of shares that is to be created based on number of servers
     * @return a list of shares of secret of length 'serverCount'
     */
    private static BigInteger[] shamirSecretSharing(BigInteger value, int serverCount) {
        Random rand = new Random(69);
        // storing the slope value for the line
        int coefficient = rand.nextInt(Constants.getMaxRandomBound() - Constants.getMinRandomBound()) +
                Constants.getMinRandomBound();
        // stores shares of the secret
        BigInteger[] share = new BigInteger[serverCount];
        // for value of x starting from 1, evaluates the share for  'value'
        for (int i = 0; i < serverCount; i++) {
            share[i] = BigInteger.valueOf((i + 1) * coefficient).add(value);
        }
        return share;
    }

    /**
     * It performs initialization tasks
     */
    private static void doWork() {

        // evaluating the row and column filter based on items to be searched
        int filter_size = (int) (Math.ceil(Math.sqrt(numRows))); 
        for (int i = 0; i < querySize; i++) {
            row_filter[i][queryList[i] / filter_size] = BigInteger.valueOf(1);
            col_filter[i][queryList[i] % filter_size] = BigInteger.valueOf(1);
        }

        // The number of servers is 4
        int serverCount = 4;
        BigInteger[] row_split;
        BigInteger[] col_split;

        // creating shares for row and column filter values

        for (int i = 0; i < querySize; i++) {
            for (int j = 0; j < row_filter[i].length; j++) {
                row_split = shamirSecretSharing(row_filter[i][j], serverCount);
                col_split = shamirSecretSharing(col_filter[i][j], serverCount);
                server1RowShare[i][j] = row_split[0];
                server1ColShare[i][j] = col_split[0];
                server2RowShare[i][j] = row_split[1];
                server2ColShare[i][j] = col_split[1];
                server3RowShare[i][j] = row_split[2];
                server3ColShare[i][j] = col_split[2];
                server4RowShare[i][j] = row_split[3];
                server4ColShare[i][j] = col_split[3];
            }
        }
    }

    /**
     * It performs initialization tasks
     * @param args takes as string a list of row ids e.g. "1,2,5,6"
     */
    private static void doPreWork(String[] args) {

        String query = args[0];
        sum_col = args[1];

        // splitting the argument value to extract row ids to be searched
        queryList = Stream.of(query.split(","))
                .mapToInt(Integer::parseInt).map(i -> i - 1)
                .toArray();

        querySize = queryList.length;

        // reads configuration properties of the client
        String pathName = "config/Client.properties";
        Properties properties = Helper.readPropertiesFile( pathName);

        seedClient = Integer.parseInt(properties.getProperty("seedClient"));

        numRows = Integer.parseInt(properties.getProperty("numRows"));

        // the +10 prevents it clashing with one of the first four programs when doing a * query
        clientPort = Integer.parseInt(properties.getProperty("clientPort")) + portIncrement + 10; 
        server1IP = properties.getProperty("server1IP");
        server1Port = Integer.parseInt(properties.getProperty("server1Port")) + portIncrement;
        server2IP = properties.getProperty("server2IP");
        server2Port = Integer.parseInt(properties.getProperty("server2Port")) + portIncrement;
        server3IP = properties.getProperty("server3IP");
        server3Port = Integer.parseInt(properties.getProperty("server3Port")) + portIncrement;
        server4IP = properties.getProperty("server4IP");
        server4Port = Integer.parseInt(properties.getProperty("server4Port")) + portIncrement;

        int filter_size = (int) (Math.ceil(Math.sqrt(numRows))); 
        row_filter = new BigInteger[querySize][filter_size];
        col_filter = new BigInteger[querySize][filter_size];

        // initialize rowfilter and colfilter to 0
        for (int i = 0; i < querySize; i++) {
            for (int j = 0; j < filter_size; j++) {
                row_filter[i][j] = BigInteger.valueOf(0);
                col_filter[i][j] = BigInteger.valueOf(0);
            }
        }

        server1RowShare = new BigInteger[querySize][filter_size];
        server1ColShare = new BigInteger[querySize][filter_size];
        server2RowShare = new BigInteger[querySize][filter_size];
        server2ColShare = new BigInteger[querySize][filter_size];
        server3RowShare = new BigInteger[querySize][filter_size];
        server3ColShare = new BigInteger[querySize][filter_size];
        server4RowShare = new BigInteger[querySize][filter_size];
        server4ColShare = new BigInteger[querySize][filter_size];
    }

    private static void get_col_types(){
        Map<String, Integer> tableMetadata = new HashMap<String, Integer>();
        // Open csv file at "data/metadata/table_metadata.csv" and load into tableMetaData hasmap
        String csvFile = "data/metadata/table_metadata.csv";
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";
        try {
            br = new BufferedReader(new FileReader(csvFile));
            while ((line = br.readLine()) != null) {
                String[] table_metadata = line.split(cvsSplitBy);
                tableMetadata.put(table_metadata[0], Integer.parseInt(table_metadata[1]));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (Map.Entry<String, Integer> entry : tableMetadata.entrySet()) {
            colNames.add(entry.getKey());
            colTypes.add(entry.getValue());
        }
    }
    
    private static String calculateSum(){
        double sum = 0; 
        int sum_col_idx = colNames.indexOf(sum_col);
        HashMap<String, Integer> columnListWithNumTypes = Helper.getColumnListWithNumType();
        if(columnListWithNumTypes.get(sum_col) == 3){ // there is a decimal and thus we have to convert from string to numerical value
            for(int i = 0; i < resultCombiner.length; i++){
                double temp = Double.valueOf(Helper.ascii_reverse(resultCombiner[i][sum_col_idx].toString()));
                sum += temp;
            }
        }

        else{
            for(int i = 0; i < resultCombiner.length; i++){
                sum += resultCombiner[i][sum_col_idx].doubleValue();
            }
        }

        return String.valueOf(sum);
    }

    /**
     * This program is used to retrieve the records corresponding to requested row ids based on multiplicative shares.
     *
     * @param args takes as string a list of row ids e.g. "1,2,5,6"
     * @throws InterruptedException
     * @throws IOException
     */
    public static String main(String[] args) throws InterruptedException, IOException {
        timestamps1.add(Instant.now());

        doPreWork(args);

        doWork();

        doPostWork();

        if(sum_col.equals("NONE/NULL/??")){
            return Helper.rowFetchResultString(resultCombiner, queryList, resultFileName, colNames, colTypes);
        }
        else{
            return calculateSum();
        }
    }
}


