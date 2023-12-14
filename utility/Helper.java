package utility;

import java.io.*;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import java.math.BigInteger;

public class Helper {

    private static final String mainDir = "";
    private static final Logger log = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    private static LinkedHashMap<String, String> columnList = new LinkedHashMap<>();
    private static String tableName = "lineitem";
    private static String databaseName = "tpch";
    private static int noOfColumns;

    private static String mulMod = "9794379537450709974983168981399384873473832303";
    private static int addMod = 100000007;


    public static Boolean getServer() {
        return true;
    }

    public static String getDBUser() {
        return "root";
    }

    public static String getDBPass() {
        return "password";
    }

    public static String getConPath() {
        // MySql
        if (getServer())
            return "jdbc:mysql://localhost:3306?allowLoadLocalInfile=true";
        else
            return "null";
    }

    public static Connection getConnection() throws SQLException {
        if (getServer())
            return DriverManager.getConnection(getConPath(), getDBUser(), getDBPass());
        else
            return DriverManager.getConnection(getConPath());
    }


    public static String getTablePrefix() {
        if (getServer())
            return "tpch.";
        else
            return "";
    }


    public static String getMainDir() {
        return mainDir;
    }

    public static Properties readPropertiesFile(String fileName) {
        FileInputStream fileInputStream;
        Properties properties = null;
        try {
            fileInputStream = new FileInputStream(fileName);
            properties = new Properties();
            properties.load(fileInputStream);
        } catch (IOException ioException) {
            log.log(Level.SEVERE, ioException.getMessage());
        }
        return properties;
    }



    // **********************************************8
    // Multiplicative Mod Functions

    public static BigInteger mod(BigInteger number) {
        BigInteger modulo = new BigInteger(mulMod);
        number = number.mod(modulo);
        if (number.compareTo(new BigInteger("0")) < 0)
            number = number.add(modulo);
        return number;
    }

    // **********************************************8
    // Additive Mod Functions

    public static int mod(int number) {
        number = number%addMod;
        if (number < 0)
            number += addMod;
        return number;
    }

    public static long mod(long number) {
        number = number%addMod;
        if (number < 0)
            number += addMod;
        return number;
    }

    
    /*
    public static int mod(int number) {
        BigInteger modulo = new BigInteger(modVal);
        number = BigInteger.valueOf(number).mod(modulo).intValue();
        if (number < 0)
            number = BigInteger.valueOf(number).add(modulo).intValue();
        return number;
    }
 
    public static long mod(long number) {
        BigInteger modulo = new BigInteger(modVal);
        number = BigInteger.valueOf(number).mod(modulo).longValueExact();
        if (number < 0)
            number = BigInteger.valueOf(number).add(modulo).longValueExact();
        return number;
    }
    */





    public static int[] stringToIntArray(String data) {
        int[] result = new int[data.length()];
        for (int i = 0; i < data.length(); i++) {
            result[i] = (data.charAt(i) - '0');
        }
        return result;
    }

    public static int[] tripletStringToIntArray(String data) {
        int numChars = (int)data.length()/3;
        int[] result = new int[numChars];
        for (int i = 0; i < numChars; i++) {
            result[i] = Integer.parseInt(data.substring(i*3, (i+1)*3));
        }
        return result;
    }

    public static void printResult(List<Integer> result, String fileName) throws IOException {
        System.out.println("The number of rows matching the query is " + result.size());

        System.out.println(result);

        FileWriter writer = new FileWriter(mainDir + "result/" + fileName);
        BufferedWriter bufferedWriter = new BufferedWriter(writer);
        for (int data:result) {
            bufferedWriter.append(String.valueOf(data)).append(",");
        }
        bufferedWriter.close();
    }

    public static void printResult(Set<Integer> result, String fileName) throws IOException {
        System.out.println("The number of rows matching the query is " + result.size());

        FileWriter writer = new FileWriter(mainDir + "result/" + fileName);
        BufferedWriter bufferedWriter = new BufferedWriter(writer);
        for (int data:result) {
            bufferedWriter.append(String.valueOf(data)).append(",");
        }
        bufferedWriter.close();
    }

    public static void printResult(int[][] result, int[] query, String fileName) throws IOException {

        FileWriter writer = new FileWriter(mainDir + "result/" + fileName);
        BufferedWriter bufferedWriter = new BufferedWriter(writer);
        for (int i = 0; i < result.length; i++) {
            bufferedWriter.append(String.valueOf(query[i] + 1)).append("\n");
            String temp="";
            for (int j = 0; j < result[0].length; j++) {
                bufferedWriter.append(String.valueOf(result[i][j])).append(",");
                temp+=result[i][j]+",";
            }
            System.out.println(temp);
            bufferedWriter.append("\n");
        }
        bufferedWriter.close();
    }

    public static void printResult(BigInteger[][] result, int[] query, String fileName, ArrayList<String> colNames, ArrayList<Integer> colTypes) throws IOException {

        FileWriter writer = new FileWriter(mainDir + "result/" + fileName);
        BufferedWriter bufferedWriter = new BufferedWriter(writer);

        ArrayList<Integer> colWidths = new ArrayList<>();

        String header = "";

        // Print column names, and pad each name so that the total length is 15 characters
        for (int i = 0; i < colNames.size(); i++) {
            String colName = colNames.get(i);
            header += colName;
            header += "     ";
            colWidths.add(colName.length() + 5);
        }

        System.out.println(header);



        for (int i = 0; i < result.length; i++) {
            bufferedWriter.append(String.valueOf(query[i] + 1)).append("\n");
            String temp="";
            for (int j = 0; j < result[0].length; j++) {
                String temp_val = result[i][j].toString();
                if(colTypes.get(j)!=0)
                    temp_val = Helper.ascii_reverse(temp_val);
                bufferedWriter.append((temp_val)).append(",");
                if(temp_val.length()>colWidths.get(j))
                    temp_val = temp_val.substring(0,colWidths.get(j)-6)+"...";
                String spaces = "";
                for (int k = 0; k < colWidths.get(j) - temp_val.length(); k++) {
                    spaces += " ";
                }
                temp+=temp_val+spaces;
            }
            temp = temp.substring(0, temp.length() - 1);
            System.out.println(temp);
            bufferedWriter.append("\n");
        }
        bufferedWriter.close();
    }

    public static ArrayList<Long> getProgramTimes(ArrayList<Instant> timestamps) {

        ArrayList<Long> durations = new ArrayList<>();

        for (int i = 0; i < timestamps.size() - 1; i++) {
            durations.add(Duration.between(timestamps.get(i), timestamps.get(i + 1)).toMillis());
        }

        return durations;
    }

    public static String strArrToStr(String[] arr) {
        ArrayList<String> arrAsList = new ArrayList<>(Arrays.asList(arr));
        return arrAsList.stream().map(Object::toString).collect(Collectors.joining(", "));
    }

    public static String arrToStr(int[] arr) {
        ArrayList<Integer> arrAsList = new ArrayList<>();
        for (Integer num : arr)
            arrAsList.add(num);
        return arrAsList.stream().map(Object::toString).collect(Collectors.joining(", "));
    }

    public static String arrToStr(BigInteger[] arr) {
        ArrayList<BigInteger> arrAsList = new ArrayList<>();
        for (BigInteger num : arr)
            arrAsList.add(num);
        return arrAsList.stream().map(Object::toString).collect(Collectors.joining(", "));
    }

    public static String arrToStr(int[][] arr) {
        String str = Arrays.deepToString(arr);
        str = str.replaceAll("\\], \\[", "\n");
        str = str.replaceAll("\\], \\[", "");
        str = str.replaceAll("\\[\\[", "");
        str = str.replaceAll("\\]\\]", "");

        return str;
    }

    public static String arrToStr(BigInteger[][] arr) {
        String str = Arrays.deepToString(arr);
        str = str.replaceAll("\\], \\[", "\n");
        str = str.replaceAll("\\], \\[", "");
        str = str.replaceAll("\\[\\[", "");
        str = str.replaceAll("\\]\\]", "");

        return str;
    }

    public static String strArrToStr(String[][] arr) {
        String str = Arrays.deepToString(arr);
        str = str.replaceAll("\\], \\[", "\n");
        str = str.replaceAll("\\], \\[", "");
        str = str.replaceAll("\\[\\[", "");
        str = str.replaceAll("\\]\\]", "");

        return str;
    }

    public static int[][] strToStrArr1(String data) {
        String[] temp = data.split("\n");
        int[][] result = new int[temp.length][];

        int count = 0;
        for (String line : temp) {
            result[count++] = Stream.of(line.split(", "))
                    .mapToInt(Integer::parseInt)
                    .toArray();
        }
        return result;
    }

    public static int[] strToArr(String str) {
        ArrayList<Integer> arrList = new ArrayList<>();
        String temp[];

        if (str.contains(", "))
            temp = str.split(", ");

        else if (str.contains("|"))
            temp = str.split("\\|");

        else
            temp = new String[]{str};


        for (String val : temp) {
            arrList.add(Integer.parseInt(val));
        }
        int[] result = new int[arrList.size()];

        for (int i = 0; i < result.length; i++) {
            result[i] = arrList.get(i);
        }

        return result;
    }

    public static String convertMillisecondsToHourMinuteAndSeconds(long milliseconds) {
        long seconds = (milliseconds / 1000) % 60;
        long minutes = (milliseconds / (1000 * 60)) % 60;
        long hours = (milliseconds / (1000 * 60 * 60)) % 24;
        return String.format("%02d:%02d:%02d", hours, minutes, seconds);
    }

    public static void progressBar(double percentInp, long timeSinceStart) {
        int percent = (int) (percentInp + 0.5);
        if (percent == 0)
            percent = 1;

        String bar = "|";
        String progress = "";
        for (int i = 0; i < percent / 2 - 1; i++)
            progress += "=";
        if (percent == 100)
            progress += "=";
        else
            progress += ">";
        for (int i = 0; i < 50 - percent / 2; i++)
            progress += "-";
        String finalString = bar + progress + bar + " " + percent + "%  |  Est. Time Remaining: " + convertMillisecondsToHourMinuteAndSeconds(timeSinceStart * (100 - percent) / percent) + "        ";
        if (percent != 100)
            finalString += " \r";
        else
            finalString += " \n";
        System.out.print(finalString);
    }


    public static String arrToStr(long[] arr) {
        ArrayList<Long> arrAsList = new ArrayList<>();
        for (Long num : arr)
            arrAsList.add(num);
        return arrAsList.stream().map(Object::toString).collect(Collectors.joining(", "));
    }


    public static String arrToStr(String[][] arr) {
        String str = Arrays.deepToString(arr);
        str = str.replaceAll("\\], \\[", "\n");
        str = str.replaceAll("\\], \\[", "");
        str = str.replaceAll("\\[\\[", "");
        str = str.replaceAll("\\]\\]", "");

        return str;
    }

    public static String arrToStr(long[][] arr) {
        String str = Arrays.deepToString(arr);
        str = str.replaceAll("\\], \\[", "\n");
        str = str.replaceAll("\\], \\[", "");
        str = str.replaceAll("\\[\\[", "");
        str = str.replaceAll("\\]\\]", "");

        return str;
    }

    public static <T> String listToStr(ArrayList<T> list) {
        return list.stream().map(Object::toString).collect(Collectors.joining(", "));
    }


    public static long[] strToArr1(String str) {
        ArrayList<Long> arrList = new ArrayList<>();
        String temp[];

        if (str.contains(", "))
            temp = str.split(", ");

        else if (str.contains("|"))
            temp = str.split("\\|");

        else
            temp = new String[]{str};


        for (String val : temp) {
            arrList.add(Long.parseLong(val));
        }
        long[] result = new long[arrList.size()];

        for (int i = 0; i < result.length; i++) {
            result[i] = arrList.get(i);
        }

        return result;
    }

    public static int[][] strToArr(ArrayList<String> list, int startRow, int endRow) {
        int numValsInRow = Helper.strToArr(list.get(startRow)).length;

        int[][] result = new int[endRow - startRow][numValsInRow];

        for (int i = startRow; i < endRow; i++) {
            int[] arr = Helper.strToArr(list.get(i));
            System.arraycopy(arr, 0, result[i - startRow], 0, numValsInRow);
        }

        return result;
    }

    public static long[][] strToArr1(ArrayList<String> list, int startRow, int endRow) {
        int numValsInRow = Helper.strToArr1(list.get(startRow)).length;

        long[][] result = new long[endRow - startRow][numValsInRow];

        for (int i = startRow; i < endRow; i++) {
            long[] arr = Helper.strToArr1(list.get(i));
            System.arraycopy(arr, 0, result[i - startRow], 0, numValsInRow);
        }

        return result;
    }

    public static String[] strToStrArr(String str) {
        String result[] = str.split(", ");
        return result;
    }

    public static BigInteger[] strToBiArr(String str) {
        String result_str[] = str.split(", ");
        BigInteger result[] = new BigInteger[result_str.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = new BigInteger(result_str[i]);
        }
        return result;
    }

    public static void getMetadata() {
        Connection con = null;
        try {
                con = Helper.getConnection();
        } catch (SQLException e) {
                e.printStackTrace();
        }
        Statement stmt;
        ResultSet rs;

        try {
                stmt = con.createStatement();
                rs = stmt.executeQuery("show COLUMNS from " + databaseName + "." + tableName);

                while(rs.next()){
                        String col_type = rs.getString("Type");
                        columnList.put(rs.getString("Field"),col_type);
                }

                noOfColumns = columnList.size();
        } catch (Exception e) {
                e.printStackTrace();
        }
    }

    public static String getTableName(){
        return tableName;
    }

    public static String getDatabaseName(){
        return databaseName;
    }

    public static int getNoOfColumns(){
        return noOfColumns;
    }

    public static LinkedHashMap<String,String> getColumnList(){
        return columnList;
    }

    public static int[] generateRandomArr(int[] intArray) {
        int[] randomArray = new int[intArray.length];
        Random random = new Random();
        for (int i = 0; i < intArray.length; i++) {
            randomArray[i] = random.nextInt(intArray[i]);
        }
        return randomArray;
    }
    public static int[] separateNumericIntoArr(String numeric) {
        int[] intArray = new int[numeric.length() / 2];
        for (int i = 0; i < numeric.length(); i += 2) {
            intArray[i / 2] = Integer.parseInt(numeric.substring(i, i + 2));
        }
        return intArray;
    }
    public static int[] generateSubtractedArr(int[] intArray,int[] randArray) {
        int[] resultArray = new int[intArray.length];
        for (int i = 0; i < intArray.length; i++) {
            resultArray[i] = intArray[i] - randArray[i];
        }
        return resultArray;
    }
    public static String arrayToString(int[] intArray) {
        StringBuilder sb = new StringBuilder();
        for (int i : intArray) {
            sb.append(i);
            sb.append("|");
        }
        return sb.toString();
    }
    public static void duplicateFile(String sourceFileName, String destFileName) throws IOException {
        FileReader sourceReader = new FileReader(sourceFileName);
        FileWriter destWriter = new FileWriter(destFileName);
        int ch;
        while ((ch = sourceReader.read()) != -1) {
            destWriter.write(ch);
        }
        sourceReader.close();
        destWriter.close();
    }
    public static void mergeCsvFiles(String firstCsvFile, String secondCsvFile, String outputCsvFile) throws IOException {
        List<String[]> firstCsvData = readCsvFile(firstCsvFile);
        List<String[]> secondCsvData = readCsvFile(secondCsvFile);
        List<String[]> mergedData = mergeCsvData(firstCsvData, secondCsvData);
        writeCsvFile(mergedData, outputCsvFile);
    }

    private static List<String[]> readCsvFile(String csvFile) throws IOException {
        List<String[]> csvData = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] rowData = line.split(",");
                csvData.add(rowData);
            }
        }
        return csvData;
    }

    private static List<String[]> mergeCsvData(List<String[]> firstCsvData, List<String[]> secondCsvData) {
        List<String[]> mergedData = new ArrayList<>();
        for (int i = 0; i < firstCsvData.size(); i++) {
            String[] firstRow = firstCsvData.get(i);
            String[] secondRow = secondCsvData.get(i);
            String[] mergedRow = new String[firstRow.length + secondRow.length];
            System.arraycopy(firstRow, 0, mergedRow, 0, firstRow.length);
            System.arraycopy(secondRow, 0, mergedRow, firstRow.length, secondRow.length);
            mergedData.add(mergedRow);
        }
        return mergedData;
    }

    private static void writeCsvFile(List<String[]> csvData, String outputCsvFile) throws IOException {
        try (FileWriter writer = new FileWriter(outputCsvFile)) {
            for (String[] rowData : csvData) {
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < rowData.length; i++) {
                    sb.append(rowData[i]);
                    if (i != rowData.length - 1) {
                        sb.append(",");
                    }
                }
                sb.append("\n");
                writer.write(sb.toString());
            }
        }
    }
    public static String numericToString(int[] data){
        String result="";
        for(int i=0;i<data.length;i++){
            String temp=String.valueOf(data[i]);
            for(int j=0;j<temp.length();j+=2){
                int num=Integer.parseInt(temp.substring(j,j+2));
                if(num!=99){
                    result+=((char)(num+ 86));
                }
            }
        }
        System.out.print(result+",");
        return  result;
    }

    public static String ascii_reverse(String inp){

        // Pad start of input with 0s until string length is a multiple of 3
        while(inp.length()%3!=0){
            inp="0"+inp;
        }
        
        // Convert each set of three digits to a character
        String result="";
        for(int i=0;i<inp.length();i+=3){
            int num=Integer.parseInt(inp.substring(i,i+3));
            if(num!=999){
                result+=((char)(num));
            }
        }

        return result;
    }

    public static String dateToString(int data){
        String result="";
        String temp=String.valueOf(data);
        result+=temp.substring(0,2)+"/"+temp.substring(2,4)+"/"+temp.substring(4);
        System.out.print(result+",");
        return  result;
    }
}