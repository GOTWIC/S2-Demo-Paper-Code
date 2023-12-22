package src;
import utility.Helper;

import java.io.*;
import java.util.*;


public class convertCSV {
    private static LinkedHashMap<String, Integer> columnList = null;
    static {
        Helper.getTableMetadata();
        columnList = Helper.getColumnList();
    }
    public static HashMap<String, Integer> getColumnList(){
        return columnList;
    }
    private static Properties properties;
    public static Properties getProperties(){
        return properties;
    }
    public static int maxLength = 15;
    public static int getMaxLength(){
        return maxLength;
    }


    private static final Map<Integer, String> map = new HashMap<Integer, String>();

    public static synchronized String get(int key) {
        return map.get(key);
    }

    public static synchronized int get2(int key) {
        return Integer.parseInt(map.get(key));
    }

    public static String writeData(String data, PrintWriter pw, boolean newLine) throws FileNotFoundException {
        if (newLine) {//handle new line
            pw.write(data + '\r'+'\n');
            return data;
        }
        pw.write(data + ",");//write converted data
        return data;
    }

    static public String stringToNumber(String a, PrintWriter pw, boolean newLine) throws FileNotFoundException {
        String char1 = a.toLowerCase();
        String charInt = "";

        for (int i = 0; i < a.length(); i++) {

            char char9 = char1.charAt(i);
            if (char9 == ' ') {
                String char2 = "37";
                charInt = charInt + char2;
                continue;
            }
            if (char9 == '.') {
                int char2 = 38;
                charInt = charInt + char2;
                continue;
            }
            if (Character.isLetter(char9)) {
                int char2 = char1.charAt(i) - 86;
                charInt = charInt + char2;
            } else {
                System.out.println("Invalid input.");
                return charInt;
            }
        }
        writeData(charInt, pw, newLine);
        return charInt;
    }

    static public long dateToNumber(String a, PrintWriter pw, boolean newLine) throws FileNotFoundException {
        long x = 0;
        a = a.replace("/", "");
        a = a.replace("-", "");
        writeData(a, pw, newLine);
        return x;
    }

    static public long intToNumber(String a, PrintWriter pw, boolean newLine) throws FileNotFoundException {
        long x = 0;
        a = a.replace(".", "");
        a = a.replace("-", "");
        writeData(a, pw, newLine);
        return x;
    }


    public static String readAndWriteData() throws IOException {
        PrintWriter pw = new PrintWriter(new FileWriter("data/cleartext/table_numeric.csv"));
        String data1 = "";
        boolean newLine;
        Scanner sc = new Scanner(new File("data/cleartext/table.csv"));


        //read first line(col names) and write into new converted file
        String firstLine= sc.nextLine();
        String[] columnNames=firstLine.split(",");
        pw.write(firstLine + '\r');

        int i = 0;
        sc.useDelimiter(",|\n");
        while (sc.hasNext()){
            newLine = false;
            data1 = sc.next();
            if(data1.charAt(data1.length()-1) == '\r') {
                newLine = true;
            }
            data1 = data1.replace("\n","").replace("\r", "");
            String data2 = String.valueOf(columnList.get(columnNames[i]));
            if(Objects.equals(data2, "string")) {
                //global max length
//                if(data1.length() > maxLength)
//                    maxLength = data1.length();
                stringToNumber(data1, pw, newLine);
            }
            if(Objects.equals(data2, "date")) {
                dateToNumber(data1, pw, newLine);
            }
            if(Objects.equals(data2, "number")) {
                intToNumber(data1, pw, newLine);
            }
            i++;
            if( i == columnList.size()) {
                i = 0;
            }
        }
        sc.close();  //closes the scanner
        pw.close(); // closes the writer
        addPadding();
        return data1;
    }

    public static void addPadding() throws IOException {
        int max = getMaxLength();
        //if(max%5 != 0)
        //max = max + (5-max%5);
        PrintWriter pw = new PrintWriter(new FileWriter("data/cleartext/table_preprocessed.csv"));
        String data1 = "";
        boolean newLine;
        Properties properties = getProperties();
        HashMap<String, Integer> colList = getColumnList();
        Scanner sc = new Scanner(new File("data/cleartext/table_numeric.csv")); //converted file

        //read first line(col names) and write into new converted file
        String firstLine= sc.nextLine();
        String[] columnNames=firstLine.split(",");
        pw.write(firstLine + '\r');
        int i = 0;
        sc.useDelimiter(",|\n");
        while (sc.hasNext()){
            newLine = false;
            data1 = sc.next();
            if(data1.charAt(data1.length()-1) == '\r') {
                newLine = true;
            }
            data1 = data1.replace("\n","").replace("\r", "");
            String data2 = String.valueOf(columnList.get(columnNames[i]));
            if(Objects.equals(data2, "string")) {
                while(data1.length()/2 < max) {
                    data1 = data1 + "99";
                }
            }
            if(newLine) {
                pw.write(data1 + '\r'+'\n');
                i=0;
            }
            else {
                pw.write(data1 + ",");
                i++;
            }
        }
        sc.close();
        pw.close();
    }


}
