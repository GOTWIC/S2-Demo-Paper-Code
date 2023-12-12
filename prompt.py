from subprocess import Popen
import time

# Compile

base = "-cp \"C:/Users/shoum/Documents\VLDBPaperDemo/S2-VLDB-2023-main;C:/Users/shoum/Documents\VLDBPaperDemo/S2-VLDB-2023-main/mysqlConnector/mysql-connector-java-8.0.29.jar\" "
 

def compile_scripts():
    Popen("javac " + base + " src/_00_Database_Table_Creator/*.java")

    Popen("javac " + base + " src/_01_oneColumnNumberSearch/client/*.java src/_01_oneColumnNumberSearch/combiner/*.java src/_01_oneColumnNumberSearch/server/*.java")
    Popen("javac " + base + " src/_02_oneColumnStringSearch/client/*.java src/_02_oneColumnStringSearch/combiner/*.java src/_02_oneColumnStringSearch/server/*.java")

    Popen("javac " + base + " src/*.java")
    Popen("javac -cp \"C:/Users/shoum/Documents\VLDBPaperDemo/S2-VLDB-2023-main;C:/Users/shoum/Downloads/S2-VLDB-2023-original/S2-VLDB-2023-main/mysqlConnector/mysql-connector-java-8.0.29.jar\" utility/Helper.java")


def run_scripts():
    Popen("java " + base + " src._02_oneColumnStringSearch.server.Server1 > prompt_logs/s1.txt", shell = True)
    Popen("java " + base + " src._02_oneColumnStringSearch.server.Server2 > prompt_logs/s2.txt", shell = True)
    Popen("java " + base + " src._02_oneColumnStringSearch.combiner.Combiner > prompt_logs/comb.txt", shell = True)

def updateInfo(dbName, tableName):
    f = open("config/userinfo.properties", "w")
    f.write(f"dbName={dbName}\n")
    f.write(f"tableName={tableName}")
    f.close()

compile_scripts()
run_scripts()

Popen("clear")

time.sleep(1)

      
while True:
        
        query = input("> ")

        if query == "quit":
                break
        
        # add a backslash before each double quotation mark
        query = query.replace("\"", "\\\"")
        
        f = open("result/prompt.txt", "w")
        f.write("")

        if "enc use" in query.lower():
            temp = query.replace("enc use ", "").split(".")
            updateInfo(temp[0], temp[1])
            print("Successfully set database and table")
            continue 

        if "enc create table" in query.lower():
            temp = query.replace("enc create table from ", "").split(" ")[0].split(".")
            updateInfo(temp[0], temp[1])
            

        Popen(f"java " + base + " src/QueryParser \"{query}\" > prompt_logs/client.txt")
        
        while True:
                f = open("result/prompt.txt", "r")
                contents = f.read()
                if contents != "":
                        print(contents)
                        f.close()
                        break
                f.close()