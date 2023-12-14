from subprocess import Popen
import time

# Compile

def compile_scripts():
    Popen("javac -cp \"C:/Users/shoum/Documents\VLDBPaperDemo/S2-VLDB-2023-main;C:/Users/shoum/Documents\VLDBPaperDemo/S2-VLDB-2023-main/mysqlConnector/mysql-connector-java-8.0.29.jar\" src/_00_Database_Table_Creator/*.java")

    Popen("javac -cp \"C:/Users/shoum/Documents\VLDBPaperDemo/S2-VLDB-2023-main;C:/Users/shoum/Documents\VLDBPaperDemo/S2-VLDB-2023-main/mysqlConnector/mysql-connector-java-8.0.29.jar\" src/_01_oneColumnNumberSearch/client/*.java src/_01_oneColumnNumberSearch/combiner/*.java src/_01_oneColumnNumberSearch/server/*.java")
    Popen("javac -cp \"C:/Users/shoum/Documents\VLDBPaperDemo/S2-VLDB-2023-main;C:/Users/shoum/Documents\VLDBPaperDemo/S2-VLDB-2023-main/mysqlConnector/mysql-connector-java-8.0.29.jar\" src/_02_oneColumnStringSearch/client/*.java src/_02_oneColumnStringSearch/combiner/*.java src/_02_oneColumnStringSearch/server/*.java")

    Popen("javac -cp \"C:/Users/shoum/Documents\VLDBPaperDemo/S2-VLDB-2023-main;C:/Users/shoum/Documents\VLDBPaperDemo/S2-VLDB-2023-main/mysqlConnector/mysql-connector-java-8.0.29.jar\" src/*.java")
    Popen("javac -cp \"C:/Users/shoum/Documents\VLDBPaperDemo/S2-VLDB-2023-main;C:/Users/shoum/Downloads/S2-VLDB-2023-original/S2-VLDB-2023-main/mysqlConnector/mysql-connector-java-8.0.29.jar\" utility/Helper.java")


def run_scripts():
    Popen("java -cp \"C:/Users/shoum/Documents\VLDBPaperDemo/S2-VLDB-2023-main;C:/Users/shoum/Documents\VLDBPaperDemo/S2-VLDB-2023-main/mysqlConnector/mysql-connector-java-8.0.29.jar\" src._02_oneColumnStringSearch.server.Server1 > prompt_logs/s1.txt", shell = True)
    Popen("java -cp \"C:/Users/shoum/Documents\VLDBPaperDemo/S2-VLDB-2023-main;C:/Users/shoum/Documents\VLDBPaperDemo/S2-VLDB-2023-main/mysqlConnector/mysql-connector-java-8.0.29.jar\" src._02_oneColumnStringSearch.server.Server2 > prompt_logs/s2.txt", shell = True)
    Popen("java -cp \"C:/Users/shoum/Documents\VLDBPaperDemo/S2-VLDB-2023-main;C:/Users/shoum/Documents\VLDBPaperDemo/S2-VLDB-2023-main/mysqlConnector/mysql-connector-java-8.0.29.jar\" src._02_oneColumnStringSearch.combiner.Combiner > prompt_logs/comb.txt", shell = True)

compile_scripts()
run_scripts()

Popen("clear")

time.sleep(1)
       
while True:
        
        

        query = input("> ")

        if query == "quit":
                break
        
        f = open("result/prompt.txt", "w")
        f.write("")

        if "enc create table" in query:
               # get the db and table name
                temp = query.replace("enc create table from ", "").split(" ")[0].split(".")

                # set the db and table name in config/userinfo.properties
                f = open("config/userinfo.properties", "w")
                f.write(f"dbName={temp[0]}\n")
                f.write(f"tableName={temp[1]}\n")
                f.close()
                

        Popen(f"java -cp \"C:/Users/shoum/Documents\VLDBPaperDemo/S2-VLDB-2023-main;C:/Users/shoum/Documents\VLDBPaperDemo/S2-VLDB-2023-main/mysqlConnector/mysql-connector-java-8.0.29.jar\" src/QueryParser \"{query}\" > prompt_logs/client.txt")
        
        while True:
                f = open("result/prompt.txt", "r")
                contents = f.read()
                if contents != "":
                        print(contents)
                        f.close()
                        break
                f.close()