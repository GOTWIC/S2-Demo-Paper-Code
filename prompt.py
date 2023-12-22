from subprocess import Popen
import time

basePath = "C:/Users/shoum/Documents/VLDBPaperDemo/S2-VLDB-2023-main"
classPath = basePath + ";" + basePath + "/mysqlConnector/mysql-connector-java-8.0.29.jar"

# Compile

def compile_scripts():
    # Database Table Creator
    Popen("javac -cp \"" + classPath + "\" src/_00_Database_Table_Creator/*.java")

    # One Column Number Search
    Popen("javac -cp \"" + classPath + "\" src/_01_oneColumnNumberSearch/client/*.java src/_01_oneColumnNumberSearch/combiner/*.java src/_01_oneColumnNumberSearch/server/*.java")
    
    # One Column String Search
    Popen("javac -cp \"" + classPath + "\" src/_02_oneColumnStringSearch/client/*.java src/_02_oneColumnStringSearch/combiner/*.java src/_02_oneColumnStringSearch/server/*.java")

    # AND Search
    Popen("javac -cp \"" + classPath + "\" src/_03_AND_Search/client/*.java src/_03_AND_Search/combiner/*.java src/_03_AND_Search/server/*.java")

    # OR Search
    Popen("javac -cp \"" + classPath + "\" src/_04_OR_Search/client/*.java src/_04_OR_Search/combiner/*.java src/_04_OR_Search/server/*.java")

    # Multiplicative Row Fetch
    Popen("javac -cp \"" + classPath + "\" src/_05_Multiplicative_Row_Fetch/client/*.java src/_05_Multiplicative_Row_Fetch/combiner/*.java src/_05_Multiplicative_Row_Fetch/server/*.java")

    # Helper
    Popen("javac -cp \"" + classPath + "\" utility/Helper.java")

    # Everything Else
    Popen("javac -cp \"" + classPath + "\" src/*.java")
    


def run_scripts():
    serverCounts = [2,2,2,4,4]
    folderNames = ["_01_oneColumnNumberSearch", "_02_oneColumnStringSearch", "_03_AND_Search", "_04_OR_Search", "_05_Multiplicative_Row_Fetch"]

    serverCounter = 0

    for i in range(len(serverCounts)):
        for j in range(serverCounts[i]):
            Popen("java -cp \"" + classPath + "\" src/" + folderNames[i] + "/server/Server" + str(j+1) + " > prompt_logs/s" + str(serverCounter) + ".txt", shell = True)
            serverCounter += 1

        Popen("java -cp \"" + classPath + "\" src/" + folderNames[i] + "/combiner/Combiner > prompt_logs/comb" + str(i)  + ".txt", shell = True)

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
        
        if "enc use" in query:
                # get the db and table name
                temp = query.replace("enc use ", "").split(".")

                # set the db and table name in config/userinfo.properties
                f = open("config/userinfo.properties", "w")
                f.write(f"dbName={temp[0]}\n")
                f.write(f"tableName={temp[1]}\n")
                f.close()

        if "\"" in query:
                query = query.replace("\"", "\\\"")

        Popen("java -cp \"" + classPath + f"\" src/QueryParser \"{query}\" > prompt_logs/client.txt")
        
        while True:
                f = open("result/prompt.txt", "r")
                contents = f.read()
                if contents != "":
                        print(contents)
                        f.close()
                        break
                f.close()