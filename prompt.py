from subprocess import Popen
import time
from configobj import ConfigObj

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
            Popen("java -cp \"" + classPath + "\" src/" + folderNames[i] + "/server/Server" + str(j+1) + " > prompt_logs/s" + str(serverCounter) + ".txt", shell = False)
            serverCounter += 1
        Popen("java -cp \"" + classPath + "\" src/" + folderNames[i] + "/combiner/Combiner > prompt_logs/comb" + str(i)  + ".txt", shell = False)


def run_scripts_test():
    Popen("java -cp \"" + classPath + "\" src/server1" + " > prompt_logs/s" + "1" + ".txt", shell = False)
    Popen("java -cp \"" + classPath + "\" src/server2" + " > prompt_logs/s" + "2" + ".txt", shell = False)
    Popen("java -cp \"" + classPath + "\" src/server3" + " > prompt_logs/s" + "3" + ".txt", shell = False)
    Popen("java -cp \"" + classPath + "\" src/server4" + " > prompt_logs/s" + "4s  " + ".txt", shell = False)
    Popen("java -cp \"" + classPath + "\" src/combiner" + " > prompt_logs/comb1" + ".txt", shell = False)
    Popen("java -cp \"" + classPath + "\" src/_04_OR_Search/combiner/Combiner" + " > prompt_logs/comb2" + ".txt", shell = False)

def getRowCount(req):
        f = open("config/encryptedSchemas.properties", "r")
        contents = f.read()
        for line in contents.split("\n"):
            if req.lower() in line.lower():
                f.close()
                return line.split(".")[2]
        f.close()
        return -1

def updateConfigFiles(db,tbl,r):
      # set the db and table name in config/userinfo.properties
    f = open("config/userinfo.properties", "w")
    f.write(f"dbName={db}\n")
    f.write(f"tableName={tbl}\n")
    f.write(f"numRows={r}\n")
    f.close()

    # update the config files
    config_files = ['Client', 'Combiner', 'Server1', 'Server2', 'Server3', 'Server4']
    for config_file in config_files:
        config = ConfigObj(f"config/{config_file}.properties")
        config['numRows'] = r
        config.write()  
        #f = open(f"config/{config_file}.properties", "w")
        #f.write(f"numRows={r}\n")
        #f.close()

compile_scripts()
run_scripts_test()

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
                names = query.replace("enc create table from ", "").split(".")

                # clear the numrow log
                f = open("result/numrows.txt", "w")
                f.write("")
                f.close()

                print("Fetching Metadata from table...")

                # get numrows
                numRows = 0
                Popen("java -cp \"" + classPath + f"\" src/QueryParser \"getdbtbinfo\" {names[0]} {names[1]} > prompt_logs/client.txt")

                # wait for the numrows to be written

                while True:
                    f = open("result/numrows.txt", "r")
                    contents = f.read()
                    if contents != "":
                            numRows = contents
                            f.close()
                            break
                    f.close()

                updateConfigFiles(names[0],names[1],numRows)

                # append all info in config/encryptedSchemas.properties
                f = open("config/encryptedSchemas.properties", "r+")
                contents = f.read().split("\n")
                update = False
                new_contents = []
                for schema in contents:
                    temp = schema.split(".")
                    if temp[0] == names[0] and temp[1] == names[1]:
                            temp[2] = numRows
                            update = True
                    new_contents.append(".".join(temp))

                if not update:
                        new_contents.append(f"{names[0]}.{names[1]}.{numRows}")

                f.seek(0)
                f.write("\n".join(new_contents))
                f.close()
        
        if "enc use" in query:
                # get the db and table name
                names = query.replace("enc use ", "").split(".")

                if getRowCount(query.replace("enc use ", "")) == -1:
                    print("The requested schema/table has not yet been encrypted. Please encrypt it first.")
                    continue

                updateConfigFiles(names[0],names[1],getRowCount(query.replace("enc use ", "")))
                print("Successfully switched to " + names[0] + "." + names[1] + ".")
                continue

        #if "'" in query:
            #query = query.replace("'", "'\\\"'")

        Popen("java -cp \"" + classPath + f"\" src/QueryParser \"{query}\" > prompt_logs/client.txt")
        
        # Check for finish flag from the query parser
        while True:
                f = open("result/prompt.txt", "r")
                contents = f.read()
                if contents != "":
                        f.close()
                        break
                f.close()