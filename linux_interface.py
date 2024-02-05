import time
from configobj import ConfigObj
import subprocess
import sys
import os
import glob

classPath = ".:mysqlConnector/mysql-connector-java-8.0.29.jar"

subprocesses = []

# Compile
def compile_scripts():
    # Database Table Creator
    #Popen("javac -cp " + classPath + " src/_00_Database_Table_Creator/*.java")
    subprocess.run(["javac", "-cp", classPath, "src/_00_Database_Table_Creator/Database_Table_Creator.java"])

    # One Column Number Search
    #Popen("javac -cp " + classPath + " src/_01_oneColumnNumberSearch/client/*.java src/_01_oneColumnNumberSearch/combiner/*.java src/_01_oneColumnNumberSearch/server/*.java")
    subprocess.run(["javac", "-cp", classPath, "src/_01_oneColumnNumberSearch/client/Client01.java"])
    
    # One Column String Search
    #Popen("javac -cp " + classPath + " src/_02_oneColumnStringSearch/client/*.java src/_02_oneColumnStringSearch/combiner/*.java src/_02_oneColumnStringSearch/server/*.java")
    subprocess.run(["javac", "-cp", classPath, "src/_02_oneColumnStringSearch/client/Client02.java"])

    # AND Search
    #Popen("javac -cp " + classPath + " src/_03_AND_Search/client/*.java src/_03_AND_Search/combiner/*.java src/_03_AND_Search/server/*.java")
    subprocess.run(["javac", "-cp", classPath, "src/_03_AND_Search/client/Client03.java"])

    # OR Search
    #Popen("javac -cp " + classPath + " src/_04_OR_Search/client/*.java src/_04_OR_Search/combiner/*.java src/_04_OR_Search/server/*.java")
    subprocess.run(["javac", "-cp", classPath, "src/_04_OR_Search/client/Client04.java"])
    subprocess.run(["javac", "-cp", classPath, "src/_04_OR_Search/combiner/Combiner.java"])

    # Multiplicative Row Fetch
    #Popen("javac -cp " + classPath + " src/_05_Multiplicative_Row_Fetch/client/*.java src/_05_Multiplicative_Row_Fetch/combiner/*.java src/_05_Multiplicative_Row_Fetch/server/*.java")
    subprocess.run(["javac", "-cp", classPath, "src/_05_Multiplicative_Row_Fetch/client/Client05.java"])

    # Helper
    #Popen("javac -cp " + classPath + " utility/Helper.java")
    subprocess.run(["javac", "-cp", classPath, "utility/Helper.java"])

    # Everything Else
    #Popen("javac -cp " + classPath + " src/*.java")
    subprocess.run(["javac", "-cp", classPath, "src/server1.java"])
    subprocess.run(["javac", "-cp", classPath, "src/server2.java"])
    subprocess.run(["javac", "-cp", classPath, "src/server3.java"])
    subprocess.run(["javac", "-cp", classPath, "src/server4.java"])
    subprocess.run(["javac", "-cp", classPath, "src/combiner.java"])
    subprocess.run(["javac", "-cp", classPath, "src/QueryParser.java"])
    subprocess.run(["javac", "-cp", classPath, "src/convertCSV.java"])
    
def run_scripts():
    ao_scripts = []
    ao_scripts.append(f"exec java -cp {classPath} src/server1 > prompt_logs/s1.txt")
    ao_scripts.append(f"exec java -cp {classPath} src/server2 > prompt_logs/s2.txt")
    ao_scripts.append(f"exec java -cp {classPath} src/server3 > prompt_logs/s3.txt")
    ao_scripts.append(f"exec java -cp {classPath} src/server4 > prompt_logs/s4.txt")
    ao_scripts.append(f"exec java -cp {classPath} src/combiner > prompt_logs/comb1.txt")
    ao_scripts.append(f"exec java -cp {classPath} src/_04_OR_Search/combiner/Combiner > prompt_logs/comb2.txt")

    for ao_script in ao_scripts:
        p = subprocess.Popen(ao_script,shell=True)
        subprocesses.append(p)

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

def kill_processes():
    for p in subprocesses:
        p.kill()

def quitPrompt():
    kill_processes()
    print("\nByeeee")
    sys.exit()

def check_compiled():
    class_files_pattern = os.path.join('src', '**', '*.class')

    # Use glob to find all .class files in the directory and its subdirectories
    class_files = glob.glob(class_files_pattern, recursive=True)

    if not class_files:
        return False
    else:
        return True

try:
    # check if src has class files. if it doesn't, run compile_scripts()
    # if it does, run run_scripts()

    if not check_compiled():
        compile_scripts()
    run_scripts()

    subprocess.run(["clear"])

    time.sleep(1)
     
    while True:
            
            query = input("> ")

            if query == "quit":
                    break
            
            if query == "compile":
                print("\nRecompiling...")
                compile_scripts()
                kill_processes()
                run_scripts()
                print("Restarted Kernel")
                continue
            
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
                    #Popen("java -cp " + classPath + f" src/QueryParser \"getdbtbinfo\" {names[0]} {names[1]} > prompt_logs/client.txt")
                    subprocess.run(["java", "-cp", classPath, "src/QueryParser", "getdbtbinfo", names[0], names[1], ">", "prompt_logs/client.txt"])

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

            #Popen("java -cp " + classPath + f" src/QueryParser \"{query}\" > prompt_logs/client.txt")
            subprocess.run(["java", "-cp", classPath, "src/QueryParser",  query , ">", "prompt_logs/client.txt"])
            
            # Check for finish flag from the query parser
            while True:
                    f = open("result/prompt.txt", "r")
                    contents = f.read()
                    if contents != "":
                            f.close()
                            break
                    f.close()

except KeyboardInterrupt:
    quitPrompt()
      
quitPrompt()