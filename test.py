from subprocess import Popen
import time
from configobj import ConfigObj

classPath = ".:mysqlConnector/mysql-connector-java-8.0.29.jar"
Popen("javac -cp " +  classPath + " src/_00_Database_Table_Creator/*.java", shell=True)