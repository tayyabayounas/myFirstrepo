import time
import sys
i=0
filehandler = open("Movies-2018.csv","r", encoding="latin-1")
rows = filehandler.readlines()
for row in rows:
    temp_file = open("data/myfile"+str(i)+".csv","w+") 
    temp_file.write(row) 
    temp_file.close()   
    time.sleep(1)
    i=i+1 
    if(i==20):
        break