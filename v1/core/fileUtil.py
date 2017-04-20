#coding=utf-8
import os
STORE_PID_PATH = os.path.split(os.path.realpath(__file__))[0] + "/ccpid"

'''
@pid the format of list with all pid
store pid as format of linux process id
1.apped to file
2.make a set
'''
def store_pid(pid, pidPath):
    #open the file,if exist will overwrite
    store_list = pid
    store_set = set(store_list)
    store_list = list(store_set)
    
    store_file = open(pidPath,'w')
    for x in xrange(0,len(store_list)):
        store_file.write(str(store_list[x])+"\n")  
    store_file.close()

def is_exist(pidPath):
    if os.path.exists(pidPath):
        return True
    else:
        return False
