#!/usr/local/bin/python

"""
A example script of testing and debugging. 

Author: Xiaomeng Chen
"""

from database import Database
from mi_parser import MiParser
from utils import *
from refine_data import *
from mi_parser import MiParser

from os import listdir
from os.path import isfile, join
import sys
import time

Config = ConfigParser.ConfigParser()
Config.read("config.ini")
DB_USER = Config.get('Database', 'user')
DB_PASSWORD = Config.get('Database', 'password')
DB_HOST = Config.get('Database', 'host')
DB_UNIX_SOCKET = Config.get('Database', 'unix_socket')
UNZIP_FOLDER = Config.get('Database', 'unzip_folder')

def main():
    db = Database(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, unix_socket=DB_UNIX_SOCKET)
    db.init_database()

    # put id of users here.
    users = []

    for user_id in users: 
        # put start_time and end_time here.
        start_time, end_time = []
        print user_id

        #print "get_mylog_running_time_and_insert_to_db"
        #get_mylog_running_time_and_insert_to_db(db, user_id, start_time, end_time) 
        #print "get_milog_running_time_and_insert_to_db"
        #get_milog_running_time_and_insert_to_db(db, user_id, start_time, end_time) 
        #print "get_running_time_and_insert_to_db"
        #get_running_time_and_insert_to_db(db, user_id, start_time, end_time)
        #db.commit()

        print "get_messages_and_insert_to_db"
        get_messages_and_insert_to_db(db, user_id, start_time, end_time)

        # get rrc failures which start from the first RRC Conn Req which causes failure
        #    and end up with the last RRC Conn Req which leads to establishment success
        get_rrc_failure_and_insert_to_db(db, user_id, start_time, end_time)
        get_rrc_success_and_insert_to_db(db, user_id, start_time, end_time)

        # Each rrc failure includes one or more RRC Establishment sections
        get_rrc_failure_sections_and_insert_to_db(db, user_id, start_time, end_time)

        update_last_msib_for_rrc_success_failure(db, user_id, start_time, end_time)
        update_next_msib_for_rrc_success_failure(db, user_id, start_time, end_time)

        sys.stdout.flush()
        db.commit()

    db.close()
    

if __name__ == '__main__':
    main()
    


