#!/usr/local/bin/python
"""
The main entry point of processing an uploaded file.

The uploaded file is in zip format and includes MI logs,
Mylogger logs and TCPdump logs of one user during one 
uploading interval. 

Author: Xiaomeng Chen
"""

import os
import sys
import re
import zipfile
import ConfigParser
from os import listdir
from os.path import isfile, join

from database import Database
from mi_parser import MiParser
# When needed, import update parser.
from mi_parser_update_reconfig import MiParserUpdateReconfig
from mi_parser_update_meas_report import MiParserUpdateMeasReport
from mi_parser_update_intra_freq_meas import MiParserUpdateIntraFreqMeas
from mylogger_parser import MyloggerParser
from tcpdump_parser import TcpdumpParser
from utils import *

Config = ConfigParser.ConfigParser()
Config.read("config.ini")
DB_USER = Config.get('Database', 'user')
DB_PASSWORD = Config.get('Database', 'password')
DB_HOST = Config.get('Database', 'host')
DB_UNIX_SOCKET = Config.get('Database', 'unix_socket')
UNZIP_FOLDER = Config.get('Database', 'unzip_folder')

#
# mode = 0: parse all the files
# mode = 1: parse only new files
# mode = 2: parse all the mylogger files 
# mode = 3: parse all the tcpdump files 
# mode = 4: parse all the milogger files 
# mode = 5: parse all the milogger files for reconfig
# mode = 6: parse all the milogger files for meas report
# mode = 7: parse all the milogger files for intra freq meas in connected mode
#
def main(mode):
    
    db = Database(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, unix_socket=DB_UNIX_SOCKET)
    #db.init_database()
    # Change upload_decoded_foloder to your path.
    upload_decoded_folder = "/var/www/modem/upload_decoded/"
    files = [f for f in listdir(upload_decoded_folder) if isfile(join(upload_decoded_folder, f)) and f[-4:] == '.zip'] 
    files.sort(key=lambda x: os.path.getmtime(join(upload_decoded_folder,x)))
    all_log_file_user_array = []
    if mode == 1:
        file_rows = db.select_multiple_row('''select file_name, user_id from file_timestamp where user_id >= 79''')
        for log_file, user_id in file_rows:
            log_file_user = log_file + "," + str(user_id)
            all_log_file_user_array.append(log_file_user)

    started = False
    for upload_decoded_file in files:   
        date = os.path.getmtime(join(upload_decoded_folder, upload_decoded_file))
        #if date < 1517396106 + 12 * 3600:
        #    continue
        log_file_user_array = parse_one_zip_file(mode, db, upload_decoded_folder, upload_decoded_file, all_log_file_user_array)
        all_log_file_user_array = all_log_file_user_array + log_file_user_array
        db.commit()
    db.close()

def parse_one_zip_file(mode, db, upload_decoded_folder, upload_decoded_file, all_log_file_user_array):
    print "Parse zip:" + upload_decoded_file
    match_obj = re.match(r'^.*modem_(.+)_(\d+)_(\d+)\.zip$', upload_decoded_file, re.M|re.I)
    if match_obj is None:
        print "Invalid file name: " + upload_decoded_file
        return []
    hash_id = match_obj.group(1)
    date_str = match_obj.group(2) + "_" + match_obj.group(3)
    if '_' in hash_id:
        match_obj = re.match(r'^.*modem_(.+)_(\d+)_(\d+)_(\d+)\.zip$', upload_decoded_file, re.M|re.I)
        hash_id = match_obj.group(1)
        date_str = match_obj.group(2) + "_" + match_obj.group(3) + "_" + match_obj.group(4)
    
    # Get user ID using hashed ID    
    user_id = db.get_id('user', {'hash_id':hash_id})
    if user_id == 0:
        print 'Error in db.get_id for ' + upload_decoded_file + ' , no user_id found!'
        return []

    # Unzip the uploaded zip files, then we get MI logs, mylogger logs and tcpdump logs
    try:
        zip_ref = zipfile.ZipFile(join(upload_decoded_folder, upload_decoded_file), 'r')
    except:
        print 'Because of bad zip file, skip this file'
        return []
    zip_ref = zipfile.ZipFile(join(upload_decoded_folder, upload_decoded_file), 'r')
    log_files = zip_ref.namelist()
    zip_ref.extractall(UNZIP_FOLDER)
    zip_ref.close()
    log_file_user_array = []
    for log_file in log_files:
        if log_file[:4] == 'log_' and mode in [0, 1, 2]:
            log_file_user = log_file + "," + str(user_id)
            if log_file_user in all_log_file_user_array:
                continue
            #"""
            print "Parse my:" + log_file 
            # Use MyloggerParser to parse mylogger logs
            myloggerParser = MyloggerParser(db, join(UNZIP_FOLDER, log_file), join(upload_decoded_folder, upload_decoded_file), log_file, user_id)
            myloggerParser.parse()
            log_file_user_array.append(log_file_user)
            sys.stdout.flush()
            #"""
            pass
        elif log_file[:8] == 'tcpdump_' and mode in [0, 1, 3]:
            log_file_user = log_file + "," + str(user_id)
            if log_file_user in all_log_file_user_array:
                continue
            #"""
            print "Parse tcpdump:" + log_file 
            # Use TcpdumpParser to parse tcpdump logs
            tcpdumpParser = TcpdumpParser(db, join(UNZIP_FOLDER, log_file), join(upload_decoded_folder, upload_decoded_file), log_file, user_id)
            tcpdumpParser.parse()
            log_file_user_array.append(log_file_user)
            sys.stdout.flush()
            #"""
            pass
        elif log_file[:8] == 'diag_log' and log_file[-3:] == 'xml'\
            and mode in [0, 1, 4, 5, 6, 7]:
            log_file_user = log_file + "," + str(user_id)
            if log_file_user in all_log_file_user_array:
                continue
            #"""
            print "Parse mi:" + log_file 
            # Use MiParser to parse mylogger logs
            if mode in [0, 1, 4]:
                miParser = MiParser(db, join(UNZIP_FOLDER, log_file), join(upload_decoded_folder, upload_decoded_file), log_file, user_id)
                miParser.parse()    
                miParser.parse_to_message()
            # When you want to update modem messages, two steps are involved.
            #     1) Import update parser file in the front.
            #     2) Uncomment two lines below for parse. 
            if mode in [5]:
                miParserUpdate = MiParserUpdateReconfig(db, join(UNZIP_FOLDER, log_file), join(upload_decoded_folder, upload_decoded_file), log_file, user_id)
                miParserUpdate.parse()    
            if mode in [6]:
                miParserUpdate = MiParserUpdateMeasReport(db, join(UNZIP_FOLDER, log_file), join(upload_decoded_folder, upload_decoded_file), log_file, user_id)
                miParserUpdate.parse()    
            if mode in [7]:
                miParserUpdate = MiParserUpdateIntraFreqMeas(db, join(UNZIP_FOLDER, log_file), join(upload_decoded_folder, upload_decoded_file), log_file, user_id)
                miParserUpdate.parse()    
            log_file_user_array.append(log_file_user)
            sys.stdout.flush()
            #"""
            #return log_file_user_array
            pass    
        os.remove(join(UNZIP_FOLDER, log_file))

    return log_file_user_array

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Please provide the mode of parse upload decoded files."
        exit(-1)
    main(int(sys.argv[1]))
    


