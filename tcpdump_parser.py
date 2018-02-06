#!/usr/local/bin/python
"""
A parser to parse Mylogger log file,
and insert parsed information into database. 

Author: Xiaomeng Chen
"""

import json
from pprint import pprint
import re
import os
import sys
from utils import *
import database
from shutil import copyfile

class TcpdumpParser:
    
    def __init__(self, db, file_path, original_zip_file, original_log_name, user_id):
        self.db = db
        self.file_path = file_path
        self.original_zip_file = original_zip_file
        self.original_log_name = original_log_name
        self.user_id = user_id

    def parse(self):
        db = self.db
        file_path = self.file_path
        user_id = self.user_id

        match_obj = re.match(r'^.*tcpdump_(\d*)_(\d*)', file_path, re.M|re.I)
        if match_obj is not None:
            system_date_str = match_obj.group(1)# + "_000000" 
        else:
            print 'ERROR: filename doesn\'t match the regex expression'
            return
    
        start_time = None
        end_time = None

        os.system("tcpdump -tt -n 'tcp[tcpflags] != 0' -r %s > tcpdump_tmp" %(file_path))
        with open('tcpdump_tmp') as data_file:
            line = data_file.readline()
            if len(line) == 0:
                file_path_new = file_path
            else:
                file_path_new = 'tcpdump_tmp'
            
        with open(file_path_new) as data_file:
            line = data_file.readline()
            while line:
                #print '\n\n' + line
                line = line.strip('\n')
                time, proto, ip_src, ip_src_port, ip_dst, ip_dst_port\
                    , flags , seq, ack, win, length, options = \
                    [None for i in range(0, 12)]
                segment_index = 0
                for segment in line.split(', '):
                    if segment_index == 0:
                        in_segment = segment.split(': ')
                        segment_0 = in_segment[0]
                        entry_index = 0
                        for entry in segment_0.split(' '):
                            #print str(entry_index) + ' ' + entry
                            # 0. time
                            if entry_index == 0:
                                time_array = entry.split('.')
                                if len(time_array) < 2:
                                    us = 0
                                else:
                                    us = int(time_array[1])
                                #print system_date_str + '_' + time_array[0]
                                if re.match(r'(\d\d):(\d\d):(\d\d)', time_array[0], re.M|re.I):
                                    print 'ERROR in tcpdump_parser for user_id %d: time_array[0] not match new in %s %s!'\
                                        % (user_id, file_path, self.original_zip_file)
                                    return
                                if not re.match(r'[0-9]{10}', time_array[0], re.M|re.I):
                                    print 'ERROR in tcpdump_parser for user_id %d: time_array[0] not match both in %s %s!'\
                                        % (user_id, file_path, self.original_zip_file)
                                    return
                                time = float(time_array[0]) * 1000000.0 + us
                                if start_time is None:
                                    start_time = time
                                else:
                                    end_time = time
                            # 1. proto: IP, ARP, IP6
                            if entry_index == 1:
                                proto = entry
                            # 2. ip_src, ip_src_port
                            # 4. ip_dst, ip_dst_port
                            #    Available formats include: data
                            #    But sometimes ip6 has different format.
                            #    Ignore extreme cases here
                            if entry_index in (2, 4):
                                ip_port = entry.split('.')
                                if len(ip_port) <= 1:
                                    continue
                                ip = '.'.join(ip_port[0: len(ip_port) - 1])
                                if entry_index == 2:
                                    ip_src = ip
                                    ip_src_port = ip_port[len(ip_port) - 1]
                                if entry_index == 4:
                                    ip_dst = ip
                                    ip_dst_port = ip_port[len(ip_port) - 1]
                            entry_index += 1 # end of segment_index = 0
                        flags = ''
                        for segment_others in in_segment[1:]:
                            flags += segment_others + ' '
                        flags = flags[:-1]
                    else:
                        entry_list = segment.split(' ')
                        if entry_list[0] == 'seq':
                            seq = entry_list[1]
                        if entry_list[0] == 'ack': 
                            ack = entry_list[1]
                        if entry_list[0] == 'win': 
                            win = entry_list[1]
                        if entry_list[0] == 'length': 
                            length = entry_list[1]
                        if entry_list[0] == 'options':
                            options = ' '.join(entry_list[1:])
                            if options[0] == '[':
                                options = options[1:]
                            if options[len(options) - 1] == ']':
                                options = options[:-1]
                    segment_index += 1

                #print [time, proto, ip_src, ip_src_port, ip_dst, ip_dst_port\
                #    , flags , seq, ack, win, length, options]
                db.insert_to_event_table('event_tcpdump', {'user_id': user_id, 'time': time, 'proto': proto, 'ip_src': ip_src, 'ip_src_port': ip_src_port\
                    , 'ip_dst': ip_dst, 'ip_dst_port': ip_dst_port, 'flags': flags, 'seq': seq\
                    , 'ack': ack, 'win': win, 'length': length, 'options': options})
                #print ['event_tcpdump', {'user_id': user_id, 'proto': proto, 'ip_src': ip_src, 'ip_src_port': ip_src_port\
                #    , 'ip_dst': ip_dst, 'ip_dst_port': ip_dst_port, 'flags': flags, 'seq': seq\
                #    , 'ack': ack, 'win': win, 'length': length, 'options': options}]
                # read next line
                line = data_file.readline()
                #db.commit()

        #print [start_time, end_time]
        if start_time is not None:
            db.insert_to_event_table('file_timestamp',\
             {'file_path': self.original_zip_file, 'file_name': self.original_log_name, 'user_id': user_id, 'start_time': start_time, 'end_time': end_time})
            #print 'insert_to_file_timestamp'
