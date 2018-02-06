#!/usr/local/bin/python
"""
A parser to parse Mylogger log file,
and insert parsed information into database. 

"""

import json
from pprint import pprint
import re
import sys
import database
from shutil import copyfile
class MyloggerParser:
    
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

        start_time = None
        end_time = None

        with open(file_path) as data_file:
            try:
                entries = json.load(data_file)
            except ValueError as err:
                #print('JSON syntax error ' + str(err) + ":" + file_path + "," + str(user_id))
                data_file.seek(0)
                file_str = data_file.read()
                last_id_index = file_str.rfind('{"id":')
                if last_id_index <= 0:
                    print('Cannot find the last id:' + file_path + "," + str(user_id))
                    copyfile(file_path, file_path + "_error") 
                    return
                
                if last_id_index == 1:
                    file_str = "[]"
                else:
                    file_str = file_str[:last_id_index - 1] + ']' 
                try:
                    entries = json.loads(file_str)
                except ValueError as err:
                    print('2nd JSON syntax error ' + str(err) + ":" + file_path + "," + str(user_id))
                    copyfile(file_path, file_path + "_error") 
                    return
            i = 0
            uid_pkg_map = {}
            pkg_appid_map = {}
            pre_pkg_cputime_map = {}
            pre_pkg_net_map = {}
            pre_cpu_freq = {}
            pre_cpu_freq_time = -1
            for entry in entries:
                entry["user_id"] = user_id
                entry["time"] = entry["time"] * 1000
                if start_time is None:
                    start_time = entry["time"]
                end_time = entry["time"]
                if entry["id"] in ['location']:
                    db.insert_to_event_table('event_' + entry["id"], entry, ['id'])
                elif entry["id"] == 'system':
                    db.insert_to_event_table('event_' + entry["id"], entry, ['id', 'hash_id'])
                elif entry["id"] == 'cpu':
                    cpu_util_result = self.parse_cpu_util(entry["util"], int(entry["time"]))
                    for uid, pkg_name in cpu_util_result['uid_pkg_map'].iteritems():
                        uid_pkg_map[uid] = pkg_name
                    pkg_cputime_map = cpu_util_result['pkg_cputime_map']
                    for pkg_name, cputime in pkg_cputime_map.iteritems():
                        if pkg_name in pre_pkg_cputime_map:
                            pre_cputime = pre_pkg_cputime_map[pkg_name]
                            if pkg_name in pkg_appid_map:
                                app_id = pkg_appid_map[pkg_name]
                            else:
                                app_id = db.get_id('app', {'pkg_name': pkg_name})
                                pkg_appid_map[pkg_name] = app_id
                            if cputime[0] - pre_cputime[0] > 0 or cputime[1] - pre_cputime[1] > 0:
                                db.insert_to_event_table('event_cpu', 
                                    {'user_id': user_id, 'start_time':pre_cputime[2], 'end_time':cputime[2], 
                                    'app_id':app_id, 'user_time': cputime[0] - pre_cputime[0], 'system_time': cputime[1] - pre_cputime[1]}
                                )
                        pre_pkg_cputime_map[pkg_name] = cputime
                elif entry["id"] == 'user_activity':
                    match_obj = re.match(r'^DetectedActivity \[type=(.*), confidence=(\d*)\]', entry["value"], re.M|re.I)
                    if match_obj is not None:
                        entry["type_id"] = db.get_id('type_event_user_activity', {'value': match_obj.group(1)})
                        entry["confidence"] = int(match_obj.group(2))
                        db.insert_to_event_table('event_user_activity', entry, ['id', 'value'])
                elif entry["id"] in ['start_tcpdump', 'stop_tcpdump']:
                    if entry["id"] == 'start_tcpdump':
                        entry["on"] = 1
                    else:
                        entry["on"] = 0
                    db.insert_to_event_table('event_tcpdumplog', entry, ['id'])
                elif entry["id"] in ['start_mi', 'stop_mi']:
                    if entry["id"] == 'start_mi':
                        entry["on"] = 1
                    else:
                        entry["on"] = 0
                    db.insert_to_event_table('event_milog', entry, ['id'])
                    pass
                elif entry["id"] in ['app']:
                    db.insert_to_event_table('event_app', entry, ['id'])
                elif entry["id"] in ['lte_cell_identity']:
                    db.insert_to_event_table('event_lte_cell_identity', entry, ['id'])
                elif entry["id"] in ['error']:
                    db.insert_to_event_table('event_error', entry, ['id'])
                else:
                    print "Unknown id: " + entry["id"]

        if start_time is not None:
            db.insert_to_event_table('file_timestamp',\
             {'file_path': self.original_zip_file, 'file_name': self.original_log_name, 'user_id': user_id, 'start_time': start_time, 'end_time': end_time})
