#!/usr/bin/env python 
"""
A parser to parse decoded MI log file (xml file),
and insert parsed information into database. 

"""

import xml.etree.ElementTree as ET
import sys
import re
import os
import pytz
from utils import *
import database
from shutil import copyfile

class MiParserUpdateIntraFreqMeas:
    
    def __init__(self, db, file_path, original_zip_file, original_log_name, user_id):
        self.db = db
        self.file_path = file_path
        self.original_zip_file = original_zip_file
        self.original_log_name = original_log_name
        self.user_id = user_id

    def parse(self):
        db = self.db
        user_id = self.user_id
        file_path = self.file_path
        if os.stat(file_path).st_size == 0:
            return
        match_obj = re.match(r'^.*diag_log_(\d*)_(\d*)_.*', file_path, re.M|re.I)
        filename_time = None
        if match_obj is not None:
            system_date_str = match_obj.group(1) + "_" + match_obj.group(2)
            filename_time = unix_time_second(datetime.strptime(system_date_str, "%Y%m%d_%H%M%S")) * 1000000

        start_time = None
        end_time = None
        start_line = None 
        #end_line = None
        trust = None
        #
        # 1st pass to decide the time range of this file
        with open(file_path, 'r') as f:
            pre_time = None
            packet_no = 0
            for line in f:
                if "<dm_log_packet>" in line:
                    packet_no += 1
                    match_obj = re.match(r'^.*key="timestamp">(.*?)</pair>.*', line, re.M|re.I)
                    if match_obj is None:
                        print "Error: Cannot find timestamp:" + self.original_zip_file + "/" + self.original_log_name 
                        copyfile(file_path, file_path + "_error")  
                        return
                    time_str = match_obj.group(1)
                    time_array = time_str.split('.')
                    if len(time_array) < 2:
                        us = 0
                    else:
                        us = int(time_array[1])
                    time = unix_time_micros(datetime.strptime(time_array[0], "%Y-%m-%d %H:%M:%S")) + us

                    if pre_time is None:
                        start_time = time
                        start_line = packet_no
                    else:
                        if time - pre_time < -1000000 * 60 or abs(time - pre_time) > 1000000 * 3600 * 1.5:
                            # The time in the file is not continous. Ignore the previous content.
                            start_time = time
                            start_line = packet_no
                    pre_time = time        
            
            end_time = time 
            
            if start_time is None:
                print "Empty file"
                return
            
            if abs(start_time - (filename_time + 4 * 3600 * 1000 * 1000)) > 1000000 * 3600 * 24:
                print "Error: Filename time and start time is not in one day:" + self.original_zip_file + "/" + self.original_log_name  
                copyfile(file_path, file_path + "_error")  
                return

            # rows_start = db.select_multiple_row('''select time from event_milog
            #     where user_id = %d and `on` = 1 and time >= %d and time <= %d '''
            #     % (user_id, start_time - 1000000 * 120, start_time + 1000000 * 120))
            #
            # rows_end = db.select_multiple_row('''select time from event_milog
            #     where user_id = %d and `on` = 0 and time >= %d and time <= %d '''
            #     % (user_id, end_time - 1000000 * 120, end_time + 1000000 * 120))
            #
            # if len(rows_start) == 0 and len(rows_end) == 0:
            #     trust = 2
            #     #print "Error: Cannot find start_mi and end_mi event:%d,%s/%s" % (start_time, self.original_zip_file, self.original_log_name)
            #     #copyfile(file_path, file_path + "_error")
            
        """ This is only for refine date ...""" 
        """
        if start_time is not None:
            db.insert_to_event_table('file_timestamp',\
                {'file_path': self.original_zip_file, 'file_name': self.original_log_name, 'user_id': user_id, 'start_time': start_time, 'end_time': end_time})
        return 
        """
        
        # 2nd pass to parse the file and insert to database 
        dm_log_packet_str = ""
        with open(file_path, 'r') as f:
            packet_no = 0
            for line in f:
                if "<dm_log_packet>" in line:
                    packet_no += 1
                    dm_log_packet_str = line
                elif "<dm_log_packet>" not in line:
                    dm_log_packet_str = dm_log_packet_str + line
                if "</dm_log_packet>" in line:
                    if "LTE_PHY_Connected_Mode_Intra_Freq_Meas" not in dm_log_packet_str:
                        continue
                    dm_log_packet = ET.fromstring(dm_log_packet_str)
                    pairs = dm_log_packet.findall('pair')
                    packet_type = pairs[1].text
                    time_array = pairs[2].text.split('.')
                    if len(time_array) < 2:
                        us = 0
                    else:
                        us = int(time_array[1])
                    time = unix_time_micros(datetime.strptime(time_array[0], "%Y-%m-%d %H:%M:%S")) + us
                    if trust is None:
                        if packet_no < start_line:
                            trust = 0 
                        else:
                            trust = 1

                    if packet_type == "LTE_PHY_Connected_Mode_Intra_Freq_Meas":
                        extra = {}
                        for pair in pairs:
                            if pair.get('key') == 'Serving Physical Cell ID':
                                extra['spcid'] = int(pair.text)
                            elif pair.get('key') == 'E-ARFCN':
                                extra['earfcn'] = int(pair.text)
                            elif pair.get('key') == 'RSRP(dBm)':
                                extra['rsrp'] = float(pair.text)
                            elif pair.get('key') == 'RSRQ(dB)':
                                extra['rsrq'] = float(pair.text)
                            elif pair.get('key') == 'Neighbor Cells':
                                extra['nb_cells'] = {}
                                meas_dicts = pair.findall('./list/item/dict')
                                for meas_dict in meas_dicts:
                                    extra_nb_cell = {}
                                    meas_pairs = meas_dict.findall('pair')
                                    for meas_pair in meas_pairs:
                                        meas_key = meas_pair.get('key')
                                        meas_val = meas_pair.text
                                        if meas_key == 'Physical Cell ID':
                                            extra_nb_cell['pcid'] = int(meas_val)
                                        elif meas_key == 'RSRP(dBm)':
                                            extra_nb_cell['RSRP'] = float(meas_val)
                                        elif meas_key == 'RSRQ(dB)':
                                            extra_nb_cell['RSRQ'] = float(meas_val)
                                        else:
                                            meas_key = meas_key.replace(' ', '_')
                                            meas_val = meas_val.replace(' ', '_')
                                            extra_nb_cell[meas_key] = meas_val
                                    if 'pcid' not in extra_nb_cell: 
                                        print 'Missing pcid: ' + str(meas_pairs)
                                    else:
                                        extra['nb_cells'][extra_nb_cell['pcid']] = {}
                                        for k, v in extra_nb_cell.iteritems():
                                            if k == 'pcid':
                                                continue
                                            extra['nb_cells'][extra_nb_cell['pcid']][k] = v
                                            
                        #print extra
                        db.execute(''' update messages set extra = "%s" 
                            where user_id = %d and time = %d and message_type_id = 9 
                            ''' % (serialize_extra(extra), user_id, time))

    def get_showname_context(self, showname):
        return showname.split(':')[1].strip(' ').split(' ')[0]

            
        
