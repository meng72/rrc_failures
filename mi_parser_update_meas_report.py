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

class MiParserUpdateMeasReport:
    
    def __init__(self, db, file_path, original_zip_file, original_log_name, user_id):
        self.db = db
        self.file_path = file_path
        self.original_zip_file = original_zip_file
        self.original_log_name = original_log_name
        self.user_id = user_id

    def parse_extra(self, extra_text):
        result = {}
        tokens = extra_text.split(',')
        for token in tokens:
            subtokens = token.split('=')
            if len(subtokens) == 2:
                key = subtokens[0]
                value = subtokens[1]
                result[key] = value
        return result

    def serialize_extra(self, extra_dict):
        result = ""
        for key in extra_dict:
            result = result + key + '=' + str(extra_dict[key]) + ','
        if len(result) > 0:
            result = result[:-1]
        return result

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
                    if "LTE_RRC_OTA_Packet" not in dm_log_packet_str:
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

                    if packet_type == "LTE_RRC_OTA_Packet":
                        if 'c1: measurementReport (1)' in dm_log_packet_str:
                            extra = {}
                            for pair in pairs:
                                if pair.get('key') == 'Msg':
                                    reconfig_list = pair.findall('.//')
                                    for reconfig in reconfig_list:
                                        if reconfig.get('showname') == 'c1: measurementReport (1)':
                                            #config_list = reconfig.findall('./field/field/field/field/field/field')
                                            config_list = reconfig.findall('.//')
                                            for config in config_list:
                                                #extra = self.get_config_for_meas_report(config, extra)
                                                if config.get('name') == 'lte-rrc.measId':
                                                    measId = int(self.get_showname_context(config.get('showname')))
                                                    extra['measId'] = measId
                                                    break
                                                    

                            #print extra
                            db.execute(''' update messages set extra = "%s" 
                                where user_id = %d and time = %d and message_type_id = 14 
                                ''' % (serialize_extra(extra), user_id, time))

        # if start_time is not None:
        #     db.insert_to_event_table('file_timestamp',\
        #         {'file_path': self.original_zip_file, 'file_name': self.original_log_name, 'user_id': user_id, 'start_time': start_time, 'end_time': end_time})

    def get_showname_context(self, showname):
        return showname.split(':')[1].strip(' ').split(' ')[0]

    def get_config_for_meas_report(self, config, extra):
        # copy from reconfig without any modification
        config_name = config.get('name')
        if config_name == 'lte-rrc.measObjectToAddModList':
            item_num = int(self.get_showname_context(config.get('showname')))
            extra['measObjList'] = {}
            extra['measObjList']['measObj_num'] = item_num
            meas_objs = config.findall('.//')
            for meas_obj in meas_objs:
                if meas_obj.get('name') == 'lte-rrc.MeasObjectToAddMod_element':
                    meas_info_list = meas_obj.findall('.//')
                    cells_list = []
                    for meas_info in meas_info_list:
                        if meas_info.get('name') == 'lte-rrc.measObjectId':
                            meas_id = int(self.get_showname_context(meas_info.get('showname')))
                        elif meas_info.get('name') == 'lte-rrc.carrierFreq':
                            carrier_freq = int(self.get_showname_context(meas_info.get('showname')))
                        elif meas_info.get('name') == 'lte-rrc.cellsToAddModList':
                            cells = meas_info.findall('.//')
                            for cell in cells:
                                #if cell.get('name') == 'lte-rrc.CellsToAddMod_element'\
                                #    or cell.get('name') == 'lte-rrc.CellsToAddModUTRA_FDD_element':
                                if 'element' in cell.get('name'):
                                    cell_info_list = cell.findall('.//')
                                    for cell_info in cell_info_list:
                                        if cell_info.get('name') == 'lte-rrc.cellIndex':
                                            cellIndex = int(self.get_showname_context(cell_info.get('showname')))
                                        elif cell_info.get('name') == 'lte-rrc.physCellId':
                                            pcid = int(self.get_showname_context(cell_info.get('showname')))
                                        elif cell_info.get('name') == 'lte-rrc.cellIndividualOffset':
                                            cellOffset = self.get_showname_context(cell_info.get('showname'))
                                    if 'FDD' in cell.get('name'):
                                        cells_list.append({'mod': cell.get('name'), 'cellIndex': cellIndex, 'pcid': pcid})
                                    else:
                                        cells_list.append({'mod': cell.get('name'), 'cellIndex': cellIndex, 'pcid': pcid, 'cellOffset': cellOffset})
                    extra['measObjList']['measObj[%d]' % (meas_id)] = {}
                    extra['measObjList']['measObj[%d]' % (meas_id)]['carrierFreq'] = carrier_freq
                    extra['measObjList']['measObj[%d]' % (meas_id)]['cells'] = cells_list
        elif config_name == 'lte-rrc.reportConfigToAddModList':
            item_num = int(self.get_showname_context(config.get('showname')))
            extra['reportConfigList'] = {}
            extra['reportConfigList']['reportConfig_num'] = item_num
            report_objs = config.findall('.//')
            for report_obj in report_objs:
                if report_obj.get('name') == 'lte-rrc.ReportConfigToAddMod_element':
                    report_info_list = report_obj.findall('.//')
                    for report_info in report_info_list:
                        if report_info.get('name') == 'lte-rrc.reportConfigId':
                            report_id = int(self.get_showname_context(report_info.get('showname')))
                        elif report_info.get('name') == 'lte-rrc.triggerType':
                            trig_type = self.get_showname_context(report_info.get('showname'))
                        elif report_info.get('name') == 'lte-rrc.eventId':
                            info = self.get_showname_context(report_info.get('showname'))
                        elif report_info.get('name') == 'lte-rrc.purpose':
                            info = self.get_showname_context(report_info.get('showname'))
                    #print [report_id, trig_type, event_id]
                    extra['reportConfigList']['reportConfig[%d]' % (report_id)] = {}
                    extra['reportConfigList']['reportConfig[%d]' % (report_id)]['trig_type'] = trig_type
                    extra['reportConfigList']['reportConfig[%d]' % (report_id)]['info'] = info
        elif config_name == 'lte-rrc.measIdToAddModList':
            item_num = int(self.get_showname_context(config.get('showname')))
            extra['measIdList'] = {}
            extra['measIdList']['measId_num'] = item_num
            measId_objs = config.findall('.//')
            for measId_obj in measId_objs:
                if measId_obj.get('name') == 'lte-rrc.MeasIdToAddMod_element':
                    measId_info_list = measId_obj.findall('.//')
                    for measId_info in measId_info_list:
                        if measId_info.get('name') == 'lte-rrc.measId':
                            meas_id = int(self.get_showname_context(measId_info.get('showname')))
                        elif measId_info.get('name') == 'lte-rrc.measObjectId':
                            measObj_id = int(self.get_showname_context(measId_info.get('showname')))
                        elif measId_info.get('name') == 'lte-rrc.reportConfigId':
                            reportConfig_id = int(self.get_showname_context(measId_info.get('showname')))
                    #print [meas_id, measObj_id, reportConfig_id]
                    extra['measIdList']['measId[%d]' % (meas_id)] = {}
                    extra['measIdList']['measId[%d]' % (meas_id)]['measObj_id'] = measObj_id
                    extra['measIdList']['measId[%d]' % (meas_id)]['reportConfig_id'] = reportConfig_id
        return extra
            
            
        
