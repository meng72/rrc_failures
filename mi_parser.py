#!/usr/bin/env python 
"""
A parser to parse decoded MI log file (xml file),
and insert parsed information into database. 

Author: Xiaomeng Chen
"""

import xml.etree.ElementTree as ET
import sys
import re
import os
import pytz
from utils import *
import database
from shutil import copyfile

class MiParser:
    
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

            rows_start = db.select_multiple_row('''select time from event_milog 
                where user_id = %d and `on` = 1 and time >= %d and time <= %d ''' 
                % (user_id, start_time - 1000000 * 120, start_time + 1000000 * 120))
            
            rows_end = db.select_multiple_row('''select time from event_milog 
                where user_id = %d and `on` = 0 and time >= %d and time <= %d ''' 
                % (user_id, end_time - 1000000 * 120, end_time + 1000000 * 120))
            
            if len(rows_start) == 0 and len(rows_end) == 0:
                trust = 2
                #print "Error: Cannot find start_mi and end_mi event:%d,%s/%s" % (start_time, self.original_zip_file, self.original_log_name) 
                #copyfile(file_path, file_path + "_error") 
            
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
                    if packet_type in ['LTE_LL1_PCFICH_Decoding_Results', 
                                    'LTE_PHY_Connected_Mode_Neighbor_Measurement', 
                                    'LTE_PHY_Serv_Cell_Measurement',
                                    'LTE_PHY_Inter_RAT_Measurement',
                                    'LTE_RRC_MIB_Message_Log_Packet',
                                    'LTE_NAS_ESM_OTA_Incoming_Packet',
                                    'LTE_NAS_ESM_OTA_Outgoing_Packet',
                                    'UMTS_NAS_OTA_Packet'
                                    ]:
                        db.insert_to_event_table('event_' + packet_type, {'user_id': user_id, 'time': time, 'trust':trust})
                    elif packet_type in ['LTE_NAS_EMM_OTA_Incoming_Packet',
                                        'LTE_NAS_EMM_OTA_Outgoing_Packet']:
                        insert_values = {}
                        insert_values['user_id'] = user_id
                        insert_values['time'] = time
                        insert_values['trust'] = trust 
                        insert_values['extra'] = ''
                        for pair in pairs:
                            key = pair.get("key")
                            if key != "Msg":
                                continue
                            packet = pair.find('msg').find('packet')
                            protos = packet.findall('proto')
                            for proto in protos:
                                name = proto.get("name")
                                if name != "nas-eps":
                                    continue
                                fields = proto.findall('field')
                                for field in fields:
                                    field_name = field.get('name')
                                    if field_name == 'nas_eps.nas_msg_emm_type':
                                        showname = field.get('showname')
                                        nas_msg_emm_type_id = db.get_id('nas_msg_emm_types', {'value' : showname})
                                        insert_values['nas_msg_emm_type_id'] = nas_msg_emm_type_id
                                    elif field_name in ['nas_eps.emm.active_flg',
                                                    'nas_eps.emm.cause',
                                                    'nas_eps.emm.csfb_resp',
                                                    'nas_eps.emm.detach_type_dl',
                                                    'nas_eps.emm.detach_type_ul',
                                                    'nas_eps.emm.eps_att_type',
                                                    'nas_eps.emm.EPS_attach_result',
                                                    'nas_eps.emm.eps_update_result_value',
                                                    'nas_eps.emm.type_of_id',
                                                    'nas_eps.emm.update_type_value'
                                                    ]:
                                        showname = field.get('showname')
                                        if len(insert_values['extra']) != 0:
                                            insert_values['extra'] += ','
                                        insert_values['extra'] += field_name + ' = \"' + showname + '\"'

                        db.insert_to_event_table('event_' + packet_type, insert_values)
                        #db.replace_to_event_table('event_' + packet_type, insert_values)
                    elif packet_type == 'LTE_PHY_Connected_Mode_Intra_Freq_Meas':
                        insert_values = {}
                        insert_values['user_id'] = user_id
                        insert_values['time'] = time
                        insert_values['trust'] = trust 
                        for pair in pairs:
                            key = pair.get("key")
                            if key == "E-ARFCN":
                                insert_values['E_ARFCN'] = int(pair.text)
                            elif key == "Serving Physical Cell ID":
                                insert_values['spcid'] = int(pair.text)
                            elif key == "RSRP(dBm)":
                                insert_values['RSRP'] = float(pair.text)
                            elif key == "RSRQ(dB)":
                                insert_values['RSRQ'] = float(pair.text)
                            elif key == "Number of Neighbor Cells":
                                insert_values['n_neic'] = int(pair.text)
                            elif key == "Number of Detected Cells":
                                insert_values['n_detc'] = int(pair.text)
                            elif key == "Neighbor Cells":
                                if insert_values['n_neic'] > 0:
                                    adict = pair.find('list').find('item').find('dict')
                                    for adict_pair in adict.findall('pair'):
                                        if adict_pair.get("key") == "Physical Cell ID":
                                            insert_values['neipcid'] = int(adict_pair.text)
                                        elif adict_pair.get("key") == "RSRP(dBm)":
                                            insert_values['neiRSRP'] = float(adict_pair.text)
                                        elif adict_pair.get("key") == "RSRQ(dB)":
                                            insert_values['neiRSRQ'] = float(adict_pair.text)
                        db.insert_to_event_table('event_' + packet_type, insert_values)
                    elif packet_type == 'LTE_PHY_PDSCH_Packet':
                        insert_values = {}
                        insert_values['user_id'] = user_id
                        insert_values['time'] = time
                        insert_values['trust'] = trust 
                        for pair in pairs:
                            key = pair.get("key")
                            if key == "Serving Cell ID":
                                insert_values["scid"] = int(pair.text)
                            elif key == "Number of Tx Antennas(M)":
                                insert_values["n_txant"] = int(pair.text)
                            elif key == "Number of Rx Antennas(N)":
                                insert_values["n_rxant"] = int(pair.text)
                            elif key == "MCS 0":
                                insert_values["MCS0"] = pair.text
                            elif key == "MCS 1":
                                insert_values["MCS1"] = pair.text
                        db.insert_to_event_table('event_' + packet_type, insert_values)
                    elif packet_type in ['LTE_MAC_DL_Transport_Block', 'LTE_MAC_UL_Transport_Block']:
                        insert_values = {}
                        insert_values['user_id'] = user_id
                        insert_values['time'] = time
                        insert_values['trust'] = trust 
                        if packet_type == 'LTE_MAC_DL_Transport_Block':
                            insert_values['direction'] = "rcv"
                        else:
                            insert_values['direction'] = "snd"
                        n_byte = 0
                        for pair in pairs:
                            key = pair.get("key")
                            if key == "Subpackets":
                                adict = pair.find('list').find('item').find('dict')
                                for adict_pair in adict.findall('pair'):
                                    adict_key = adict_pair.get("key")
                                    if adict_key == "Num Samples":
                                        insert_values["n_sample"] = int(adict_pair.text)
                                    elif adict_key == "Sample":
                                        for sub_pair in adict_pair.find('dict').findall('pair'):
                                            if sub_pair.get("key") in ["Grant (bytes)", "DL TBS (bytes)"]:
                                                n_byte += int(sub_pair.text)
                                break
                        insert_values['n_byte'] = n_byte
                        db.insert_to_event_table('event_LTE_MAC_Transport_Block', insert_values)
                    
                    elif packet_type in ['LTE_RLC_DL_AM_All_PDU', 'LTE_RLC_UL_AM_All_PDU']:
                        insert_values = {}
                        insert_values['user_id'] = user_id
                        insert_values['time'] = time
                        insert_values['trust'] = trust 
                        if packet_type == 'LTE_RLC_DL_AM_All_PDU':
                            insert_values['direction'] = "rcv"
                        else:
                            insert_values['direction'] = "snd"
                        pdu_bytes = 0
                        for pdu_bytes_start in [m.start() for m in re.finditer('pdu_bytes', dm_log_packet_str)]:
                            start_index = pdu_bytes_start + len('pdu_bytes">')
                            end_index = dm_log_packet_str.index('</pair>', start_index)
                            pdu_bytes += int(dm_log_packet_str[start_index:end_index])
                        insert_values['pdu_bytes'] = pdu_bytes 
                        db.insert_to_event_table('event_LTE_RLC_AM_All_PDU', insert_values)

                    elif packet_type == "LTE_RRC_Serv_Cell_Info":
                        insert_values = {}
                        insert_values['user_id'] = user_id
                        insert_values['time'] = time
                        insert_values['trust'] = trust 
                        for pair in pairs:
                            key = pair.get("key")
                            if key == "Cell ID":
                                insert_values['cid'] = int(pair.text)
                            elif key == "Downlink frequency":
                                insert_values['dl_freq'] = int(pair.text)
                            elif key == "Uplink frequency":
                                insert_values['ul_freq'] = int(pair.text)
                            elif key == "Downlink bandwidth":
                                if pair.text[-3:] == "MHz":
                                    insert_values['dl_bandwidth'] = int(pair.text[0:-4])
                                else:
                                    print "Wrong format in " + xml.etree.ElementTree.tostring(pair)
                            elif key == "Uplink bandwidth":
                                if pair.text[-3:] == "MHz":
                                    insert_values['ul_bandwidth'] = int(pair.text[0:-4])
                                else:
                                    print "Wrong format in " + xml.etree.ElementTree.tostring(pair)
                                    exit(-1)
                            elif key == "Cell Identity":
                                insert_values['cell_identity'] = int(pair.text)
                            elif key == "TAC":
                                insert_values['tac'] = int(pair.text)
                            elif key == "MCC":
                                insert_values['mcc'] = int(pair.text)
                            elif key == "MNC":
                                insert_values['mnc'] = int(pair.text)
                        db.insert_to_event_table('event_' + packet_type, insert_values)
                    elif packet_type == "LTE_RRC_OTA_Packet":
                        insert_values = {}
                        insert_values['user_id'] = user_id
                        insert_values['time'] = time
                        insert_values['trust'] = trust 
                        for pair in pairs:
                            key = pair.get("key")
                            if key == "Physical Cell ID":
                                insert_values['pcid'] = int(pair.text)
                            elif key == "Freq":
                                insert_values['freq'] = int(pair.text)
                            elif key == "SysFrameNum/SubFrameNum":
                                insert_values['sfnum'] = int(pair.text)
                        xml_str = dm_log_packet_str
                        rrc_state = ""
                        if 'c1: systemInformationBlockType1' in xml_str:
                            rrc_state = "SIB1"
                        elif 'c1: systemInformation (0)' in xml_str:
                            rrc_state = "SIBN"
                        elif 'c1: rrcConnectionRequest (1)' in xml_str:
                            rrc_state = 'rrcConnectionRequest'
                        elif 'c1: rrcConnectionSetup (3)' in xml_str:
                            rrc_state = 'rrcConnectionSetup'
                        elif 'c1: rrcConnectionSetupComplete (4)' in xml_str:
                            rrc_state = 'rrcConnectionSetupComplete'
                        elif 'c1: securityModeCommand (6)' in xml_str:
                            rrc_state = 'securityModeCommand'
                        elif 'c1: securityModeComplete (5)' in xml_str:
                            rrc_state = 'securityModeComplete'
                        elif 'c1: rrcConnectionReconfiguration (4)' in xml_str:
                            rrc_state = 'rrcConnectionReconfiguration'
                        elif 'c1: rrcConnectionReconfigurationComplete (2)' in xml_str:
                            rrc_state = 'rrcConnectionReconfigurationComplete'
                        elif 'c1: rrcConnectionRelease (5)' in xml_str:
                            rrc_state = 'rrcConnectionRelease'
                        elif 'c1: measurementReport (1)' in xml_str:
                            rrc_state = 'measurementReport'
                        elif 'c1: dlInformationTransfer (1)' in xml_str:
                            rrc_state = 'dlInformationTransfer'
                        elif 'c1: ulInformationTransfer (9)' in xml_str:
                            rrc_state = 'ulInformationTransfer'
                        elif 'c1: ueCapabilityEnquiry (7)' in xml_str:
                            rrc_state = 'ueCapabilityEnquiry'
                        elif 'c1: ueCapabilityInformation (7)' in xml_str:
                            rrc_state = 'ueCapabilityInformation'
                        elif 'c1: rrcConnectionReestablishmentRequest (0)' in xml_str:
                            rrc_state = 'rrcConnectionReestablishmentRequest'
                        elif 'c1: rrcConnectionReestablishment (0)' in xml_str:
                            rrc_state = 'rrcConnectionReestablishment'
                        elif 'c1: rrcConnectionReestablishmentComplete (3)' in xml_str:
                            rrc_state = 'rrcConnectionReestablishmentComplete'
                        elif 'c1: mobilityFromEUTRACommand (3)' in xml_str:
                            rrc_state = 'mobilityFromEUTRACommand'
                        elif 'c1: rrcConnectionReestablishmentReject (1)' in xml_str:
                            rrc_state = 'rrcConnectionReestablishmentReject'
                        elif 'c1: rrcConnectionReject (2)' in xml_str:
                            rrc_state = 'rrcConnectionReject'
                        elif 'PCCH-Message' in xml_str:
                            rrc_state = 'PCCH-Message'
                        else:
                            print "Unknown rrc state:" + xml_str
                            continue
                            # exit(-1)
                        rrc_state_id = db.get_id('lte_rrc_state', {'value': rrc_state})
                        insert_values['rrc_state_id'] = rrc_state_id
                        db.insert_to_event_table('event_' + packet_type, insert_values)
                        #db.replace_to_event_table('event_' + packet_type, insert_values)
                    elif packet_type == "LTE_RRC_MIB_Packet":
                        insert_values = {}
                        insert_values['user_id'] = user_id
                        insert_values['time'] = time
                        insert_values['trust'] = trust 
                        for pair in pairs:
                            key = pair.get("key")
                            if key == "Physical Cell ID":
                                insert_values['pcid'] = int(pair.text)
                            if key == "Freq":
                                insert_values['freq'] = int(pair.text)
                            if key == "Number of Antenna":
                                insert_values['n_ant'] = int(pair.text)
                            if key == "DL BW":
                                insert_values['dl_bandwidth'] = int(pair.text[:-4])
                        db.insert_to_event_table('event_' + packet_type, insert_values)
                    elif packet_type == "LTE_NAS_EMM_State":
                        insert_values = {}
                        insert_values['user_id'] = user_id
                        insert_values['time'] = time
                        insert_values['trust'] = trust 
                        for pair in pairs:
                            key = pair.get("key")
                            if key == "EMM State":
                                insert_values['emm_state_id'] = db.get_id('emm_state', {'value': pair.text})
                            elif key == "EMM Substate":
                                insert_values['emm_substate_id'] = db.get_id('emm_substate', {'value': pair.text})
                            elif key == "GUTI Valid":
                                insert_values['guti_valid'] = int(pair.text)
                            elif key == "GUTI UE Id":
                                insert_values['guti_ueid'] = int(pair.text)
                            elif key == "GUTI PLMN":
                                insert_values['guti_plmn'] = pair.text
                            elif key == "GUTI MME Group ID":
                                insert_values['guti_mme_groupid'] = pair.text
                            elif key == "GUTI MME Code":
                                insert_values['guti_mme_code'] = pair.text
                            elif key == "GUTI M-TMSI":
                                insert_values['guti_m_tmsi'] = pair.text
                        db.insert_to_event_table('event_' + packet_type, insert_values)
                    elif packet_type == "LTE_NAS_ESM_State":
                        insert_values = {}
                        insert_values['user_id'] = user_id
                        insert_values['time'] = time
                        insert_values['trust'] = trust 
                        for pair in pairs:
                            key = pair.get("key")
                            if key == "EPS bearer type":
                                insert_values['eps_bearer_type'] = int(pair.text)
                            if key == "EPS bearer state":
                                insert_values['eps_bearer_state'] = int(pair.text)
                        db.insert_to_event_table('event_' + packet_type, insert_values)
                    elif packet_type == "WCDMA_RRC_OTA_Packet":
                        xml_str = dm_log_packet_str
                        rrc_state = ""
                        if 'payload: completeSIB-List (7)' in xml_str:
                            rrc_state = "completeSIB-List"
                        elif 'payload: firstSegment (1)' in xml_str:
                            rrc_state = "firstSegment"
                        elif 'payload: subsequentSegment (2)' in xml_str:
                            rrc_state = 'subsequentSegment'
                        elif 'payload: lastSegmentShort (3)' in xml_str:
                            rrc_state = 'lastSegmentShort'
                        elif 'payload: noSegment (0)' in xml_str:
                            rrc_state = 'noSegment'
                        else:
                            rrc_state = 'unknown'
                        rrc_state_id = db.get_id('wcdma_rrc_state', {'value': rrc_state})
                        db.insert_to_event_table('event_' + packet_type, {'user_id': user_id, 'time': time, 'rrc_state_id': rrc_state_id, 'trust':trust})
                    elif packet_type == "WCDMA_RRC_Serv_Cell_Info":
                        insert_values = {}
                        insert_values['user_id'] = user_id
                        insert_values['time'] = time
                        insert_values['trust'] = trust 
                        for pair in pairs:
                            key = pair.get("key")
                            if key == "Cell ID":
                                insert_values['cid'] = int(pair.text)
                            elif key == "PSC":
                                insert_values['psc'] = int(pair.text)
                            elif key == "PLMN":
                                insert_values['plmn'] = pair.text
                            elif key == "LAC":
                                insert_values['lac'] = int(pair.text)
                            elif key == "RAC":
                                insert_values['rac'] = int(pair.text)
                        db.insert_to_event_table('event_' + packet_type, insert_values)
                    elif packet_type == "UMTS_NAS_GMM_State":
                        insert_values = {}
                        insert_values['user_id'] = user_id
                        insert_values['time'] = time
                        insert_values['trust'] = trust 
                        for pair in pairs:
                            key = pair.get("key")
                            if key == "GMM State":
                                insert_values['gmm_state_id'] = db.get_id('gmm_state', {'value': pair.text})
                            elif key == "GMM Substate":
                                insert_values['gmm_substate_id'] = db.get_id('gmm_substate', {'value': pair.text})
                            elif key == "GMM Update Status":
                                insert_values['gmm_update_status_id'] = db.get_id('gmm_update_status', {'value': pair.text})
                        db.insert_to_event_table('event_' + packet_type, insert_values)
                    elif packet_type == "UMTS_NAS_MM_State":
                        insert_values = {}
                        insert_values['user_id'] = user_id
                        insert_values['time'] = time
                        insert_values['trust'] = trust 
                        for pair in pairs:
                            key = pair.get("key")
                            if key == "MM State":
                                insert_values['mm_state_id'] = db.get_id('mm_state', {'value': pair.text})
                            elif key == "MM Substate":
                                insert_values['mm_substate_id'] = db.get_id('mm_substate', {'value': pair.text})
                            elif key == "MM Update Status":
                                insert_values['mm_update_status_id'] = db.get_id('mm_update_status', {'value': pair.text})
                        db.insert_to_event_table('event_' + packet_type, insert_values)
                    elif packet_type == "UMTS_NAS_MM_REG_State":
                        insert_values = {}
                        insert_values['user_id'] = user_id
                        insert_values['time'] = time
                        insert_values['trust'] = trust 
                        for pair in pairs:
                            key = pair.get("key")
                            if key == "Network operation mode":
                                insert_values['net_op_mode'] = int(pair.text)
                            elif key == "PLMN":
                                insert_values['plmn'] = pair.text
                            elif key == "LAC":
                                insert_values['lac'] = int(pair.text)
                            elif key == "RAC":
                                insert_values['rac'] = int(pair.text)
                        db.insert_to_event_table('event_' + packet_type, insert_values)
        if start_time is not None:
            db.insert_to_event_table('file_timestamp',\
                {'file_path': self.original_zip_file, 'file_name': self.original_log_name, 'user_id': user_id, 'start_time': start_time, 'end_time': end_time})



    def parse_to_message(self):
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

            rows_start = db.select_multiple_row('''select time from event_milog 
                where user_id = %d and `on` = 1 and time >= %d and time <= %d ''' 
                % (user_id, start_time - 1000000 * 120, start_time + 1000000 * 120))
            
            rows_end = db.select_multiple_row('''select time from event_milog 
                where user_id = %d and `on` = 0 and time >= %d and time <= %d ''' 
                % (user_id, end_time - 1000000 * 120, end_time + 1000000 * 120))
            
            if len(rows_start) == 0 and len(rows_end) == 0:
                trust = 2

        # 2nd pass to parse the file and insert to database 
        dm_log_packet_str = ""
        with open(file_path, 'r') as f:
            rrc_state = 'idle' 
            RSRP = None 
            RSRQ = None 
            dl_bandwidth = -1
            pre_cell_id = -1
            for line in f:
                if "<dm_log_packet>" in line:
                    dm_log_packet_str = line
                elif "<dm_log_packet>" not in line:
                    dm_log_packet_str = dm_log_packet_str + line
                if "</dm_log_packet>" in line:
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
                    sub_type = ""
                    extra = ""  
                    if packet_type == "Unsupported":
                        continue
                    #if packet_type in ['LTE_PHY_RLM_Report',
                    #    'LTE_NAS_EMM_OTA_Outgoing_Packet', 'LTE_MAC_Rach_Attempt', 'LTE_MAC_Configuration', \
                    #    'LTE_RLC_DL_Config_Log_Packet', 'LTE_RLC_UL_Config_Log_Packet', 'LTE_NAS_ESM_State', 'LTE_RRC_Serv_Cell_Info',\
                    #    'LTE_ML1_System_Scan_Results']:
                    #    pass
                    if packet_type == "LTE_RRC_OTA_Packet":
                        xml_str = dm_log_packet_str
                        sub_type = ""
                        if 'c1: systemInformationBlockType1' in xml_str:
                            sub_type = "SIB1"
                        elif 'c1: systemInformation (0)' in xml_str:
                            sub_type = "SIBN"
                        elif 'c1: rrcConnectionRequest (1)' in xml_str:
                            sub_type = 'rrcConnectionRequest'
                        elif 'c1: rrcConnectionSetup (3)' in xml_str:
                            sub_type = 'rrcConnectionSetup'
                            rrc_state = 'connected' 
                        elif 'c1: rrcConnectionSetupComplete (4)' in xml_str:
                            sub_type = 'rrcConnectionSetupComplete'
                        elif 'c1: securityModeCommand (6)' in xml_str:
                            sub_type = 'securityModeCommand'
                        elif 'c1: securityModeComplete (5)' in xml_str:
                            sub_type = 'securityModeComplete'
                        elif 'c1: rrcConnectionReconfiguration (4)' in xml_str:
                            sub_type = 'rrcConnectionReconfiguration'
                        elif 'c1: rrcConnectionReconfigurationComplete (2)' in xml_str:
                            sub_type = 'rrcConnectionReconfigurationComplete'
                        elif 'c1: rrcConnectionRelease (5)' in xml_str:
                            sub_type = 'rrcConnectionRelease'
                            rrc_state = 'idle' 
                        elif 'c1: measurementReport (1)' in xml_str:
                            sub_type = 'measurementReport'
                        elif 'c1: dlInformationTransfer (1)' in xml_str:
                            sub_type = 'dlInformationTransfer'
                        elif 'c1: ulInformationTransfer (9)' in xml_str:
                            sub_type = 'ulInformationTransfer'
                        elif 'c1: ueCapabilityEnquiry (7)' in xml_str:
                            sub_type = 'ueCapabilityEnquiry'
                        elif 'c1: ueCapabilityInformation (7)' in xml_str:
                            sub_type = 'ueCapabilityInformation'
                        elif 'c1: rrcConnectionReestablishmentRequest (0)' in xml_str:
                            sub_type = 'rrcConnectionReestablishmentRequest'
                        elif 'c1: rrcConnectionReestablishment (0)' in xml_str:
                            sub_type = 'rrcConnectionReestablishment'
                        elif 'c1: rrcConnectionReestablishmentComplete (3)' in xml_str:
                            sub_type = 'rrcConnectionReestablishmentComplete'
                        elif 'c1: mobilityFromEUTRACommand (3)' in xml_str:
                            sub_type = 'mobilityFromEUTRACommand'
                        elif 'c1: rrcConnectionReestablishmentReject (1)' in xml_str:
                            sub_type = 'rrcConnectionReestablishmentReject'
                        elif 'c1: rrcConnectionReject (2)' in xml_str:
                            sub_type = 'rrcConnectionReject'
                            rrc_state = 'idle' 
                        elif 'PCCH-Message' in xml_str:
                            sub_type = 'PCCH-Message'
                        else:
                            print "Unknown rrc state:" + xml_str
                            continue
                    
                    elif packet_type == 'LTE_PHY_Connected_Mode_Intra_Freq_Meas':
                        for pair in pairs:
                            key = pair.get("key")
                            if key == "RSRP(dBm)":
                                _RSRP = float(pair.text)
                                if abs(_RSRP - (-30)) > 0.01:
                                    RSRP = _RSRP
                            elif key == "RSRQ(dB)":
                                _RSRQ = float(pair.text)
                                if abs(_RSRQ) > 0.01:
                                    RSRQ = _RSRQ
                        extra = "RSRP=%f,RSRQ=%f" % (_RSRP, _RSRQ) 
                    
                    elif packet_type in ['LTE_MAC_DL_Transport_Block', 'LTE_MAC_UL_Transport_Block']:
                        n_byte = 0
                        n_sample = 0
                        for pair in pairs:
                            key = pair.get("key")
                            if key == "Subpackets":
                                adict = pair.find('list').find('item').find('dict')
                                for adict_pair in adict.findall('pair'):
                                    adict_key = adict_pair.get("key")
                                    if adict_key == "Num Samples":
                                        n_sample = int(adict_pair.text)
                                    elif adict_key == "Sample":
                                        for sub_pair in adict_pair.find('dict').findall('pair'):
                                            if sub_pair.get("key") in ["Grant (bytes)", "DL TBS (bytes)"]:
                                                n_byte += int(sub_pair.text)
                                break
                        extra = "byte=%d,sample=%d,dl_bw=%d" % (n_byte, n_sample, dl_bandwidth)
                    
                    elif packet_type == 'LTE_PHY_PDSCH_Packet':
                        TBS0 = None 
                        TBS1 = None 
                        MCS0 = None 
                        MCS1 = None 
                        n_txant = None
                        n_rxant = None
                        for pair in pairs:
                            key = pair.get("key")
                            if key == "Number of Tx Antennas(M)":
                                n_txant = int(pair.text)
                            elif key == "Number of Rx Antennas(N)":
                                n_rxant = int(pair.text)
                            elif key == "TBS 0":
                                TBS0 = int(pair.text)
                            elif key == "TBS 1":
                                TBS1 = int(pair.text)
                            elif key == "MCS 0":
                                MCS0 = pair.text
                            elif key == "MCS 1":
                                MCS1 = pair.text
                        extra = "TBS0=%d,TBS1=%d,MCS0=%s,MCS1=%s,dl_bw=%d" % (TBS0, TBS1,MCS0, MCS1, dl_bandwidth) 
                    elif packet_type == 'LTE_LL1_PCFICH_Decoding_Results':
                        extra = "dl_bw=%d" % (dl_bandwidth)
                         
                    elif packet_type == "LTE_RRC_MIB_Packet":
                        cell_id = -1
                        dl_bandwidth = -1
                        for pair in pairs:
                            key = pair.get("key")
                            if key == "Physical Cell ID":
                                cell_id = int(pair.text)
                            if key == "DL BW":
                                dl_bandwidth = int(pair.text[:-4])
                        extra = "dl_bw=%d,cell_id=%d,pre_cell_id=%d" % (dl_bandwidth, cell_id, pre_cell_id)
                        pre_cell_id = cell_id
                     
                    elif packet_type == "LTE_NAS_EMM_State":
                        for pair in pairs:
                            key = pair.get("key")
                            if key == "EMM State":
                                sub_type = pair.text
                            elif key == "EMM Substate":
                                sub_type = sub_type + ":" + pair.text
                    elif packet_type == 'LTE_PHY_Inter_RAT_Measurement':
                        match_obj = re.match(r'^.*key="Subpacket count">(.*?)</pair>(.*)', dm_log_packet_str, re.M|re.I)
                        if match_obj is not None:
                            subpacket_count = int(match_obj.group(1))
                        else:
                            subpacket_count = -1
                        extra = "subpacket_count=%d" % subpacket_count
                    elif packet_type in ['LTE_PHY_Connected_Mode_Neighbor_Measurement', 
                                    'LTE_PHY_Serv_Cell_Measurement',
                                    ]:
                        extra = ""
                    elif packet_type in ['LTE_RLC_DL_AM_All_PDU', 'LTE_RLC_UL_AM_All_PDU']:
                        pdu_bytes = 0
                        for pdu_bytes_start in [m.start() for m in re.finditer('pdu_bytes', dm_log_packet_str)]:
                            start_index = pdu_bytes_start + len('pdu_bytes">')
                            end_index = dm_log_packet_str.index('</pair>', start_index)
                            pdu_bytes += int(dm_log_packet_str[start_index:end_index])
                        extra = "pdu_bytes=%d" % pdu_bytes
                    elif packet_type == 'LTE_MAC_Rach_Trigger':
                        match_obj = re.match(r'^.*key="Rach reason">(.*?)</pair>(.*)', dm_log_packet_str, re.M|re.I)
                        if match_obj is not None:
                            rach_reason = match_obj.group(1)
                        extra = "rach_reason=%s" % rach_reason
                    else:
                        pass
                        print "Unknown message:" + packet_type
                    message_type_id = db.get_id('message_types', {'message_type': packet_type, 'message_sub_type': sub_type})
                    db.insert_to_event_table('messages', \
                        {'user_id': user_id, 'time': time, 'message_type_id': message_type_id,\
                        'extra': extra, 'signal_strength': RSRP})

