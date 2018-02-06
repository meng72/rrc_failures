#!/usr/local/bin/python

import re
import sys
import zipfile
import utils_analysis as ua

from utils import *
from os import listdir
from os.path import isfile, join

import bisect
import copy
import numpy

def get_mylog_running_time_and_insert_to_db(db, user_id, start_time, end_time):
    sql = '''select start_time, end_time from event_cpu_freq 
        where user_id = %d order by start_time ''' % user_id
    rows = db.select_multiple_row(sql)
    if len(rows) == 0:
        return
    sql_insert = 'insert ignore into mylog_running_time (`user_id`, `start_time`, `end_time`) values'
     
    interval_start_time = rows[0][0] 
    interval_end_time = rows[0][1] 
    for i in range(1, len(rows)):
        if rows[i - 1][1] == rows[i][0]:
            interval_end_time = rows[i][1]
        else:
            sql_insert = sql_insert + '(%d, %d, %d),' % (user_id, interval_start_time, interval_end_time)
            interval_start_time = rows[i][0] 
            interval_end_time = rows[i][1]
    sql_insert = sql_insert + '(%d, %d, %d),' % (user_id, interval_start_time, interval_end_time)
    sql_insert = sql_insert[:-1]
    
    db.execute(sql_insert) 

def get_milog_running_time_and_insert_to_db(db, user_id, start_time, end_time):
    sql = '''insert ignore into milog_running_time 
        select user_id, start_time, end_time from file_timestamp
        where file_name like 'diag_log%%' and user_id = %d ''' % user_id
    db.execute(sql)

def get_running_time_and_insert_to_db(db, user_id, start_time, end_time):
    sql = '''select start_time, end_time from mylog_running_time where user_id = %d order by start_time ''' % user_id
    mylog_intervals = db.select_multiple_row(sql)
    sql = '''select start_time, end_time from milog_running_time where user_id = %d order by start_time ''' % user_id
    milog_intervals = db.select_multiple_row(sql)
    mylog_i = 0
    milog_i = 0
    while mylog_i < len(mylog_intervals) and milog_i < len(milog_intervals):
        if mylog_intervals[mylog_i][1] <= milog_intervals[milog_i][0]:
            mylog_i += 1
        elif mylog_intervals[mylog_i][0] >= milog_intervals[milog_i][1]:
            milog_i += 1
        else:
            _start_time = max(mylog_intervals[mylog_i][0], milog_intervals[milog_i][0]) 
            _end_time = min(mylog_intervals[mylog_i][1], milog_intervals[milog_i][1]) 
            db.execute(''' insert ignore into running_time (`user_id`, `start_time`, `end_time`) 
                values (%d, %d, %d) ''' % (user_id, _start_time, _end_time))
            if mylog_intervals[mylog_i][1] < milog_intervals[milog_i][1]:
                mylog_i += 1
            else:
                milog_i += 1 

def day_to_time(day):
    if day < 0:
        return -1
    return (day * 3600 * 24 + 14400) * 1000000 
    
def time_to_day(time):
    if time < 0:
        return -1
    return (time / 1000000 - 14400) / (3600 * 24) 

INITIAL_START_PROCESS_TIME = 1491019200 * 1000000
DAY = 24 * 3600 * 1000000 
    
def parse_extra(extra_text):
    result = {}
    if extra_text is None:
        return result
    tokens = extra_text.split(',')
    for token in tokens:
        subtokens = token.split('=')
        if len(subtokens) == 2:
            key = subtokens[0]
            value = subtokens[1]
            result[key] = value
    return result

def serialize_extra(extra_dict):
    result = ""
    for key in extra_dict:
        result = result + key + '=' + str(extra_dict[key]) + ','
    if len(result) > 0:
        result = result[:-1]
    return result
def get_messages_and_insert_to_db(db, user_id, start_time, end_time):

    insert_sql_head = "insert ignore into messages (`user_id`, `time`, `message_type_id`, `trust`) values "
    insert_sql = insert_sql_head
    count = 0
    for message_type in [
#'LTE_LL1_PCFICH_Decoding_Results',
        'LTE_PHY_Connected_Mode_Neighbor_Measurement',
        'LTE_PHY_Serv_Cell_Measurement',
        'LTE_PHY_Inter_RAT_Measurement',
        'LTE_PHY_PDSCH_Packet',
        'LTE_RRC_Serv_Cell_Info',
        'LTE_RRC_MIB_Message_Log_Packet',
        'LTE_NAS_EMM_OTA_Incoming_Packet',
        'LTE_NAS_EMM_OTA_Outgoing_Packet',
        'LTE_NAS_ESM_OTA_Incoming_Packet',
        'LTE_NAS_ESM_OTA_Outgoing_Packet',
        'LTE_NAS_ESM_State',
        ]:
        message_type_id = db.get_id('message_types', {'message_type': message_type, 'message_sub_type': ''})
        sql = ''' select time, trust from event_%s 
            where user_id = %d and time >= %d and time <= %d order by time''' % (message_type, user_id, start_time, end_time)
        rows = db.select_multiple_row(sql)
        for row in rows:
            time = row[0]
            trust = row[1]
            current_value = "(%d, %d, %d, %d)," % (user_id, time, message_type_id, trust)
            insert_sql = insert_sql + current_value
            count += 1
            if count > 1000:
                insert_sql = insert_sql[:-1]
                db.execute(insert_sql)
                insert_sql = insert_sql_head
                count = 0
            # db.insert_to_event_table('messages', {'user_id': user_id, 'time': time, 'message_type_id': message_type_id, \
            #     'trust': trust})
    if count > 0:
        insert_sql = insert_sql[:-1]
        db.execute(insert_sql)

    insert_sql_head = "insert ignore into messages (`user_id`, `time`, `message_type_id`, `trust`, `extra`) values "
    insert_sql = insert_sql_head
    count = 0
    message_type = 'LTE_PHY_Connected_Mode_Intra_Freq_Meas'
    message_type_id = db.get_id('message_types', {'message_type': message_type, 'message_sub_type': ''})
    sql = ''' select time, trust, RSRP from event_%s 
        where user_id = %d and time >= %d and time <= %d order by time''' % (message_type, user_id, start_time, end_time)
    rows = db.select_multiple_row(sql)
    for row in rows:
        time = row[0]
        trust = row[1]
        RSRP = row[2]
        current_value = "(%d, %d, %d, %d, '%s')," % (user_id, time, message_type_id, trust, serialize_extra({'RSRP': RSRP}))
        insert_sql = insert_sql + current_value
        count += 1
        if count > 1000:
            insert_sql = insert_sql[:-1]
            db.execute(insert_sql)
            insert_sql = insert_sql_head
            count = 0
        # db.insert_to_event_table('messages', {'user_id': user_id, 'time': time, 'message_type_id': message_type_id, \
        #     'trust': trust, 'extra': serialize_extra({'RSRP': RSRP}) })
    if count > 0:
        insert_sql = insert_sql[:-1]
        db.execute(insert_sql)

    insert_sql_head = "insert ignore into messages (`user_id`, `time`, `message_type_id`, `trust`) values "
    insert_sql = insert_sql_head
    count = 0
    message_type = 'LTE_MAC_Transport_Block'
    message_type_id = db.get_id('message_types', {'message_type': 'LTE_MAC_UL_Transport_Block', 'message_sub_type': ''})
    sql = ''' select time, trust, direction from event_%s 
            where user_id = %d and direction = 'snd' and time >= %d and time <= %d order by time''' % (message_type, user_id, start_time, end_time)
    rows = db.select_multiple_row(sql)
    for row in rows:
        time = row[0]
        trust = row[1]
        direction = row[2]
        current_value = "(%d, %d, %d, %d)," % (
        user_id, time, message_type_id, trust)
        insert_sql = insert_sql + current_value
        count += 1
        if count > 1000:
            insert_sql = insert_sql[:-1]
            db.execute(insert_sql)
            insert_sql = insert_sql_head
            count = 0
        # db.insert_to_event_table('messages', {'user_id': user_id, 'time': time, 'message_type_id': message_type_id, \
        #     'trust': trust })
    if count > 0:
        insert_sql = insert_sql[:-1]
        db.execute(insert_sql)

    insert_sql_head = "insert ignore into messages (`user_id`, `time`, `message_type_id`, `trust`) values "
    insert_sql = insert_sql_head
    count = 0
    message_type = 'LTE_MAC_Transport_Block'
    message_type_id = db.get_id('message_types', {'message_type': 'LTE_MAC_DL_Transport_Block', 'message_sub_type': ''})
    sql = ''' select time, trust, direction from event_%s 
                where user_id = %d and direction = 'rcv' and time >= %d and time <= %d order by time''' % (
    message_type, user_id, start_time, end_time)
    rows = db.select_multiple_row(sql)
    for row in rows:
        time = row[0]
        trust = row[1]
        direction = row[2]
        current_value = "(%d, %d, %d, %d)," % (
            user_id, time, message_type_id, trust)
        insert_sql = insert_sql + current_value
        count += 1
        if count > 1000:
            insert_sql = insert_sql[:-1]
            db.execute(insert_sql)
            insert_sql = insert_sql_head
            count = 0
            # db.insert_to_event_table('messages', {'user_id': user_id, 'time': time, 'message_type_id': message_type_id, \
            #     'trust': trust })
    if count > 0:
        insert_sql = insert_sql[:-1]
        db.execute(insert_sql)

    insert_sql_head = "insert ignore into messages (`user_id`, `time`, `message_type_id`, `trust`, `extra`) values "
    insert_sql = insert_sql_head
    count = 0
    message_type = 'LTE_RLC_AM_All_PDU'
    message_type_id = db.get_id('message_types', {'message_type': 'LTE_RLC_UL_AM_All_PDU', 'message_sub_type': ''})
    sql = ''' select time, trust, direction, pdu_bytes from event_%s 
                where user_id = %d and direction = 'snd' and time >= %d and time <= %d order by time
            ''' % (message_type, user_id, start_time, end_time)
    rows = db.select_multiple_row(sql)
    for row in rows:
        time = row[0]
        trust = row[1]
        direction = row[2]
        pdu_bytes = row[3]
        current_value = "(%d, %d, %d, %d, '%s')," % (
            user_id, time, message_type_id, trust, serialize_extra({'pdu_bytes': pdu_bytes}) )
        insert_sql = insert_sql + current_value
        count += 1
        if count > 1000:
            insert_sql = insert_sql[:-1]
            db.execute(insert_sql)
            insert_sql = insert_sql_head
            count = 0
        # db.insert_to_event_table('messages', {'user_id': user_id, 'time': time, 'message_type_id': message_type_id, \
        #                                    'trust': trust, 'extra': serialize_extra({'pdu_bytes': pdu_bytes})})
    if count > 0:
        insert_sql = insert_sql[:-1]
        db.execute(insert_sql)

    insert_sql_head = "insert ignore into messages (`user_id`, `time`, `message_type_id`, `trust`, `extra`) values "
    insert_sql = insert_sql_head
    count = 0
    message_type = 'LTE_RLC_AM_All_PDU'
    message_type_id = db.get_id('message_types', {'message_type': 'LTE_RLC_DL_AM_All_PDU', 'message_sub_type': ''})
    sql = ''' select time, trust, direction, pdu_bytes from event_%s 
                    where user_id = %d and direction = 'rcv' and time >= %d and time <= %d order by time
                ''' % (message_type, user_id, start_time, end_time)
    rows = db.select_multiple_row(sql)
    for row in rows:
        time = row[0]
        trust = row[1]
        direction = row[2]
        pdu_bytes = row[3]
        current_value = "(%d, %d, %d, %d, '%s')," % (
            user_id, time, message_type_id, trust, serialize_extra({'pdu_bytes': pdu_bytes}))
        insert_sql = insert_sql + current_value
        count += 1
        if count > 1000:
            insert_sql = insert_sql[:-1]
            db.execute(insert_sql)
            insert_sql = insert_sql_head
            count = 0
            # db.insert_to_event_table('messages', {'user_id': user_id, 'time': time, 'message_type_id': message_type_id, \
            #                                    'trust': trust, 'extra': serialize_extra({'pdu_bytes': pdu_bytes})})
    if count > 0:
        insert_sql = insert_sql[:-1]
        db.execute(insert_sql)

    insert_sql_head = "insert ignore into messages (`user_id`, `time`, `message_type_id`, `trust`) values "
    insert_sql = insert_sql_head
    count = 0
    message_type = 'LTE_RRC_OTA_Packet'
    sql = ''' select time, trust, rrc_state_id from event_%s 
            where user_id = %d and time >= %d and time <= %d order by time''' % (message_type, user_id, start_time, end_time)
    rows = db.select_multiple_row(sql)
    for row in rows:
        time = row[0]
        trust = row[1]
        rrc_state_id = row[2]
        sub_rows = db.select_multiple_row('''select value from lte_rrc_state where id = %d''' % rrc_state_id)
        if len(sub_rows) == 0:
            print "Error in select rrc_state_id"
            return
        message_sub_type = sub_rows[0][0]
        message_type_id = db.get_id('message_types', {'message_type': message_type, 'message_sub_type': message_sub_type})
        current_value = "(%d, %d, %d, %d)," % (
            user_id, time, message_type_id, trust )
        insert_sql = insert_sql + current_value
        count += 1
        if count > 1000:
            insert_sql = insert_sql[:-1]
            db.execute(insert_sql)
            insert_sql = insert_sql_head
            count = 0
        # db.insert_to_event_table('messages', {'user_id': user_id, 'time': time, 'message_type_id': message_type_id, \
        #     'trust': trust })
    if count > 0:
        insert_sql = insert_sql[:-1]
        db.execute(insert_sql)

    insert_sql_head = "insert ignore into messages (`user_id`, `time`, `message_type_id`, `trust`, `extra`) values "
    insert_sql = insert_sql_head
    count = 0
    message_type = 'LTE_RRC_MIB_Packet'
    message_type_id = db.get_id('message_types', {'message_type': message_type, 'message_sub_type': ''})
    sql = ''' select time, trust, dl_bandwidth, pcid from event_%s 
                where user_id = %d and time >= %d and time <= %d order by time''' % (message_type, user_id, start_time, end_time)
    rows = db.select_multiple_row(sql)
    for row in rows:
        time = row[0]
        trust = row[1]
        dl_bw = row[2]
        cell_id = row[3]
        current_value = "(%d, %d, %d, %d, '%s')," % (
            user_id, time, message_type_id, trust, serialize_extra({'dl_bw': dl_bw, 'cell_id': cell_id}) )
        insert_sql = insert_sql + current_value
        count += 1
        if count > 1000:
            insert_sql = insert_sql[:-1]
            db.execute(insert_sql)
            insert_sql = insert_sql_head
            count = 0
        # db.insert_to_event_table('messages', {'user_id': user_id, 'time': time, 'message_type_id': message_type_id, \
        #     'trust': trust, 'extra': serialize_extra({'dl_bw': dl_bw, 'cell_id': cell_id})})
    if count > 0:
        insert_sql = insert_sql[:-1]
        db.execute(insert_sql)

    insert_sql_head = "insert ignore into messages (`user_id`, `time`, `message_type_id`, `trust`) values "
    insert_sql = insert_sql_head
    count = 0
    message_type = 'LTE_NAS_EMM_State'
    sql = ''' select time, trust, emm_state_id, emm_substate_id from event_%s 
                where user_id = %d and time >= %d and time <= %d order by time
                ''' % (message_type, user_id, start_time, end_time)
    rows = db.select_multiple_row(sql)
    for row in rows:
        time = row[0]
        trust = row[1]
        emm_state_id = row[2]
        emm_substate_id = row[3]
        sub_rows = db.select_multiple_row('''select value from emm_state where id = %d''' % emm_state_id)
        if len(sub_rows) == 0:
            print "Error in select emm_state_id"
            return
        emm_state = sub_rows[0][0]
        sub_rows = db.select_multiple_row('''select value from emm_substate where id = %d''' % emm_substate_id)
        if len(sub_rows) == 0:
            print "Error in select emm_substate_id"
            return
        emm_substate = sub_rows[0][0]
        message_sub_type = emm_state + ':' + emm_substate
        message_type_id = db.get_id('message_types',
                                    {'message_type': message_type, 'message_sub_type': message_sub_type})
        current_value = "(%d, %d, %d, %d)," % (
            user_id, time, message_type_id, trust)
        insert_sql = insert_sql + current_value
        count += 1
        if count > 1000:
            insert_sql = insert_sql[:-1]
            db.execute(insert_sql)
            insert_sql = insert_sql_head
            count = 0
        # db.insert_to_event_table('messages', {'user_id': user_id, 'time': time, 'message_type_id': message_type_id, \
        #                                    'trust': trust})
    if count > 0:
        insert_sql = insert_sql[:-1]
        db.execute(insert_sql)

def get_messages_and_insert_to_db_fix_bug(db, user_id, start_time, end_time):

    insert_sql_head = "insert ignore into messages (`user_id`, `time`, `message_type_id`, `trust`, `extra`) values "
    insert_sql = insert_sql_head
    count = 0
    message_type = 'LTE_RLC_AM_All_PDU'
    message_type_id = db.get_id('message_types', {'message_type': 'LTE_RLC_UL_AM_All_PDU', 'message_sub_type': ''})
    sql = ''' select time, trust, direction, pdu_bytes from event_%s 
                where user_id = %d and direction = 'snd' and time >= %d and time <= %d order by time
            ''' % (message_type, user_id, start_time, end_time)
    rows = db.select_multiple_row(sql)
    for row in rows:
        time = row[0]
        trust = row[1]
        direction = row[2]
        pdu_bytes = row[3]
        current_value = "(%d, %d, %d, %d, '%s')," % (
            user_id, time, message_type_id, trust, serialize_extra({'pdu_bytes': pdu_bytes}) )
        insert_sql = insert_sql + current_value
        count += 1
        if count > 1000:
            insert_sql = insert_sql[:-1]
            db.execute(insert_sql)
            insert_sql = insert_sql_head
            count = 0
        # db.insert_to_event_table('messages', {'user_id': user_id, 'time': time, 'message_type_id': message_type_id, \
        #                                    'trust': trust, 'extra': serialize_extra({'pdu_bytes': pdu_bytes})})
    if count > 0:
        insert_sql = insert_sql[:-1]
        db.execute(insert_sql)

    insert_sql_head = "insert ignore into messages (`user_id`, `time`, `message_type_id`, `trust`, `extra`) values "
    insert_sql = insert_sql_head
    count = 0
    message_type = 'LTE_RLC_AM_All_PDU'
    message_type_id = db.get_id('message_types', {'message_type': 'LTE_RLC_DL_AM_All_PDU', 'message_sub_type': ''})
    sql = ''' select time, trust, direction, pdu_bytes from event_%s 
                    where user_id = %d and direction = 'rcv' and time >= %d and time <= %d order by time
                ''' % (message_type, user_id, start_time, end_time)
    rows = db.select_multiple_row(sql)
    for row in rows:
        time = row[0]
        trust = row[1]
        direction = row[2]
        pdu_bytes = row[3]
        current_value = "(%d, %d, %d, %d, '%s')," % (
            user_id, time, message_type_id, trust, serialize_extra({'pdu_bytes': pdu_bytes}))
        insert_sql = insert_sql + current_value
        count += 1
        if count > 1000:
            insert_sql = insert_sql[:-1]
            db.execute(insert_sql)
            insert_sql = insert_sql_head
            count = 0
            # db.insert_to_event_table('messages', {'user_id': user_id, 'time': time, 'message_type_id': message_type_id, \
            #                                    'trust': trust, 'extra': serialize_extra({'pdu_bytes': pdu_bytes})})
    if count > 0:
        insert_sql = insert_sql[:-1]
        db.execute(insert_sql)

def get_messages_and_insert_to_db_fix_bug2(db, user_id, start_time, end_time):

    insert_sql_head = "insert ignore into messages (`user_id`, `time`, `message_type_id`, `trust`, `extra`) values "
    insert_sql = insert_sql_head
    count = 0
    message_type = 'LTE_MAC_Transport_Block'
    message_type_id = db.get_id('message_types', {'message_type': 'LTE_MAC_UL_Transport_Block', 'message_sub_type': ''})
    sql = ''' select time, trust, direction, n_byte, n_sample from event_%s 
                where user_id = %d and direction = 'snd' and time >= %d and time <= %d order by time
            ''' % (message_type, user_id, start_time, end_time)
    rows = db.select_multiple_row(sql)
    for row in rows:
        time = row[0]
        trust = row[1]
        direction = row[2]
        n_byte = row[3]
        n_sample = row[4]
        current_value = "(%d, %d, %d, %d, '%s')," % (
            user_id, time, message_type_id, trust, serialize_extra({'byte': n_byte, 'sample': n_sample}) )
        insert_sql = insert_sql + current_value
        count += 1
        if count > 1000:
            insert_sql = insert_sql[:-1]
            db.execute(insert_sql)
            insert_sql = insert_sql_head
            count = 0
    if count > 0:
        insert_sql = insert_sql[:-1]
        db.execute(insert_sql)

    insert_sql_head = "insert ignore into messages (`user_id`, `time`, `message_type_id`, `trust`, `extra`) values "
    insert_sql = insert_sql_head
    count = 0
    message_type = 'LTE_MAC_Transport_Block'
    message_type_id = db.get_id('message_types', {'message_type': 'LTE_MAC_DL_Transport_Block', 'message_sub_type': ''})
    sql = ''' select time, trust, direction, n_byte, n_sample from event_%s 
                where user_id = %d and direction = 'rcv' and time >= %d and time <= %d order by time
            ''' % (message_type, user_id, start_time, end_time)
    rows = db.select_multiple_row(sql)
    for row in rows:
        time = row[0]
        trust = row[1]
        direction = row[2]
        n_byte = row[3]
        n_sample = row[4]
        current_value = "(%d, %d, %d, %d, '%s')," % (
            user_id, time, message_type_id, trust, serialize_extra({'byte': n_byte, 'sample': n_sample}) )
        insert_sql = insert_sql + current_value
        count += 1
        if count > 1000:
            insert_sql = insert_sql[:-1]
            db.execute(insert_sql)
            insert_sql = insert_sql_head
            count = 0
    if count > 0:
        insert_sql = insert_sql[:-1]
        db.execute(insert_sql)

def get_serv_cell_id_and_update_db(db, user_id, start_time, end_time):
    message_type = 'LTE_PHY_Connected_Mode_Intra_Freq_Meas'
    message_type_id = db.get_id('message_types', {'message_type': 'LTE_PHY_Connected_Mode_Intra_Freq_Meas', 'message_sub_type': ''})
    sql = ''' select time, E_ARFCN, spcid from event_%s 
                    where user_id = %d and time >= %d and time <= %d order by time
                ''' % (message_type, user_id, start_time, end_time)
    rows = db.select_multiple_row(sql)
    for row in rows:
        time = row[0]
        E_ARFCN = row[1]
        spcid = row[2]
        db.execute(''' update messages set extra = concat(extra, ',E_ARFCN=%d,spcid=%d')
            where user_id = %d and time = %d and extra not like '%%E_ARFCN=%d,spcid=%d%%'
            ''' % (E_ARFCN, spcid, user_id, time, E_ARFCN, spcid))

def get_rrc_serv_cell_info_and_update_db(db, user_id, start_time, end_time):
    message_type = 'LTE_RRC_Serv_Cell_Info'
    sql = ''' select time, dl_freq, ul_freq, dl_bandwidth, ul_bandwidth from event_%s 
                    where user_id = %d and time >= %d and time <= %d order by time
                ''' % (message_type, user_id, start_time, end_time)
    rows = db.select_multiple_row(sql)
    for row in rows:
        time = row[0]
        dl_freq = row[1]
        ul_freq = row[2]
        dl_bandwidth = row[3]
        ul_bandwidth = row[4]
        db.execute(''' update messages set extra = concat(extra, ',dl_freq=%d,ul_freq=%d,dl_bandwidth=%d,ul_bandwidth=%d')
            where user_id = %d and time = %d and extra not like '%%dl_freq=%d,ul_freq=%d,dl_bandwidth=%d,ul_bandwidth=%d%%'
            ''' % (dl_freq, ul_freq, dl_bandwidth, ul_bandwidth, user_id, time,\
            dl_freq, ul_freq, dl_bandwidth, ul_bandwidth))

def get_rrc_ota_packet_freq_and_update_db(db, user_id, start_time, end_time):
    message_type = 'LTE_RRC_OTA_Packet'
    sql = ''' select time, freq from event_%s 
          where user_id = %d and time >= %d and time <= %d order by time
        ''' % (message_type, user_id, start_time, end_time)
    rows = db.select_multiple_row(sql)
    for row in rows:
        time = row[0]
        freq = row[1]
        db.execute(''' update messages set extra = concat(extra, ',freq=%d')
            where user_id = %d and time = %d and extra not like '%%freq=%d%%'
            ''' % (freq, user_id, time, freq))

def get_signal_strength_and_update_db(db, user_id, start_time, end_time):
    LTE_PHY_Connected_Mode_Intra_Freq_Meas_id = 9
    LTE_RRC_Serv_Cell_Info_id = 11

    freq_rows = db.select_multiple_row(''' select time, extra, message_type_id from messages 
        where (message_type_id = 11 or (message_type_id >= 14 and message_type_id <= 35)) 
        and user_id = %d and time >= %d and time <= %d order by time
        ''' % (user_id, start_time, end_time))

    freq_periods = []
    pre_time = -1
    freq = -1
    pre_freq = -1
    for row in freq_rows:
        time = row[0]
        extra = row[1]
        extra_dict = parse_extra(extra)
        freq = int(extra_dict['dl_freq']) if row[2] == LTE_RRC_Serv_Cell_Info_id else int(extra_dict['freq'])
        if freq == pre_freq:
            continue
        if pre_time < 0:
            freq_periods.append([start_time, time, freq])
        else:
            freq_periods.append([pre_time, time, pre_freq])
        pre_time = time
        pre_freq = freq
    if pre_time > 0:
        freq_periods.append([pre_time, end_time, pre_freq])

    rows = db.select_multiple_row(''' select time, extra from messages 
        where message_type_id = %d and user_id = %d and time >= %d and time <= %d order by time
        ''' % (LTE_PHY_Connected_Mode_Intra_Freq_Meas_id, user_id, start_time, end_time))
    pre_time = -1
    pre_signal_strength = -1
    freq = -1
    for row in rows:
        time = row[0]
        extra = row[1]
        extra_dict = parse_extra(extra)
        if extra_dict.get('RSRP') == None:
            continue
        signal_strength = float(extra_dict['RSRP'])
        earfcn = int(extra_dict['E_ARFCN'])
        i = 0
        while i < len(freq_periods):
            if freq_periods[i][0] < time and time < freq_periods[i][1]:
                i += 1
                break
            i += 1
        freq = freq_periods[i - 1][2]
        if freq != earfcn:
            continue
        if abs(signal_strength - (-30)) < 0.1:
            continue
        if abs(signal_strength - pre_signal_strength) < 0.01:
            continue
        if pre_time > 0:
            db.execute(''' update messages set signal_strength = %f 
                        where user_id = %d and time >= %d and time <= %d
                        ''' % (pre_signal_strength, user_id, pre_time, time))
            #print 'From %d to %d, pre_signal_strength %d with earfcn %d' % (pre_time, time, pre_signal_strength, earfcn)
        else:
            db.execute(''' update messages set signal_strength = %f 
                        where user_id = %d and time >= %d and time <= %d
                        ''' % (signal_strength, user_id, start_time, time))
            #print 'From %d to %d, pre_signal_strength %d with earfcn %d' % (start_time, time, signal_strength, earfcn)
        pre_time = time
        pre_signal_strength = signal_strength
    if pre_time > 0:
        db.execute(''' update messages set signal_strength = %f 
                    where user_id = %d and time >= %d and time <= %d
                    ''' % (pre_signal_strength, user_id, pre_time, end_time))
        #print 'From %d to %d, pre_signal_strength %d with earfcn %d' % (pre_time, time, pre_signal_strength, earfcn)
    return

def get_signal_strength_for_procedure_and_update_db(db, user_id, start_time, end_time):
    sql = ''' select start_time from procedures where user_id = %d and
            start_time >= %d and end_time <= %d order by start_time'''\
            % (user_id, start_time, end_time)
    rows = db.select_multiple_row(sql)
    for row in rows:
        _start_time = row[0]
        sql = ''' select signal_strength from messages 
                where user_id = %d and time >= %d limit 1'''\
                % (user_id, _start_time)
        ss_rows = db.select_multiple_row(sql)
        signal_strength = ss_rows[0][0]
        db.execute(''' update procedures set signal_strength = %f
                    where user_id = %d and start_time = %d '''\
                    % (signal_strength, user_id, _start_time))

def get_pre_cell_id_and_update_db(db, user_id, start_time, end_time):
    LTE_RRC_MIB_Packet_id = 12
    rows = db.select_multiple_row(''' select time, extra from messages 
            where message_type_id = %d and user_id = %d and time >= %d and time <= %d order by time
            ''' % (LTE_RRC_MIB_Packet_id, user_id, start_time, end_time))
    pre_cell_id = -1
    for row in rows:
        time = row[0]
        extra = row[1]
        extra_dict = parse_extra(extra)
        cell_id = extra_dict['cell_id']
        new_extra_dict = extra_dict
        new_extra_dict['pre_cell_id'] = pre_cell_id
        new_extra = serialize_extra(new_extra_dict)
        pre_cell_id = cell_id
        db.execute(''' update messages set extra = '%s' 
            where user_id = %d and time = %d and message_type_id = %d
            ''' % (new_extra, user_id, time, LTE_RRC_MIB_Packet_id))
    return


def get_emm_state_ids(db):
    result = {}
    rows = db.select_multiple_row(''' select id, message_sub_type from message_types 
        where message_type = 'LTE_NAS_EMM_State' order by message_sub_type ''')
    for row in rows:
        _id = row[0]
        message_sub_type = row[1]
        tokens = message_sub_type.split(':')
        emm_state = tokens[0]
        emm_substate = tokens[1]
        emm_state_id = db.get_id('emm_states', {'value': emm_state})
        emm_substate_id = db.get_id('emm_substates', {'value': emm_substate})
        result[_id] = [emm_state_id, emm_substate_id]
    return result

def get_emm_state_and_update_table(db, user_id, start_time, end_time, state_ids):
    rows_pre = db.select_multiple_row(''' select time, emm_state_id, emm_substate_id from messages 
        where user_id = %d and time < %d order by time desc limit 1
        ''' % (user_id, start_time))
    pre_time = start_time
    pre_emm_state_id = -1
    pre_emm_substate_id = -1
    if len(rows_pre) != 0:
        if start_time - rows_pre[0][0] < 15 * 1000000:
            pre_emm_state_id = rows_pre[0][1]
            pre_emm_substate_id = rows_pre[0][2]
    rows = db.select_multiple_row(''' select time, message_type_id from messages 
        where user_id = %d and time >= %d and time <= %d and message_type_id in (%s)  order by time
        ''' % (user_id, start_time, end_time, ','.join(str(x) for x in state_ids) ) )
    for row in rows:
        time = row[0]
        if pre_emm_state_id > 0:
            db.execute(''' update messages set emm_state_id = %d, emm_substate_id = %d 
                where user_id = %d and time >= %d and time < %d
                ''' %(pre_emm_state_id, pre_emm_substate_id, user_id, pre_time, time))
        message_type_id = row[1]
        emm_state_id = state_ids[message_type_id][0]
        emm_substate_id = state_ids[message_type_id][1]
        pre_time = time
        pre_emm_state_id = emm_state_id
        pre_emm_substate_id = emm_substate_id

    if pre_emm_state_id > 0:
        db.execute(''' update messages set emm_state_id = %d, emm_substate_id = %d 
                where user_id = %d and time >= %d and time <= %d
                ''' % (pre_emm_state_id, pre_emm_substate_id, user_id, pre_time, end_time))


def get_reason_for_rrc_connection(db, user_id, start_time, end_time):
    rows = db.select_multiple_row(''' select time from messages 
        where user_id = %d and time >= %d and time <= %d and message_type_id = 18
        ''' % (user_id, start_time, end_time))
    for row in rows:
        rows_reason = db.select_multiple_row(''' select time from messages 
            where user_id = %d and time >= %d and time <= %d and message_type_id in (42, 50, 52, 54)
            ''' % (user_id, row[0] - 1000000, row[0]))
        if len(rows_reason) != 1:
            print "%d %d" % (row[0], len(rows_reason))
    return

def get_nas_procedure_and_insert_to_db(db, user_id, start_time, end_time):
    sql = ''' select time, nas_msg_emm_type_id
        from event_LTE_NAS_EMM_OTA_Incoming_Packet
        where user_id = %d and time >= %d and time <= %d
        order by time''' % (user_id, start_time, end_time)
    incoming_rows = db.select_multiple_row(sql)
    
    sql = ''' select time, nas_msg_emm_type_id
        from event_LTE_NAS_EMM_OTA_Outgoing_Packet
        where user_id = %d and time >= %d and time <= %d
        order by time''' % (user_id, start_time, end_time)
    outgoing_rows = db.select_multiple_row(sql)
    
    #print str(incoming_rows)
    #print str(outgoing_rows)
    PROCS_REQ = [12, 14, 6, 10, 5, 3, 1, 11, 17, 18, 25]
    PROCS_END_SUCCEED = [13, 15, 7, 10, 9, 4, 16, 11, 17, 18, 25]
    PROCS_END_FAIL = [21, 20]
    PROCS_END_WAIT = [2]
    procs = []
    procs_dir = []
    procs_time = []
    proc_types = []
    proc_stats = []
    i = 0
    j = 0
    nas_msg_time = 0
    nas_msg_emm_type_id = 0
    nas_msg_direction = 0 # 1 for incoming. 2 for outgoing.
    nas_proc_type = 0
    while i <= len(incoming_rows) and j <= len(outgoing_rows):
        if i == len(incoming_rows) and j == len(outgoing_rows):
            break
        """
        if i < len(incoming_rows) and incoming_rows[i][1] == None:
            i += 1
            continue
        if j < len(outgoing_rows) and outgoing_rows[j][1] == None:
            j += 1
            continue
        """
        if i == len(incoming_rows):
            nas_msg_time = outgoing_rows[j][0]
            nas_msg_emm_type_id = outgoing_rows[j][1] if outgoing_rows[j][1] != None else 25
            nas_msg_direction = 2
            j += 1
        elif j == len(outgoing_rows):
            nas_msg_time = incoming_rows[i][0]
            nas_msg_emm_type_id = incoming_rows[i][1] if incoming_rows[i][1] != None else 25
            nas_msg_direction = 1
            i += 1
        elif incoming_rows[i][0] < outgoing_rows[j][0]:
            nas_msg_time = incoming_rows[i][0]
            nas_msg_emm_type_id = incoming_rows[i][1] if incoming_rows[i][1] != None else 25
            nas_msg_direction = 1
            i += 1
        elif incoming_rows[i][0] > outgoing_rows[j][0]:
            nas_msg_time = outgoing_rows[j][0]
            nas_msg_emm_type_id = outgoing_rows[j][1] if outgoing_rows[j][1] != None else 25
            nas_msg_direction = 2
            j += 1
        else:
            print 'ERROR in get_nas_procedure_and_insert_to_db: incoming_rows[%d] and outgoing_rows[%d] have the same time' % (i, j)

        if nas_msg_emm_type_id in [12, 13]:
            # Authentication procedure
            nas_proc_type = 2
        elif nas_msg_emm_type_id in [14, 15]:
            # Security mode control procedure
            nas_proc_type = 3
        elif nas_msg_emm_type_id in [6, 7]:
            # Identity procedure
            nas_proc_type = 4
        elif nas_msg_emm_type_id in [10]:
            # EMM information procedure
            nas_proc_type = 5
        elif nas_msg_emm_type_id in [5, 8, 9, 21]:
            # Attach procedure
            nas_proc_type = 6
        elif nas_msg_emm_type_id in [3, 4]:
            # Detach procedure
            nas_proc_type = 7
        elif nas_msg_emm_type_id in [1, 2, 16, 20]:
            # Tracking area updating procedure
            nas_proc_type = 8
        elif nas_msg_emm_type_id in [25]:
            # Service request procedure
            nas_proc_type = 9
        elif nas_msg_emm_type_id in [11]:
            # Extended service request procedure
            nas_proc_type = 10
        elif nas_msg_emm_type_id in [17, 18]:
            # Transport of NAS messages procedure
            nas_proc_type = 12
        else:
            print 'Unknown procedure ... for nas_msg_emm_type_id %d' % (nas_msg_emm_type_id)
            nas_proc_type = -1
            continue

        if nas_proc_type in proc_types and nas_msg_emm_type_id in PROCS_REQ:
            nas_proc_index = proc_types.index(nas_proc_type)
            if proc_stats[nas_proc_index] in [-1, 1] and\
                check_nas_procedure_status(proc_types[nas_proc_index], procs[nas_proc_index], proc_stats[nas_proc_index]) == False:
                proc_stats[nas_proc_index] = 2
            db.insert_to_event_table('nas_procedures', {'user_id': user_id, 'start_time': procs_time[nas_proc_index][0], 'end_time': procs_time[nas_proc_index][len(procs_time[nas_proc_index]) - 1], 'procedure_type': proc_types[nas_proc_index], 'message_sequence': str(procs[nas_proc_index]), 'message_direction': str(procs_dir[nas_proc_index]), 'message_time': str(procs_time[nas_proc_index]), 'status': proc_stats[nas_proc_index]})
            procs.pop(nas_proc_index)
            procs_dir.pop(nas_proc_index)
            procs_time.pop(nas_proc_index)
            proc_types.pop(nas_proc_index)
            proc_stats.pop(nas_proc_index)

        if nas_proc_type not in proc_types:
            procs.append([nas_msg_emm_type_id])
            procs_dir.append([nas_msg_direction])
            procs_time.append([nas_msg_time])
            proc_types.append(nas_proc_type)
            if nas_msg_emm_type_id in PROCS_REQ:
                proc_stats.append(1)
            else:
                proc_stats.append(3)
        else:
            nas_proc_index = proc_types.index(nas_proc_type)
            if nas_msg_emm_type_id in procs[nas_proc_index]:
                print 'ERROR in get_nas_procedure_and_insert_to_db: at time %s, nas_msg_emm_type_id %d has already been in procs[%d]-->%s' % (str(procs_time[nas_proc_index]), nas_msg_emm_type_id, nas_proc_index, str(procs[nas_proc_index]))
            else:
                procs[nas_proc_index].append(nas_msg_emm_type_id)
                procs_dir[nas_proc_index].append(nas_msg_direction)
                procs_time[nas_proc_index].append(nas_msg_time)

        stop_pre_proc = False
        nas_proc_index = -1
        if nas_proc_type in [7, 8, 9, 10, 11, 12] and 6 in proc_types:
            nas_proc_index = proc_types.index(6)
            stop_pre_proc = True
        elif nas_proc_type in [6, 7, 9, 10, 11, 12] and 8 in proc_types:
            nas_proc_index = proc_types.index(8)
            stop_pre_proc = True
        elif nas_proc_type in [6, 7, 8, 10, 11, 12] and 9 in proc_types:
            nas_proc_index = proc_types.index(9)
            stop_pre_proc = True
        elif nas_proc_type in [6, 7, 8, 9, 11, 12] and 10 in proc_types:
            nas_proc_index = proc_types.index(10)
            stop_pre_proc = True
        if stop_pre_proc == True:
            if procs[nas_proc_index][len(procs[nas_proc_index]) - 1] not in PROCS_END_WAIT:
                proc_stats[nas_proc_index] = 2
                print 'Abnormal_1 ... (undone)'
            if proc_stats[nas_proc_index] in [-1, 1] and\
                check_nas_procedure_status(proc_types[nas_proc_index], procs[nas_proc_index], proc_stats[nas_proc_index]) == False:
                proc_stats[nas_proc_index] = 2
                print 'check_nas_procedure_status_1 not complete... (undone)'
            db.insert_to_event_table('nas_procedures', {'user_id': user_id, 'start_time': procs_time[nas_proc_index][0], 'end_time': procs_time[nas_proc_index][len(procs_time[nas_proc_index]) - 1], 'procedure_type': proc_types[nas_proc_index], 'message_sequence': str(procs[nas_proc_index]), 'message_direction': str(procs_dir[nas_proc_index]), 'message_time': str(procs_time[nas_proc_index]), 'status': proc_stats[nas_proc_index]})
            procs.pop(nas_proc_index)
            procs_dir.pop(nas_proc_index)
            procs_time.pop(nas_proc_index)
            proc_types.pop(nas_proc_index)
            proc_stats.pop(nas_proc_index)

        if nas_msg_emm_type_id in PROCS_END_SUCCEED + PROCS_END_FAIL:
            nas_proc_index = proc_types.index(nas_proc_type)
            if proc_stats[nas_proc_index] in [1] and nas_msg_emm_type_id in PROCS_END_FAIL:
                proc_stats[nas_proc_index] = -1
            if proc_stats[nas_proc_index] in [-1, 1] and\
                check_nas_procedure_status(proc_types[nas_proc_index], procs[nas_proc_index], proc_stats[nas_proc_index]) == False:
                proc_stats[nas_proc_index] = 2
                print 'check_nas_procedure_status not complete_2... (undone)'
            db.insert_to_event_table('nas_procedures', {'user_id': user_id, 'start_time': procs_time[nas_proc_index][0], 'end_time': procs_time[nas_proc_index][len(procs_time[nas_proc_index]) - 1], 'procedure_type': proc_types[nas_proc_index], 'message_sequence': str(procs[nas_proc_index]), 'message_direction': str(procs_dir[nas_proc_index]), 'message_time': str(procs_time[nas_proc_index]), 'status': proc_stats[nas_proc_index]})
            procs.pop(nas_proc_index)
            procs_dir.pop(nas_proc_index)
            procs_time.pop(nas_proc_index)
            proc_types.pop(nas_proc_index)
            proc_stats.pop(nas_proc_index)

def check_nas_procedure_status(nas_proc_type, proc, proc_stat):
    # Check status
    if nas_proc_type == 2:
        # Authentication procedure
        return proc == [12, 13]
    elif nas_proc_type == 3:
        # Security mode control procedure
        return proc == [14, 15]
    elif nas_proc_type == 4:
        # Identity procedure
        return proc == [6, 7]
    elif nas_proc_type == 5:
        # EMM information procedure
        return proc == [10]
    elif nas_proc_type == 6:
        # Attach procedure
        return proc == [5, 8, 9] or proc == [5, 21]
    elif nas_proc_type == 7:
        # Detach procedure
        return proc == [3] or proc == [3, 4]
    elif nas_proc_type == 8:
        # Tracking area updating procedure
        return proc == [1, 2] or proc == [1, 2, 16] or proc == [1, 20]
    elif nas_proc_type == 9:
        # Service request procedure
        return proc == [25]
    elif nas_proc_type == 10:
        # Extended service request procedure
        return proc == [11]
    elif nas_proc_type == 12:
        # Transport of NAS messages procedure
        return proc == [17] or proc == [18]
    else:
        return True

def get_nas_procedure_with_rrc_and_insert_to_db(db, user_id, start_time, end_time):
    sql = ''' select start_time, end_time, status, procedure_type 
        from nas_procedures 
        where user_id = %d and start_time >= %d and
        end_time <= %d and (procedure_type = 9 or 
        procedure_type = 10 or procedure_type = 11)
        order by start_time''' % (user_id, start_time, end_time)
    nas_rows = db.select_multiple_row(sql)
    for nas_row in nas_rows:
        _start_time = nas_row[0]
        _end_time = nas_row[1]
        _status = nas_row[2]
        if nas_row[3] in [9, 10]:
            # Service request procedure or
            # Extended service request procedure
            sql = ''' select time, message_type_id from messages
                where user_id = %d and time >= %d and
                (message_type_id = 18 or message_type_id = 19 or 
                message_type_id = 20 or message_type_id = 35) 
                order by time limit 10''' % (user_id, _start_time)
            msg_rows = db.select_multiple_row(sql)
            rrc_step = 0
            for msg_row in msg_rows:
                if rrc_step == 0:
                    if msg_row[1] == 18:
                        rrc_step = 1
                    else:
                        print 'Abnormal in get_nas_procedure_with_rrc: after (ext) service request %d, no rrcConnectionRequest immediately' % (nas_row[3])
                        _status = 2
                        break
                    _end_time = msg_row[0]
                elif rrc_step == 1:
                    if msg_row[1] == 19:
                        rrc_step = 2
                    else:
                        print 'Abnormal in get_nas_procedure_with_rrc: after (ext) service request %d, no rrcConnectionSetup immediately' % (nas_row[3])
                        _status = 2
                        break
                    _end_time = msg_row[0]
                elif rrc_step == 2:
                    if msg_row[1] == 20:
                        rrc_step = 3
                    else:
                        print 'Abnormal in get_nas_procedure_with_rrc: after (ext) service request %d, no rrcConnectionSetupComplete immediately' % (nas_row[3])
                        _status = 2
                        break
                    _end_time = msg_row[0]
                elif rrc_step == 3:
                    if msg_row[1] == 35:
                        rrc_step = 4
                    else:
                        print 'Abnormal in get_nas_procedure_with_rrc: after (ext) service request %d, no rrcConnectionRelease immediately' % (nas_row[3])
                        _status = 2
                        break
                    _end_time = msg_row[0]
                else:
                    break
        db.update_to_event_table('nas_procedures', {'end_time': _end_time, 'status': _status}, {'user_id': user_id, 'start_time': _start_time})

def get_nas_procedure_energy_and_insert_to_db(db, user_id, start_time, end_time, power_model):
    sql = ''' select start_time, end_time, procedure_type, status
        from nas_procedures
        where user_id = %d and start_time >= %d and end_time <= %d
        order by start_time''' % (user_id, start_time, end_time)
    rows = db.select_multiple_row(sql)

    for row in rows:
        modem_energy_dict = energy.get_modem_energy(db, power_model, user_id, row[0], row[1])
        modem_energy = 0.0
        for pm_procedure_type, value in modem_energy_dict.iteritems():
            modem_energy += value
        #db.insert_to_event_table('nas_procedure_energy', {'user_id': user_id, 'start_time': row[0], 'end_time': row[1], 'procedure_type': row[2], 'status': row[3], 'modem_energy': modem_energy, 'modem_energy_dict': str(modem_energy_dict)})
        db.replace_to_event_table('nas_procedure_energy', {'user_id': user_id, 'start_time': row[0], 'end_time': row[1], 'procedure_type': row[2], 'status': row[3], 'modem_energy': modem_energy, 'modem_energy_dict': str(modem_energy_dict)})

def get_rrc_failure_and_insert_to_db(db, user_id, start_time, end_time):
    insert_sql = ''' insert ignore into `rrc_failure` (`user_id`, `start_time`, `end_time`, `message_sequence`, `signal_strength`, `velocity`) values '''
    msg_rows = db.select_multiple_row(''' select time, message_type_id from messages where user_id = %d 
        and time >= %d and time <= %d
        and message_type_id in (18,19,20,21,35)
        order by time''' % (user_id, start_time, end_time))
        #and message_type_id >= 18 and message_type_id <= 21
    pre_time = 0
    rrc_state = 0
    for msg_row in msg_rows:
        time = msg_row[0]
        msg_id = msg_row[1]
        if rrc_state == 0:
            if msg_id == 18:
                rrc_state = 1
                pre_time = time
        elif rrc_state == 1:
            if msg_id == 19:
                rrc_state = 2
            elif msg_id in (20, 21, 35):
                rrc_state = 0
            else:
                rrc_state = -1
        elif rrc_state == 2:
            if msg_id in (20, 21, 35):
                rrc_state = 0
            else:
                rrc_state = -2
        elif rrc_state < 0:
            if msg_id in (20, 21, 35):
                rrc_state = 0
                rows_sub = db.select_multiple_row(
                    ''' select message_type_id, signal_strength, trust, emm_state_id, emm_substate_id, extra from messages 
                        where user_id = %d and time >= %d and time <= %d order by time
                    ''' % (user_id, pre_time, time))
                msg_seq = ','.join(str(x[0]) for x in rows_sub)
                ss = rows_sub[0][1]
                v = ua.get_velocity(db, user_id, pre_time, time)
                db.execute(insert_sql + '''(%d,%d,%d,'%s',%f,%f)''' % (user_id, pre_time, time, msg_seq, ss, v))
                #print insert_sql + '''(%d,%d,%d,'%s',%f,%f)''' % (user_id, pre_time, time, msg_seq, ss, v)

def insert_to_rrc_sections(db, user_id, start_time, end_time, insert_sql):
    rows_sub = db.select_multiple_row(
        ''' select message_type_id, signal_strength, trust, emm_state_id, emm_substate_id, extra from messages 
            where user_id = %d and time >= %d and time < %d order by time
        ''' % (user_id, start_time - 10 * 1000000, start_time))
    msg_seq_before = ','.join(str(x[0]) for x in rows_sub)

    rows_sub = db.select_multiple_row(
        ''' select message_type_id, signal_strength, trust, emm_state_id, emm_substate_id, extra from messages 
            where user_id = %d and time >= %d and time <= %d order by time
        ''' % (user_id, start_time, end_time))
    msg_seq = ','.join(str(x[0]) for x in rows_sub)
    ss = rows_sub[0][1]
    v = ua.get_velocity(db, user_id, start_time, end_time)
    db.execute(insert_sql + '''(%d,%d,%d,'%s','%s',%f,%f)''' % (user_id, start_time, end_time, msg_seq_before, msg_seq, ss, v))
    #print insert_sql + '''(%d,%d,%d,'%s','%s',%f,%f)''' % (user_id, start_time, end_time, msg_seq_before, msg_seq, ss, v)

def get_rrc_success_and_insert_to_db(db, user_id, start_time, end_time):
    insert_sql = ''' insert ignore into `rrc_success` (`user_id`, `start_time`, `end_time`, `message_sequence_before`
        ,`message_sequence`, `signal_strength`, `velocity`) values '''
    msg_rows = db.select_multiple_row(''' select time, message_type_id from messages where user_id = %d 
        and time >= %d and time <= %d
        and message_type_id in (18,19,20,21,35)
        order by time''' % (user_id, start_time, end_time))
        #and message_type_id >= 18 and message_type_id <= 21
    pre_time = 0
    rrc_state = 0
    for msg_row in msg_rows:
        time = msg_row[0]
        msg_id = msg_row[1]
        if rrc_state == 0:
            if msg_id == 18:
                rrc_state = 1
                pre_time = time
        elif rrc_state == 1:
            if msg_id == 19:
                rrc_state = 2
            elif msg_id in (20, 21, 35):
                rrc_state = 0
                insert_to_rrc_sections(db, user_id, pre_time, time, insert_sql)
            else:
                rrc_state = -1
        elif rrc_state == 2:
            if msg_id in (20, 21, 35):
                rrc_state = 0
                insert_to_rrc_sections(db, user_id, pre_time, time, insert_sql)
            else:
                rrc_state = -2
        elif rrc_state < 0:
            if msg_id in (20, 21, 35):
                rrc_state = 0

def get_rrc_failure_sections_and_insert_to_db(db, user_id, start_time, end_time):
    insert_sql = ''' insert ignore into `rrc_failure_sections` (`user_id`, `start_time`, `end_time`, `message_sequence_before`
        ,`message_sequence`, `signal_strength`, `velocity`) values '''
    msg_rows = db.select_multiple_row(''' select time, message_type_id from messages where user_id = %d 
        and time >= %d and time <= %d
        and message_type_id in (18,19,20,21,35)
        order by time''' % (user_id, start_time, end_time))
        #and message_type_id >= 18 and message_type_id <= 21
    pre_time = 0
    rrc_state = 0
    for msg_row in msg_rows:
        time = msg_row[0]
        msg_id = msg_row[1]
        if rrc_state == 0:
            if msg_id == 18:
                rrc_state = 1
                pre_time = time
        elif rrc_state == 1:
            if msg_id == 19:
                rrc_state = 2
            elif msg_id in (20, 21, 35):
                rrc_state = 0
            else:
                rrc_state = -1
        elif rrc_state == 2:
            if msg_id in (20, 21, 35):
                rrc_state = 0
            else:
                rrc_state = -2
        if rrc_state < 0:
            if msg_id == 18:
                insert_to_rrc_sections(db, user_id, pre_time, time, insert_sql)
                rrc_state = 1
                pre_time = time
            #elif msg_id in (20, 21, 35):
            #    rrc_state = 0

def update_last_msib_for_rrc_success_failure(db, user_id, start_time, end_time):
    succ_rows = db.select_multiple_row(''' select start_time, end_time, message_sequence_before, message_sequence
        from rrc_success where user_id = %d
        and start_time >= %d and end_time <= %d
        order by start_time''' % (user_id, start_time, end_time))
    for succ_row in succ_rows:
        _start_time = succ_row[0]
        _end_time = succ_row[1]
        msg_seq_bf_txt = succ_row[2]
        msg_seq_txt = succ_row[3]
        msg_seq_bf = map(int, filter(None, msg_seq_bf_txt.split(',')))
        msg_seq = map(int, filter(None, msg_seq_txt.split(',')))
        msib_rows = db.select_multiple_row(''' select max(time) from messages where user_id = %d and time >= %d and time <= %d
            and message_type_id in (12, 16, 17)
            ''' % (user_id, start_time, _start_time))
        last_msib = -1
        if msib_rows[0][0] != None:
            last_msib = msib_rows[0][0]
        #print ''' update rrc_success set extra = concat(extra, ',last_msib=%d')
        #    where user_id = %d and start_time = %d
        #    and extra not like '%%,last_msib=%d%%'
        #    ''' % (last_msib, user_id, _start_time, last_msib)
        db.execute(''' update rrc_success set extra = concat(ifnull(extra, ''), ',last_msib=%d')
            where user_id = %d and start_time = %d
            and extra not like '%%,last_msib=%d%%'
            ''' % (last_msib, user_id, _start_time, last_msib))

    fail_rows = db.select_multiple_row(''' select start_time, end_time, message_sequence_before, message_sequence
        from rrc_failure_sections where user_id = %d
        and start_time >= %d and end_time <= %d
        order by start_time''' % (user_id, start_time, end_time))
    for fail_row in fail_rows:
        _start_time = fail_row[0]
        _end_time = fail_row[1]
        msg_seq_bf_txt = fail_row[2]
        msg_seq_txt = fail_row[3]
        msg_seq_bf = map(int, msg_seq_bf_txt.split(','))
        msg_seq = map(int, msg_seq_txt.split(','))
        msib_rows = db.select_multiple_row(''' select max(time) from messages where user_id = %d and time >= %d and time <= %d
            and message_type_id in (12, 16, 17)
            ''' % (user_id, start_time, _start_time))
        last_msib = -1
        if msib_rows[0][0] != None:
            last_msib = msib_rows[0][0]
        #print ''' update rrc_failure_sections set extra = concat(extra, ',last_msib=%d')
        #    where user_id = %d and start_time = %d
        #    and extra not like '%%,last_msib=%d%%'
        #    ''' % (last_msib, user_id, _start_time, last_msib)
        db.execute(''' update rrc_failure_sections set extra = concat(ifnull(extra, ''), ',last_msib=%d')
            where user_id = %d and start_time = %d
            and extra not like '%%,last_msib=%d%%'
            ''' % (last_msib, user_id, _start_time, last_msib))

def update_next_msib_for_rrc_success_failure(db, user_id, start_time, end_time):
    succ_rows = db.select_multiple_row(''' select start_time, end_time, message_sequence_before, message_sequence
        from rrc_success where user_id = %d
        and start_time >= %d and end_time <= %d
        order by start_time''' % (user_id, start_time, end_time))
    for succ_row in succ_rows:
        _start_time = succ_row[0]
        _end_time = succ_row[1]
        msg_seq_bf_txt = succ_row[2]
        msg_seq_txt = succ_row[3]
        msg_seq_bf = map(int, filter(None, msg_seq_bf_txt.split(',')))
        msg_seq = map(int, filter(None, msg_seq_txt.split(',')))
        msib_rows = db.select_multiple_row(''' select min(time) from messages where user_id = %d and time >= %d and time <= %d
            and message_type_id in (12, 16, 17)
            ''' % (user_id, _start_time, end_time))
        next_msib = -1
        if msib_rows[0][0] != None:
            next_msib = msib_rows[0][0]
        #print ''' update rrc_success set extra = concat(extra, ',next_msib=%d')
        #    where user_id = %d and start_time = %d
        #    and extra not like '%%,next_msib=%d%%'
        #    ''' % (next_msib, user_id, _start_time, next_msib)
        db.execute(''' update rrc_success set extra = concat(ifnull(extra, ''), ',next_msib=%d')
            where user_id = %d and start_time = %d
            and extra not like '%%,next_msib=%d%%'
            ''' % (next_msib, user_id, _start_time, next_msib))

    fail_rows = db.select_multiple_row(''' select start_time, end_time, message_sequence_before, message_sequence
        from rrc_failure_sections where user_id = %d
        and start_time >= %d and end_time <= %d
        order by start_time''' % (user_id, start_time, end_time))
    for fail_row in fail_rows:
        _start_time = fail_row[0]
        _end_time = fail_row[1]
        msg_seq_bf_txt = fail_row[2]
        msg_seq_txt = fail_row[3]
        msg_seq_bf = map(int, msg_seq_bf_txt.split(','))
        msg_seq = map(int, msg_seq_txt.split(','))
        msib_rows = db.select_multiple_row(''' select min(time) from messages where user_id = %d and time >= %d and time <= %d
            and message_type_id in (12, 16, 17)
            ''' % (user_id, _start_time, end_time))
        next_msib = -1
        if msib_rows[0][0] != None:
            next_msib = msib_rows[0][0]
        #print ''' update rrc_failure_sections set extra = concat(extra, ',next_msib=%d')
        #    where user_id = %d and start_time = %d
        #    and extra not like '%%,next_msib=%d%%'
        #    ''' % (next_msib, user_id, _start_time, next_msib)
        db.execute(''' update rrc_failure_sections set extra = concat(ifnull(extra, ''), ',next_msib=%d')
            where user_id = %d and start_time = %d
            and extra not like '%%,next_msib=%d%%'
            ''' % (next_msib, user_id, _start_time, next_msib))

