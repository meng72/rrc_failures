import os
import sys
import re
import zipfile
from os import listdir
from os.path import isfile, join
from shutil import copyfile
import ConfigParser
from database import Database
import utils
import utils_analysis as utils_a
#import correlation_analysis as ca
from power_model import PowerModel
import energy
import refine_data as rd

Config = ConfigParser.ConfigParser()
Config.read("config.ini")
DB_USER = Config.get('Database', 'user')
DB_PASSWORD = Config.get('Database', 'password')
DB_HOST = Config.get('Database', 'host')
DB_UNIX_SOCKET = Config.get('Database', 'unix_socket')
UNZIP_FOLDER = '/home/meng/modem/tmp/'

def main(demo_index):
    db = Database(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, unix_socket=DB_UNIX_SOCKET)
    power_model = PowerModel()

    if demo_index == 1:
        print '====== %d: Decode one binary mi2log file into an xml file' % (demo_index)
        print 'Command:$python mi_decode/decoder.py %s %s' % ('demo_mi2log.mi2log', 'demo_mi2log.xml')
        os.system('python mi_decode/decoder.py %s %s ' % ('demo_mi2log.mi2log', 'demo_mi2log.xml'))

    if demo_index == 2:
        print '====== %d: Init database' % (demo_index)
        print 'Code: db.init_database()'
        #db.init_database()

    if demo_index == 3:
        print '====== %d: Show tables in database' % (demo_index)
        table_rows = db.select_multiple_row('show tables')
        event_tables = []
        for table_row in table_rows:
            table_name = table_row[0].encode('utf-8')
            event_tables.append(table_name)
        print event_tables

    if demo_index == 4:
        print '====== %d: Parse files and put data into database' % (demo_index)
        print 'Command:$python parse_upload_decoded.py'
        os.system('python parse_upload_decoded.py')
        
    user_id = 2
    start_time = 1494504819195796
    end_time = 1494504880334096

    if demo_index == 5:
        print '====== %d: Get messages and insert to database' % (demo_index)
        # get messages
        #[user_id, start_time, end_time]= [80, 1516227230647609, 1516229273603147]
        [user_id, start_time, end_time]= [80, 1516229905482000, 1516230758965000]
        #rd.get_messages_and_insert_to_db(db, user_id, start_time, end_time)
        rd.get_messages_and_insert_to_db(db, user_id, start_time, end_time)
        #rd.get_messages_and_insert_to_db_fix_bug2(db, user_id, start_time, end_time)
        # update pre_cell_id via MIB packet
        #rd.get_pre_cell_id_and_update_db(db, user_id, start_time, end_time)
        # get rrc state for each message
        #rd.get_rrc_state_and_update_db(db, user_id, start_time, end_time)

    if demo_index == 6:
        print '====== %d: Get signal strength for each message and insert to database' % (demo_index)
        rd.get_serv_cell_id_and_update_db(db, user_id, start_time, end_time)
        rd.get_rrc_serv_cell_info_and_update_db(db, user_id, start_time, end_time)
        rd.get_rrc_ota_packet_freq_and_update_db(db, user_id, start_time, end_time)
        rd.get_signal_strength_and_update_db(db, user_id, start_time, end_time)
    
    if demo_index == 7:
        print '====== %d: Get procedures and insert to database' % (demo_index)
        # get procedures
        #     RLC_DL: rcv_bytes
        #     RLC_UL: snd_bytes
        rd.get_procedure_and_insert_to_db(dn, user_id, start_time, end_time)
        # get e7 events
        rd.get_register_and_insert_to_db(db, user_id, start_time, end_time)

    if demo_index == 8:
        print '====== %d: Update procedures for MAC and RRC layers and insert to database' % (demo_index)
        # update active procedures active_data/active_nodata
        #     For MAC_DL: rnti_types
        #     For MAC_UL: bsr_events
        #     For MIB, LTE_RRC_OTA_Packet: rrc_msg_len, log_msg_len
        rd.update_active_procedure(db, user_id, start_time, end_time)
        print '\n\n====== %d: get signal strength for each procedure' % (demo_index)
        # get signal strength for each procedure
        rd.get_signal_strength_for_procedure_and_update_db(db, user_id, start_time, end_time)

    if demo_index == 9:
        print '====== %d: before calculating procedure energy, get corresponding context and insert to database' % (demo_index)
        # filter_type: 5 & 6, refine_data.py by mi_log
        #   5: rrc_idle_periods; 6: rrc_conn_periods
        rd.get_interval_periods_and_insert_to_db(db, user_id, 5, start_time, end_time)

        rd.get_signal_periods_and_insert_to_db(db, user_id, start_time, end_time)
        rd.get_velocity_periods_refined_and_insert_to_db(db, user_id, start_time, end_time)
        rd.get_tod_periods_and_insert_to_db(db, user_id, start_time, end_time)

        rd.get_rrc_signal_periods(db, user_id, start_time, end_time)
        rd.get_rrc_velocity_periods(db, user_id, start_time, end_time)
        rd.get_rrc_tod_periods(db, user_id, start_time, end_time)

        rd.get_correlate_mib_location_and_insert_to_db(db, user_id, start_time, end_time)
        rd.get_location_periods_and_insert_to_db(db, user_id, _start, _end)
        rd.get_rrc_location_periods(db, user_id, _start, _end)
        
    if demo_index == 10:
        print '====== %d: get procedure energy and corresponding context and insert to database' % (demo_index)
        rd.get_procedures_energy_and_insert_to_db(db, power_model, user_id, start_time, end_time)
        rd.get_procedures_energy_info_and_insert_to_db(db, power_model, user_id, _start, _end)




    print '\n\n'

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'Please input index of demo to run'
        exit(-1)
    main(int(sys.argv[1]))
