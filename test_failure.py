from database import Database
from utils import *
import rrc_failure as rf
from refine_data import *
from mi_parser import MiParser
from mylogger_parser import MyloggerParser

from parse_upload_decoded import *

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
    #db.init_database()

    users = [2, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16]

    other = {}

    # 1. print all rrc failure cases for users
    rf.get_rrc_failure_for_users(db, users)
    # 1-1. get normal pattern of rrc success
    # except (1) 42,36
    #rf.get_rrc_success_for_users(db, users)
    # 1-2. draw for rrc success and failure
    #other['filename'] = 'figs/1_2_num_rrc_emm_for_users'
    #rf.draw_for_rrc_for_users(db, users, 'emm', 'num_rrc', other)
    #other['filename'] = 'figs/1_2_num_rrc_emm_mobility_for_users'
    #rf.draw_for_rrc_for_users(db, users, 'emm', 'num_rrc', other)
    #other['filename'] = 'figs/1_2_num_rrc_emm_non_mib_sib_for_users'
    #rf.draw_for_rrc_for_users(db, users, 'emm', 'num_rrc', other)
    #other['filename'] = 'figs/1_2_num_rrc_emm_non_mib_sib_mobility_for_users'
    #rf.draw_for_rrc_for_users(db, users, 'emm', 'num_rrc', other)
    # 1-3. draw for duration between last mib / sib and rrc conn request
    #other['filename'] = 'figs/1_3_duration_mib_sib_rrc_for_users'
    #rf.draw_cdf_for_duration_mib_sib_rrc(db, users, 'last_msib', other)
    #other['filename'] = 'figs/1_3_duration_mib_sib_rrc_mobility_for_users'
    #rf.draw_cdf_for_duration_mib_sib_rrc(db, users, 'last_msib', other)
    # 1-4. draw for duration between next mib / sib and rrc conn request
    #other['filename'] = 'figs/1_4_duration_mib_sib_rrc_for_users'
    #rf.draw_cdf_for_duration_mib_sib_rrc(db, users, 'next_msib', other)
    #other['filename'] = 'figs/1_4_duration_mib_sib_rrc_mobility_for_users'
    #rf.draw_cdf_for_duration_mib_sib_rrc(db, users, 'next_msib', other)
    # 1-5. draw for each failure duration
    #other['filename'] = 'figs/1_4_duration_rrc_failure_for_users'
    #rf.draw_cdf_for_rrc_failure_for_users(db, users, 'duration', other)
    # 1-6. draw to compare rrc success and rrc failure
    """
    other['filename'] = 'figs/1_6_duration_rrc_success_failure_for_users'
    rf.draw_cdf_to_comp_rrc_success_failure_for_users(db, users, other)
    """

    # 2. classify rrc failures
    #rf.classify_rrc_failure_for_users(db, users)

    # 3. draw for rrc failures
    # 3-1. num of rrc failures
    #other['filename'] = 'figs/3_1_num_rrc_failure_for_users'
    #rf.draw_for_rrc_failure_for_users(db, users, 'handover', 'num', other)
    # 3-2. duration of rrc failures
    #other['filename'] = 'figs/3_1_duration_rrc_failure_for_users'
    #rf.draw_for_rrc_failure_for_users(db, users, 'handover', 'duration', other)
    # 3-3. duration of rrc failures
    #other['filename'] = 'figs/3_3_percentile_duration_rrc_failure_for_users'
    #rf.draw_box_for_rrc_failure_for_users(db, users, 'handover', 'duration', other)
    # 3-4. location of rrc failures
    #other['filename'] = 'figs/3_4_map_failure_for_users'
    #rf.draw_map_for_rrc_failure_for_users(db, users, 'handover', 'num', other)
    # 3-5. location map of rrc failures
    #other['filename'] = 'figs/3_5_scattermapbox_failure_for_users'
    #rf.draw_map_for_rrc_failure_for_users(db, users, 'handover', 'num', other)

    # 4. draw for each type
    # 4-1. type 0
    #other['filename'] = 'figs/4_1_type0_msgs_for_users'
    #rf.draw_histogram_for_rrc_failure_for_users(db, users, 't0', 'msg', other)
    #other['filename'] = 'figs/4_1_type0_msgs_bf_for_users'
    #rf.draw_histogram_for_rrc_failure_for_users(db, users, 't0', 'msg_bf', other)
    #other['filename'] = 'figs/4_1_type0_num_mib_in_msg_for_users'
    #rf.draw_histogram_for_rrc_failure_for_users(db, users, 't0', 'num_mib_in_msg', other)
    #other['filename'] = 'figs/4_1_type0_num_sib_in_msg_for_users'
    #rf.draw_histogram_for_rrc_failure_for_users(db, users, 't0', 'num_sib_in_msg', other)
    #other['filename'] = 'figs/4_1_type0_last_emm_in_msg_for_users'
    #rf.draw_histogram_for_rrc_failure_for_users(db, users, 't0', 'last_emm', other)
    # 4-2. type 1
    #other['filename'] = 'figs/4_2_type1_msgs_for_users'
    #rf.draw_histogram_for_rrc_failure_for_users(db, users, 't1', 'msg', other)
    #other['filename'] = 'figs/4_1_type0_msgs_bf_for_users'
    #rf.draw_histogram_for_rrc_failure_for_users(db, users, 't0', 'msg_bf', other)
    #other['filename'] = 'figs/4_2_type1_last_emm_in_msg_for_users'
    #rf.draw_histogram_for_rrc_failure_for_users(db, users, 't1', 'last_emm', other)
    #other['filename'] = 'figs/4_2_type1_last_emm_in_msg_bf_for_users'
    #rf.draw_histogram_for_rrc_failure_for_users(db, users, 't1', 'last_emm_bf', other)
    # 4-3. type 2
    #other['filename'] = 'figs/4_3_type2_msgs_for_users'
    #rf.draw_histogram_for_rrc_failure_for_users(db, users, 't2', 'msg', other)
    #other['filename'] = 'figs/4_3_type2_last_emm_in_msg_bf_for_users'
    #rf.draw_histogram_for_rrc_failure_for_users(db, users, 't2', 'last_emm_bf', other)

if __name__ == '__main__':
    main()
