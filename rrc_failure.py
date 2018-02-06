#!/usr/local/bin/python

import re
import sys

import plotly_lib as pl

from utils import *
from utils_analysis import *

import math
import numpy

def get_rrc_failure_for_users(db, users):
    for user_id in users:
        print '----------' + str(user_id)
        fail_rows = db.select_multiple_row(''' select start_time, end_time, message_sequence from rrc_failure where user_id = %d
            order by start_time''' % (user_id))
            #and velocity > 0
        for fail_row in fail_rows:
            print fail_row

def get_rrc_success_for_users(db, users):
    for user_id in users:
        print '----------' + str(user_id)
        succ_rows = db.select_multiple_row(''' select * from rrc_success where user_id = %d
            and message_sequence_before like '%%42,36'
            order by start_time''' % (user_id))
            #and velocity > 0
        for succ_row in succ_rows:
            print succ_row

def draw_for_rrc_for_users(db, users, metric, context, other):
    data = {}
    data['bar'] = {}
    data['bar']['x'] = []
    data['bar']['y'] = []

    metric_len = 0
    if 'emm' == metric:
        emm_rows = db.select_multiple_row(''' select count(*) from message_types where message_type = 'LTE_NAS_EMM_State' order by id''')
        metric_len = emm_rows[0][0]
    elif 'mib_sib' == metric:
        metric_len = 1

    context_len = 0
    if 'num_rrc' in context:
        context_len = 2
    elif 'duration_msib_rrc' in context:
        context_len = 2

    for i in range(0, context_len):
        data['bar']['x'].append([])
        data['bar']['y'].append([])
        for j in range(0, metric_len):
            data['bar']['x'][i].append(i + j * (context_len + 1) + 1)
            data['bar']['y'][i].append(0)

    x_tickvals = [] # start from 1
    x_ticktext = []
    for i in range(0, metric_len):
        if context_len == 1:
            x_tickvals.append(i * (context_len + 1) + 1)
        else:
            x_tickvals.append(i * (context_len + 1) + (context_len + 1) / 2.0)
        x_ticktext.append(i)

    if 'emm' == metric:
        x_ticktext = []
        emm_rows = db.select_multiple_row(''' select id from message_types where message_type = 'LTE_NAS_EMM_State' order by id''')
        for i in range(0, metric_len):
            x_ticktext.append(emm_rows[i][0])

    data_bar = []
    divisor = []
    for i in range(0, context_len):
        data_bar.append([])
        divisor.append([])
        for j in range(0, metric_len):
            data_bar[i].append(0)
            divisor[i].append(0)

    for user_id in users:
        succ_rows = db.select_multiple_row(''' select start_time, end_time, message_sequence_before, message_sequence
            from rrc_success where user_id = %d
            order by start_time''' % (user_id))
            #and velocity > 0.0
        for succ_row in succ_rows:
            _start_time = succ_row[0]
            _end_time = succ_row[1]
            msg_seq_bf_txt = succ_row[2]
            msg_seq_txt = succ_row[3]
            msg_seq_bf = map(int, filter(None, msg_seq_bf_txt.split(',')))
            msg_seq = map(int, filter(None, msg_seq_txt.split(',')))
            if 'emm' == metric:
                emm_state = -1
                for i in range(len(msg_seq_bf) - 1, -1, -1):
                    if msg_seq_bf[i] in x_ticktext:
                        emm_state = x_ticktext.index(msg_seq_bf[i])
                        break
                if emm_state >= 0:
                    data_bar[0][emm_state] += 1
                    divisor[0][emm_state] = 1

        fail_rows = db.select_multiple_row(''' select start_time, end_time, message_sequence_before, message_sequence
            from rrc_failure_sections where user_id = %d
            order by start_time''' % (user_id))
            #and velocity > 0.0
        for fail_row in fail_rows:
            _start_time = fail_row[0]
            _end_time = fail_row[1]
            msg_seq_bf_txt = fail_row[2]
            msg_seq_txt = fail_row[3]
            msg_seq_bf = map(int, msg_seq_bf_txt.split(','))
            msg_seq = map(int, msg_seq_txt.split(','))
            if 12 in msg_seq_bf \
                or 16 in msg_seq_bf \
                or 17 in msg_seq_bf:
                break
            if 'emm' == metric:
                emm_state = -1
                for i in range(len(msg_seq_bf) - 1, -1, -1):
                    if msg_seq_bf[i] in x_ticktext:
                        emm_state = x_ticktext.index(msg_seq_bf[i])
                        break
                if emm_state >= 0:
                    data_bar[1][emm_state] += 1
                    divisor[1][emm_state] = 1

    print '-------------------------'
    print data_bar
    print divisor

    for i in range(0, context_len):
        for j in range(0, metric_len):
            d = divisor[i][j]
            if d > 0:
                data['bar']['y'][i][j] = 1.0 * data_bar[i][j] / d
            else:
                data['bar']['y'][i][j] = 0.0

    print '-------------------------'
    print x_ticktext
    print data['bar']['x']
    print data['bar']['y']

    if 'num_rrc' == context:
        other['bar_colors'] = ['orange', 'red']
        other['bar_names'] = ['Success', 'Failure']
        other['y_title'] = 'Num of occurrence'
        other['y_dtick'] = 50
    elif 'duration' == context:
        other['bar_colors'] = ['orange']
        other['bar_names'] = ['']
        other['y_title'] = 'Avg. duration of each failure'
        other['y_dtick'] = 20

    if 'emm' == metric:
        other['x_title'] = 'EMM State'
        other['bar_mode'] = 'stack'
        other['x_tickvals'] = x_tickvals
        other['x_ticktext'] = x_ticktext

    other['y_zeroline'] = False
    other['margin_r'] = 140
    other['margin_b'] = 80

    other['legend_orientation'] = 'h'
    pl.draw_bar_and_scatter(data, other)

def draw_cdf_for_duration_mib_sib_rrc(db, users, metric, other):
    interval = [[], []]
    for user_id in users:
        succ_rows = db.select_multiple_row(''' select start_time, end_time, message_sequence_before, message_sequence, extra
            from rrc_success where user_id = %d
            order by start_time''' % (user_id))
            #and velocity > 0.0
        for succ_row in succ_rows:
            _start_time = succ_row[0]
            _end_time = succ_row[1]
            msg_seq_bf_txt = succ_row[2]
            msg_seq_txt = succ_row[3]
            extra = succ_row[4]
            extra_dict = parse_extra(extra)
            msg_seq_bf = map(int, filter(None, msg_seq_bf_txt.split(',')))
            msg_seq = map(int, filter(None, msg_seq_txt.split(',')))
            last_msib = int(extra_dict[metric])
            if last_msib == -1:
            #if len(msib_rows) == 0 or len(msib_rows[0]) == 0:
                duration = -1
            else:
                duration = (_start_time - last_msib) / 60.0 / 1000000.0
            interval[0].append(duration)

        fail_rows = db.select_multiple_row(''' select start_time, end_time, message_sequence_before, message_sequence, extra
            from rrc_failure_sections where user_id = %d
            order by start_time''' % (user_id))
            #and velocity > 0.0
        for fail_row in fail_rows:
            _start_time = fail_row[0]
            _end_time = fail_row[1]
            msg_seq_bf_txt = fail_row[2]
            msg_seq_txt = fail_row[3]
            extra = fail_row[4]
            extra_dict = parse_extra(extra)
            msg_seq_bf = map(int, msg_seq_bf_txt.split(','))
            msg_seq = map(int, msg_seq_txt.split(','))
            last_msib = int(extra_dict[metric])
            if last_msib == -1:
                duration = -1
            else:
                duration = (_start_time - last_msib) / 60.0 / 1000000.0
            interval[1].append(duration)
    
    max_interval_0 = max(interval[0])
    for i in range(0, len(interval[0])):
        if interval[0][i] < 0:
            interval[0][i] = max_interval_0
    max_interval_1 = max(interval[1])
    for i in range(0, len(interval[1])):
        if interval[1][i] < 0:
            interval[1][i] = max_interval_0

    cdf_data = []
    for i in range(0, len(interval)):
        cdf_data.append(get_cdf_data(interval[i]))
    other['colors'] = ['orange', 'red']
    other['names'] = ['Success', 'Failure']
    other['modes'] = ['lines', 'lines']
    other['xaxises'] = ['x', 'x']
    other['yaxises'] = ['y', 'y']
    other['x_dtick'] = 1
    if metric == 'last_msib':
        other['x_title'] = 'Duration between last handover and RRC request (min)'
    elif metric == 'next_msib':
        other['x_title'] = 'Duration between next handover and RRC request (min)'
    other['y_dtick'] = 20
    other['y_title'] = 'CDF (%)'
    other['legend_orientation'] = 'h'

    pl.draw_scatter(cdf_data, other)

def draw_cdf_for_rrc_failure_for_users(db, users, metric, other):
    interval = []
    metric_len = 0
    metric_len = 1
    #metric_len = 2

    for i in range(0, metric_len):
        interval.append([])

    non_m42_list = []
    emm_types = []
    emm_rows = db.select_multiple_row(''' select id from message_types where message_type = 'LTE_NAS_EMM_State' order by id''')
    for i in range(0, len(emm_rows)):
        emm_types.append(emm_rows[i][0])

    for user_id in users:
        fail_rows = db.select_multiple_row(''' select start_time, end_time, message_sequence_before, message_sequence, extra
            , signal_strength, velocity, num_cell from rrc_failure_sections
            where user_id = %d
            order by start_time''' % (user_id))
            #and velocity > 0
        for fail_row in fail_rows:
            _start_time = fail_row[0]
            _end_time = fail_row[1]
            msg_seq_bf_txt = fail_row[2]
            msg_seq_txt = fail_row[3]
            extra = fail_row[4]
            ss = fail_row[5]
            v = fail_row[6]
            num_cell = fail_row[7]
            extra_dict = parse_extra(extra)
            msg_seq_bf = map(int, msg_seq_bf_txt.split(','))
            msg_seq = map(int, msg_seq_txt.split(','))
            fail_type = -1
            if int(extra_dict['next_msib']) >= _start_time\
                and int(extra_dict['next_msib']) <= _end_time:
                fail_type = 0
            else:
                emm_msg = -1
                for m_i in range(len(msg_seq_bf) - 1, -1, -1):
                    if msg_seq_bf[m_i] in emm_types:
                        emm_msg = msg_seq_bf[m_i]
                        break
                if emm_msg in [41, 42]:
                    fail_type = 1
                else:
                    fail_type = 1
                    if emm_msg not in non_m42_list:
                        non_m42_list.append(emm_msg)
            # dont classify failures
            fail_type = 0
            interval[fail_type].append((_end_time - _start_time) / 1000000.0)

    cdf_data = []
    for i in range(0, len(interval)):
        cdf_data.append(get_cdf_data(interval[i]))
    other['colors'] = ['orange', 'red']
    other['names'] = ['1', '2']
    other['modes'] = ['lines', 'lines']
    other['xaxises'] = ['x', 'x']
    other['yaxises'] = ['y', 'y']
    other['x_dtick'] = 1
    other['x_title'] = 'Duration (s)'
    other['y_dtick'] = 20
    other['y_title'] = 'CDF (%)'
    other['legend_orientation'] = 'h'

    pl.draw_scatter(cdf_data, other)

def draw_cdf_to_comp_rrc_success_failure_for_users(db, users, other):
    interval = []
    #emm_types = []
    metric_len = 2

    for i in range(0, metric_len):
        interval.append([])

    #emm_rows = db.select_multiple_row(''' select id from message_types where message_type = 'LTE_NAS_EMM_State' order by id''')
    #for i in range(0, len(emm_rows)):
    #    emm_types.append(emm_rows[i][0])

    for user_id in users:
        fail_rows = db.select_multiple_row(''' select start_time, end_time
            from rrc_failure_sections
            where user_id = %d
            order by start_time''' % (user_id))
            #and velocity > 0
        for fail_row in fail_rows:
            _start_time = fail_row[0]
            _end_time = fail_row[1]
            interval[0].append((_end_time - _start_time) / 1000000.0)

        succ_rows = db.select_multiple_row(''' select start_time, end_time
            from rrc_success
            where user_id = %d
            and end_time - start_time < 1000000
            order by start_time''' % (user_id))
            #and velocity > 0
        for succ_row in succ_rows:
            _start_time = succ_row[0]
            _end_time = succ_row[1]
            interval[1].append((_end_time - _start_time) / 1000000.0)

    cdf_data = []
    for i in range(0, len(interval)):
        cdf_data.append(get_cdf_data(interval[i]))
    other['colors'] = ['orange', 'red']
    other['names'] = ['fail', 'succ']
    other['modes'] = ['lines', 'lines']
    other['xaxises'] = ['x', 'x']
    other['yaxises'] = ['y', 'y']
    other['x_dtick'] = 1
    other['x_title'] = 'Duration (s)'
    other['y_dtick'] = 20
    other['y_title'] = 'CDF (%)'
    other['legend_orientation'] = 'h'

    pl.draw_scatter(cdf_data, other)

"""
Classification by handover 
0. handover right after rrc connection request
1. EMM states (normal): 41, 42
2. Other EMM states

duration, signal strength, velocity, num of cells
"""
def classify_rrc_failure_for_users(db, users):
    non_m42_list = []
    emm_types = []
    emm_rows = db.select_multiple_row(''' select id from message_types where message_type = 'LTE_NAS_EMM_State' order by id''')
    for i in range(0, len(emm_rows)):
        emm_types.append(emm_rows[i][0])

    for user_id in users:
        print '----------' + str(user_id)
        fail_rows = db.select_multiple_row(''' select start_time, end_time, message_sequence_before, message_sequence, extra
            , signal_strength, velocity, num_cell from rrc_failure_sections
            where user_id = %d
            and velocity > 0
            order by start_time''' % (user_id))
        for fail_row in fail_rows:
            _start_time = fail_row[0]
            _end_time = fail_row[1]
            msg_seq_bf_txt = fail_row[2]
            msg_seq_txt = fail_row[3]
            extra = fail_row[4]
            ss = fail_row[5]
            v = fail_row[6]
            num_cell = fail_row[7]
            extra_dict = parse_extra(extra)
            msg_seq_bf = map(int, msg_seq_bf_txt.split(','))
            msg_seq = map(int, msg_seq_txt.split(','))
            fail_type = -1
            if int(extra_dict['next_msib']) >= _start_time\
                and int(extra_dict['next_msib']) <= _end_time:
                fail_type = 0
                print (float(extra_dict['next_msib']) - _start_time) / 1000000.0
            else:
                emm_msg = -1
                for m_i in range(len(msg_seq_bf) - 1, -1, -1):
                    if msg_seq_bf[m_i] in emm_types:
                        emm_msg = msg_seq_bf[m_i]
                        break
                if emm_msg in [41, 42]:
                    fail_type = 1
                else:
                    fail_type = 1
                    if emm_msg not in non_m42_list:
                        non_m42_list.append(emm_msg)
                # what happens after next RRC Conn Req
                """
                msg_seq_after_rows = db.select_multiple_row(''' select message_type_id from messages 
                    where user_id = %d and time >= %d and time <= %d
                    ''' % (user_id, _end_time, _end_time + 5000000))
                msg_seq_after = []
                for msg_seq_after_row in msg_seq_after_rows:
                    msg_seq_after.append(msg_seq_after_row[0])
                if 19 in msg_seq_after or 20 in msg_seq_after:
                    print [30000] + msg_seq_after
                else:
                    print [10000] + msg_seq_after
                """
            print [fail_type, (_end_time - _start_time) / 1000000.0, fail_row]

def draw_for_rrc_failure_for_users(db, users, metric, context, other):
    print '=========' + other['filename']
    non_m42_list = []

    data = {}
    data['bar'] = {}
    data['bar']['x'] = []
    data['bar']['y'] = []

    emm_types = []
    emm_rows = db.select_multiple_row(''' select id from message_types where message_type = 'LTE_NAS_EMM_State' order by id''')
    for i in range(0, len(emm_rows)):
        emm_types.append(emm_rows[i][0])

    metric_len = 0
    if 'emm' == metric:
        emm_rows = db.select_multiple_row(''' select count(*) from message_types where message_type = 'LTE_NAS_EMM_State' order by id''')
        metric_len = emm_rows[0][0]
    elif 'handover' == metric:
        metric_len = 2

    context_len = 0
    if 'num' in context:
        context_len = 1
    elif 'duration' in context:
        context_len = 1

    for i in range(0, context_len):
        data['bar']['x'].append([])
        data['bar']['y'].append([])
        for j in range(0, metric_len):
            data['bar']['x'][i].append(i + j * (context_len + 1) + 1)
            data['bar']['y'][i].append(0)

    x_tickvals = [] # start from 1
    x_ticktext = []
    for i in range(0, metric_len):
        if context_len == 1:
            x_tickvals.append(i * (context_len + 1) + 1)
        else:
            x_tickvals.append(i * (context_len + 1) + (context_len + 1) / 2.0)
        x_ticktext.append(i)

    if 'emm' == metric:
        x_ticktext = []
        emm_rows = db.select_multiple_row(''' select id from message_types where message_type = 'LTE_NAS_EMM_State' order by id''')
        for i in range(0, metric_len):
            x_ticktext.append(emm_rows[i][0])

    data_bar = []
    divisor = []
    for i in range(0, context_len):
        data_bar.append([])
        divisor.append([])
        for j in range(0, metric_len):
            data_bar[i].append(0)
            divisor[i].append(0)

    for user_id in users:
        fail_rows = db.select_multiple_row(''' select start_time, end_time, message_sequence_before, message_sequence, extra
            , signal_strength, velocity, num_cell from rrc_failure_sections
            where user_id = %d
            order by start_time''' % (user_id))
            #and velocity > 0
        for fail_row in fail_rows:
            _start_time = fail_row[0]
            _end_time = fail_row[1]
            msg_seq_bf_txt = fail_row[2]
            msg_seq_txt = fail_row[3]
            extra = fail_row[4]
            ss = fail_row[5]
            v = fail_row[6]
            num_cell = fail_row[7]
            extra_dict = parse_extra(extra)
            msg_seq_bf = map(int, msg_seq_bf_txt.split(','))
            msg_seq = map(int, msg_seq_txt.split(','))
            fail_type = -1
            if int(extra_dict['next_msib']) >= _start_time\
                and int(extra_dict['next_msib']) <= _end_time:
                fail_type = 0
            else:
                emm_msg = -1
                for m_i in range(len(msg_seq_bf) - 1, -1, -1):
                    if msg_seq_bf[m_i] in emm_types:
                        emm_msg = msg_seq_bf[m_i]
                        break
                if emm_msg in [41, 42]:
                    fail_type = 1
                else:
                    fail_type = 1
                    if emm_msg not in non_m42_list:
                        non_m42_list.append(emm_msg)
            # dont classify failures
            fail_type = 0
            if 'num' == context:
                data_bar[0][fail_type] += 1
                divisor[0][fail_type] = 1
            elif 'duration' == context:
                data_bar[0][fail_type] += (_end_time - _start_time) / 1000000.0
                divisor[0][fail_type] += 1

    print non_m42_list
    print '-------------------------'
    print data_bar
    print divisor

    for i in range(0, context_len):
        for j in range(0, metric_len):
            d = divisor[i][j]
            if d > 0:
                data['bar']['y'][i][j] = 1.0 * data_bar[i][j] / d
            else:
                data['bar']['y'][i][j] = 0.0

    print '-------------------------'
    print data['bar']['x']
    print data['bar']['y']

    if 'num' == context:
        other['bar_colors'] = ['orange']
        other['bar_names'] = ['']
        other['y_title'] = 'Num of occurrence'
        other['y_dtick'] = 10
    elif 'duration' == context:
        other['bar_colors'] = ['orange']
        other['bar_names'] = ['']
        other['y_title'] = 'Avg. duration of each failure (s)'
        other['y_dtick'] = 2

    other['x_title'] = 'Failure'
    other['bar_mode'] = 'stack'
    other['x_tickvals'] = x_tickvals
    other['x_ticktext'] = x_ticktext

    other['y_zeroline'] = False
    other['margin_r'] = 140
    other['margin_b'] = 80

    other['legend_orientation'] = 'h'
    pl.draw_bar_and_scatter(data, other)

def draw_box_for_rrc_failure_for_users(db, users, metric, context, other):
    print '=========' + other['filename']
    non_m42_list = []

    data = {}
    data['box'] = {}
    data['box']['x'] = []
    data['box']['y'] = []

    emm_types = []
    emm_rows = db.select_multiple_row(''' select id from message_types where message_type = 'LTE_NAS_EMM_State' order by id''')
    for i in range(0, len(emm_rows)):
        emm_types.append(emm_rows[i][0])

    metric_len = 0
    if 'emm' == metric:
        emm_rows = db.select_multiple_row(''' select count(*) from message_types where message_type = 'LTE_NAS_EMM_State' order by id''')
        metric_len = emm_rows[0][0]
    elif 'handover' == metric:
        metric_len = 2

    context_len = 0
    if 'duration' in context:
        context_len = 1

    for i in range(0, metric_len):
        data['box']['x'].append(i * (context_len + 1) + 1)
        data['box']['y'].append([])

    x_tickvals = [] # start from 1
    x_ticktext = []
    for i in range(0, metric_len):
        if context_len == 1:
            x_tickvals.append(i * (context_len + 1) + 1)
        else:
            x_tickvals.append(i * (context_len + 1) + (context_len + 1) / 2.0)
        x_ticktext.append(i)

    if 'emm' == metric:
        x_ticktext = []
        emm_rows = db.select_multiple_row(''' select id from message_types where message_type = 'LTE_NAS_EMM_State' order by id''')
        for i in range(0, metric_len):
            x_ticktext.append(emm_rows[i][0])

    data_box = []
    for i in range(0, metric_len):
        data_box.append([])

    for user_id in users:
        fail_rows = db.select_multiple_row(''' select start_time, end_time, message_sequence_before, message_sequence, extra
            , signal_strength, velocity, num_cell from rrc_failure_sections
            where user_id = %d
            and velocity > 0
            order by start_time''' % (user_id))
        for fail_row in fail_rows:
            _start_time = fail_row[0]
            _end_time = fail_row[1]
            msg_seq_bf_txt = fail_row[2]
            msg_seq_txt = fail_row[3]
            extra = fail_row[4]
            ss = fail_row[5]
            v = fail_row[6]
            num_cell = fail_row[7]
            extra_dict = parse_extra(extra)
            msg_seq_bf = map(int, msg_seq_bf_txt.split(','))
            msg_seq = map(int, msg_seq_txt.split(','))
            fail_type = -1
            if int(extra_dict['next_msib']) >= _start_time\
                and int(extra_dict['next_msib']) <= _end_time:
                fail_type = 0
            else:
                emm_msg = -1
                for m_i in range(len(msg_seq_bf) - 1, -1, -1):
                    if msg_seq_bf[m_i] in emm_types:
                        emm_msg = msg_seq_bf[m_i]
                        break
                if emm_msg in [41, 42]:
                    fail_type = 1
                else:
                    fail_type = 1
                    if emm_msg not in non_m42_list:
                        non_m42_list.append(emm_msg)
            if 'num' == context:
                data_box[fail_type] += 1
            elif 'duration' == context:
                data_box[fail_type].append((_end_time - _start_time) / 1000000.0)

    for i in range(0, metric_len):
        data['box']['y'][i] = data_box[i]

    print non_m42_list
    print '-------------------------'
    print data_box

    print '-------------------------'
    print data['box']['x']
    print data['box']['y']

    if 'duration' == context:
        other['box_colors'] = []
        for i in range(0, metric_len):
            other['box_colors'].append('orange')
        other['box_names'] = [1, 2, 3]
        other['box_modes'] = [False, False, False]
        other['y_title'] = 'Duration of each failure (s)'
        other['y_dtick'] = 30

    other['x_title'] = 'Failure'
    other['bar_mode'] = 'stack'
    other['x_tickvals'] = x_tickvals
    other['x_ticktext'] = x_ticktext

    other['y_zeroline'] = False
    other['margin_r'] = 140
    other['margin_b'] = 80

    other['legend_orientation'] = 'h'
    pl.draw_box_and_scatter(data, other)

def draw_map_for_rrc_failure_for_users(db, users, metric, context, other):
    print '=========' + other['filename']
    non_m42_list = []

    data = []

    emm_types = []
    emm_rows = db.select_multiple_row(''' select id from message_types where message_type = 'LTE_NAS_EMM_State' order by id''')
    for i in range(0, len(emm_rows)):
        emm_types.append(emm_rows[i][0])

    metric_len = 0
    if 'emm' == metric:
        emm_rows = db.select_multiple_row(''' select count(*) from message_types where message_type = 'LTE_NAS_EMM_State' order by id''')
        metric_len = emm_rows[0][0]
    elif 'handover' == metric:
        metric_len = 2

    context_len = 0
    if 'duration' in context:
        context_len = 1

    for i in range(0, metric_len):
        data.append([[], []])

    for user_id in users:
        fail_rows = db.select_multiple_row(''' select start_time, end_time, message_sequence_before, message_sequence, extra
            , signal_strength, velocity, num_cell from rrc_failure_sections
            where user_id = %d
            and velocity > 0
            order by start_time''' % (user_id))
        for fail_row in fail_rows:
            _start_time = fail_row[0]
            _end_time = fail_row[1]
            msg_seq_bf_txt = fail_row[2]
            msg_seq_txt = fail_row[3]
            extra = fail_row[4]
            ss = fail_row[5]
            v = fail_row[6]
            num_cell = fail_row[7]
            extra_dict = parse_extra(extra)
            msg_seq_bf = map(int, msg_seq_bf_txt.split(','))
            msg_seq = map(int, msg_seq_txt.split(','))
            fail_type = -1
            if int(extra_dict['next_msib']) >= _start_time\
                and int(extra_dict['next_msib']) <= _end_time:
                fail_type = 0
            else:
                emm_msg = -1
                for m_i in range(len(msg_seq_bf) - 1, -1, -1):
                    if msg_seq_bf[m_i] in emm_types:
                        emm_msg = msg_seq_bf[m_i]
                        break
                if emm_msg in [41, 42]:
                    fail_type = 1
                else:
                    fail_type = 1
                    if emm_msg not in non_m42_list:
                        non_m42_list.append(emm_msg)

            sql = ''' select value, time from event_location where user_id = %d and
                time >= %d and time <= %d order by time''' % (user_id, _start_time, _end_time)
            location_rows = db.select_multiple_row(sql)
            if len(location_rows) == 0 or len(location_rows) == 1:
                loc_start, loc_end = get_start_and_end(db, 'event_location', user_id, _start_time, _end_time)

            rows = db.select_multiple_row(''' select start_time from mylog_running_time where user_id = %d 
                                        and start_time >= %d and start_time < %d
                                        ''' % (user_id, loc_start, loc_end))
            if len(rows) != 0:
                continue

            sql = ''' select value, time from event_location where user_id = %d and
                time >= %d and time <= %d order by time''' % (user_id, loc_start, loc_end)
            location_rows = db.select_multiple_row(sql)
            if len(location_rows) == 0 or len(location_rows) == 1:
                continue
            x_loc_list = []
            y_loc_list = []
            for i in range(0, len(location_rows)):
                location_0 = location_rows[i][0].split(' ')
                y_loc_list.append(float(location_0[0]))
                x_loc_list.append(float(location_0[1]))
            if 'num' == context:
                data[fail_type][0].append(sum(x_loc_list) / len(x_loc_list))
                data[fail_type][1].append(sum(y_loc_list) / len(y_loc_list))

    print non_m42_list
    print '-------------------------'
    print data

    other['x_title'] = ''
    other['x_dtick'] = 0.1
    other['x_tick_size'] = 18

    if 'num' == context:
        other['colors'] = ['blue', 'red', 'green']
        other['names'] = [1, 2, 3]
        other['modes'] = ['markers', 'markers', 'markers']
        other['xaxises'] = ['x', 'x', 'x']
        other['yaxises'] = ['y', 'y', 'y']
        other['y_title'] = ''
        other['y_dtick'] = 0.1
        other['y_tick_size'] = 18

    #other['x_tickvals'] = x_tickvals
    #other['x_ticktext'] = x_ticktext

    other['y_zeroline'] = False
    other['margin_r'] = 140
    other['margin_b'] = 80

    other['legend_orientation'] = 'h'
    pl.draw_scatter(data, other)
    #pl.draw_scattermapbox(data, other)

def draw_histogram_for_rrc_failure_for_users(db, users, metric, context, other):
    print '=========' + other['filename']
    non_m42_list = []

    data = {}
    data['bar'] = {}
    data['bar']['x'] = []
    data['bar']['y'] = []
    data['scatter'] = []

    emm_types = []
    emm_rows = db.select_multiple_row(''' select id from message_types where message_type = 'LTE_NAS_EMM_State' order by id''')
    for i in range(0, len(emm_rows)):
        emm_types.append(emm_rows[i][0])

    metric_len = 0
    data['bar']['x'].append([])
    data['bar']['y'].append([])

    context_len = 0
    data['scatter'].append([[], []])

    x_tickvals = [] # start from 1
    x_ticktext = []

    data_bar = []
    for i in range(0, context_len):
        data_bar.append([])
        for j in range(0, metric_len):
            data_bar[i].append(0)

    fail_index = 0
    fail_rows = db.select_multiple_row(''' select start_time, end_time, message_sequence_before, message_sequence, extra
        , signal_strength, velocity, num_cell, end_time - start_time as duration
        from rrc_failure_sections
        order by duration''')
        #where user_id = %d
        #order by (end_time - start_time)''' % (user_id))
    for fail_row in fail_rows:
        _start_time = fail_row[0]
        _end_time = fail_row[1]
        msg_seq_bf_txt = fail_row[2]
        msg_seq_txt = fail_row[3]
        extra = fail_row[4]
        ss = fail_row[5]
        v = fail_row[6]
        num_cell = fail_row[7]
        extra_dict = parse_extra(extra)
        msg_seq_bf = map(int, msg_seq_bf_txt.split(','))
        msg_seq = map(int, msg_seq_txt.split(','))
        fail_type = -1
        if int(extra_dict['next_msib']) >= _start_time\
            and int(extra_dict['next_msib']) <= _end_time:
            fail_type = 0
        else:
            emm_msg = -1
            for m_i in range(len(msg_seq_bf) - 1, -1, -1):
                if msg_seq_bf[m_i] in emm_types:
                    emm_msg = msg_seq_bf[m_i]
                    break
            if emm_msg in [41, 42]:
                fail_type = 1
            else:
                fail_type = 1
                if emm_msg not in non_m42_list:
                    non_m42_list.append(emm_msg)
        if ('t0' == metric and fail_type == 0)\
            or ('t1' == metric and fail_type == 1)\
            or ('t2' == metric and fail_type == 2):
            data['bar']['x'][0].append(fail_index)
            data['bar']['y'][0].append((_end_time - _start_time) / 1000000.0)
            x_tickvals.append(fail_index)
            x_ticktext.append(fail_index)
            if 'msg' == context:
                for msg in msg_seq:
                    data['scatter'][0][0].append(fail_index)
                    data['scatter'][0][1].append(msg)
            elif 'msg_bf' == context:
                for msg in msg_seq_bf:
                    data['scatter'][0][0].append(fail_index)
                    data['scatter'][0][1].append(msg)
            elif 'num_mib_in_msg' == context:
                num_mib = 0
                for msg in msg_seq:
                    if msg == 12:
                        num_mib += 1
                data['scatter'][0][0].append(fail_index)
                data['scatter'][0][1].append(num_mib)
            elif 'num_sib_in_msg' == context:
                num_sib = 0
                for msg in msg_seq:
                    if msg == 17:
                        num_sib += 1
                data['scatter'][0][0].append(fail_index)
                data['scatter'][0][1].append(num_sib)
            elif 'last_emm' == context:
                last_emm = -1
                for msg_i in range(len(msg_seq) - 1, -1, -1):
                    if msg_seq[msg_i] in emm_types:
                        last_emm = msg_seq[msg_i]
                        break
                if last_emm > 0:
                    data['scatter'][0][0].append(fail_index)
                    data['scatter'][0][1].append(last_emm)
            elif 'last_emm_bf' == context:
                last_emm = -1
                for msg_i in range(len(msg_seq_bf) - 1, -1, -1):
                    if msg_seq_bf[msg_i] in emm_types:
                        last_emm = msg_seq_bf[msg_i]
                        break
                print fail_row
                if last_emm > 0:
                    data['scatter'][0][0].append(fail_index)
                    data['scatter'][0][1].append(last_emm)
            fail_index += 1

    print '-------------------------'
    print len(data['bar']['x'][0])
    print data['bar']['x'][0]
    print data['bar']['y'][0]
    print '-------------------------'
    print len(data['scatter'][0][0])
    #print data['scatter'][0][0]
    #print data['scatter'][0][1]

    if 't0' == metric:
        other['x_title'] = 'Failure Type 0'
        other['bar_colors'] = ['orange']
        other['bar_names'] = ['']
        other['bar_mode'] = ['stack']
    elif 't1' == metric:
        other['x_title'] = 'Failure Type 1'
        other['bar_colors'] = ['orange']
        other['bar_names'] = ['']
        other['bar_mode'] = ['stack']
    elif 't2' == metric:
        other['x_title'] = 'Failure Type 1'
        other['bar_colors'] = ['orange']
        other['bar_names'] = ['']
        other['bar_mode'] = ['stack']
    other['y_title'] = 'Failure Duration (s)'
    other['y_dtick'] = 5
    other['y_tick_size'] = 12
    other['x_dtick'] = 0.1
    other['x_tick_size'] = 12

    if 'msg' == context:
        other['line_colors'] = ['blue']
        other['modes'] = ['markers']
        other['names'] = ['']
        other['xaxises'] = ['x']
        other['yaxises'] = ['y2']
        other['y2_title'] = 'Message index'
        other['y2_dtick'] = 1
        other['y2_tick_size'] = 12
    elif 'msg_bf' == context:
        other['line_colors'] = ['blue']
        other['modes'] = ['markers']
        other['names'] = ['']
        other['xaxises'] = ['x']
        other['yaxises'] = ['y2']
        other['y2_title'] = 'Message index (before)'
        other['y2_dtick'] = 1
        other['y2_tick_size'] = 12
    elif 'num_mib_in_msg' == context:
        other['line_colors'] = ['green']
        other['modes'] = ['markers']
        other['names'] = ['']
        other['xaxises'] = ['x']
        other['yaxises'] = ['y2']
        other['y2_title'] = 'Number of MIB'
        other['y2_dtick'] = 1
        other['y2_tick_size'] = 12
    elif 'num_sib_in_msg' == context:
        other['line_colors'] = ['purple']
        other['modes'] = ['markers']
        other['names'] = ['']
        other['xaxises'] = ['x']
        other['yaxises'] = ['y2']
        other['y2_title'] = 'Number of SIB'
        other['y2_dtick'] = 1
        other['y2_tick_size'] = 12
    elif 'last_emm' == context:
        other['line_colors'] = ['grey']
        other['modes'] = ['markers']
        other['names'] = ['']
        other['xaxises'] = ['x']
        other['yaxises'] = ['y2']
        other['y2_title'] = 'Last EMM state'
        other['y2_dtick'] = 1
        other['y2_tick_size'] = 12
    elif 'last_emm_bf' == context:
        other['line_colors'] = ['grey']
        other['modes'] = ['markers']
        other['names'] = ['']
        other['xaxises'] = ['x']
        other['yaxises'] = ['y2']
        other['y2_title'] = 'Last EMM state (before)'
        other['y2_dtick'] = 1
        other['y2_tick_size'] = 12

    other['x_tickvals'] = x_tickvals
    other['x_ticktext'] = x_ticktext

    other['y_zeroline'] = False
    other['y2_zeroline'] = False
    other['margin_r'] = 140
    other['margin_b'] = 80

    other['legend_orientation'] = 'h'
    pl.draw_bar_and_scatter(data, other)
