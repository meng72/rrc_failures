#!/usr/local/bin/python

import math

def get_start_and_end(db, table, user_id, start_time, end_time):
    sql = ''' select max(time) from %s
        where user_id = %d and time <= %d''' % (table, user_id, start_time)
    row = db.select_single_row(sql)
    if row is None or row[0] is None or row[0] == 0:
        _start_time = start_time
    else:
        _start_time = row[0]
    sql = ''' select min(time) from %s
        where user_id = %d and time >= %d''' % (table, user_id, end_time)
    row = db.select_single_row(sql)
    if row is None or row[0] is None or row[0] == 0:
        _end_time = end_time
    else:
        _end_time = row[0]
    return [_start_time, _end_time]

def get_start_time_and_end_time(db, table, user_id, start_time, end_time):
    sql = ''' select max(start_time) from %s
        where user_id = %d and start_time <= %d''' % (table, user_id, start_time)
    row = db.select_single_row(sql)
    if row is None or row[0] is None or row[0] == 0:
        _start_time = start_time
    else:
        _start_time = row[0]
    sql = ''' select min(end_time) from %s
        where user_id = %d and end_time >= %d''' % (table, user_id, end_time)
    row = db.select_single_row(sql)
    if row is None or row[0] is None or row[0] == 0:
        _end_time = end_time
    else:
        _end_time = row[0]
    return [_start_time, _end_time]

def get_time_by_base(time, base, init_type):
    init = 0
    if init_type == 1:
        init = 24 * 3600
    elif init_type == 2:
        init = 3600
    return 1.0 * (time - base) / 1000000 / init

def get_milog_running_time(db, user_id, start_time, end_time):
    sql = ''' select sum(end_time - start_time) from milog_running_time
        where user_id = %d and start_time >= %d and end_time <= %d
        order by start_time ''' % (user_id, start_time, end_time)
    row = db.select_single_row(sql)
    mi_time = row[0]
    """
    if mi_time > 0.0:
        print 'mi_time ' + str(mi_time)
    else:
        print 'mi_time is zero!'
        return
    """
    return float(mi_time)

def get_exact_milog_running_time(db, user_id, start_time, end_time):
    _duration = get_milog_running_time(db ,user_id, start_time, end_time)
    sql = ''' select min(end_time) from milog_running_time
        where user_id = %d and end_time >= %d ''' % (user_id, start_time)
    _end = db.select_multiple_row(sql)[0][0]
    if _end > end_time:
        _duration += float(end_time - start_time)
    else:
        _duration += float(_end - start_time)
        sql = ''' select max(start_time) from milog_running_time
            where user_id = %d and start_time <= %d ''' % (user_id, end_time)
        _start = db.select_multiple_row(sql)[0][0]
        _duration += float(end_time - _start)
    return _duration

def remove_invalid_plot_point(data, index, value):
    i = 0
    while True:
        if i >= len(data):
            break
        if  data[i][index] != value:
            i += 1
        else:
            data.remove(data[i])
    return data

def get_cdf_data(raw_data, scale = 1.0):
    index = 0
    x_data = []
    y_data = []
    sorted_data = sorted(raw_data)
    for row in sorted_data:
        x_data.append(1.0 * row / scale)
        y_data.append(index * 1.0 / len(sorted_data) * 100)
        index += 1
    return [x_data, y_data]

def get_distance(location, pre_location):
    diff_lati = abs(float(location[0]) - float(pre_location[0]))
    diff_long = abs(float(location[1]) - float(pre_location[1]))
    unit_long = math.cos(float(location[0])) * 69.172
    x = diff_lati * 69.172
    y = diff_long * unit_long
    return math.hypot(x, y)

def get_velocity(db, user_id, start_time, end_time):
    distance = 0.0
    len_period = 0.0
    _start_time = start_time
    _end_time = end_time

    sql = ''' select value, time from event_location where user_id = %d and
        time >= %d and time <= %d order by time''' % (user_id, _start_time, _end_time)
    location_rows = db.select_multiple_row(sql)
    if len(location_rows) == 0 or len(location_rows) == 1:
        _start_time, _end_time = get_start_and_end(db, 'event_location', user_id, start_time, end_time)

    rows = db.select_multiple_row(''' select start_time from mylog_running_time where user_id = %d 
                                and start_time >= %d and start_time < %d
                                ''' % (user_id, _start_time, _end_time))
    if len(rows) != 0:
        return -0.5

    #print [_start_time, _end_time]
    sql = ''' select value, time from event_location where user_id = %d and
        time >= %d and time <= %d order by time''' % (user_id, _start_time, _end_time)
    location_rows = db.select_multiple_row(sql)
    if len(location_rows) == 0 or len(location_rows) == 1:
        return -1.0
    for i in range(0, len(location_rows) - 1):
        _location_0 = location_rows[i][0].split(' ')
        _location_1 = location_rows[i + 1][0].split(' ')
        distance += get_distance(_location_1, _location_0)
        len_period += float((location_rows[i + 1][1] - location_rows[i][1])) / 1000000.0
    if abs(len_period - (end_time - start_time) / 1000000.0) > 200:
        return -2.0
    return distance / len_period

def append_single_segment(x_list, y_list, x_data, y_data):
    for i in range(0, len(x_data)):
        x_list.append(x_data[i])
        y_list.append(y_data[i])
    return x_list, y_list

def get_total_duration(time_pairs):
    duration = 0.0
    for time_pair in time_pairs:
        duration += (time_pair[1] - time_pair[0]) / 1000000.0
    return duration
