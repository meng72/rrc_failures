#!/usr/local/bin/python
"""
A library of basic calculation functions.

"""

from datetime import datetime, timedelta

epoch = datetime.utcfromtimestamp(0)
def unix_time_micros(dt):
    return (dt - epoch).total_seconds() * 1000000

def unix_time_second(dt):
    return (dt - epoch).total_seconds()

def normalize(values, nomalizer):	
    result = {}
    for key in values:
        result[key] = values[key] * 1.0 / nomalizer
    return result

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

def merge_dict(dict1, dict2, keys):
    result = {}
    for key in keys:
        e1 = 0
        e2 = 0
        if key in dict1:
            e1 = dict1[key]
        if key in dict2:
            e2 = dict2[key]
        result[key] = e1 + e2
    return result            
