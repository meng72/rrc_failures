#!/usr/bin/python
"""
A decoder to decode MI log file from binary
to xml format. 

Author: Xiaomeng Chen
"""

import os
import sys
from contextlib import contextmanager

from mobile_insight.monitor import OfflineReplayer
from mobile_insight.analyzer import MsgLogger
from mobile_insight.analyzer import LteRlcAnalyzer, LteRrcAnalyzer, LtePdcpAnalyzer,  LteMeasurementAnalyzer, LteNasAnalyzer, LtePhyAnalyzer, LteMacAnalyzer
from mobile_insight.analyzer import UmtsNasAnalyzer, WcdmaRrcAnalyzer, MmAnalyzer

def decode(mi2log_file, xml_file):
    src = OfflineReplayer()
    logger = MsgLogger()
    logger.set_decode_format(MsgLogger.XML)
    logger.set_dump_type(MsgLogger.FILE_ONLY)

    # Initialize a 3G/4G monitor
    src.set_input_path(mi2log_file)
    #src.enable_log_all()
    src.enable_log('LTE_MAC_DL_Transport_Block')
    src.enable_log('LTE_MAC_Rach_Attempt')
    src.enable_log('LTE_MAC_Rach_Trigger')
    src.enable_log('LTE_MAC_UL_Transport_Block')
    src.enable_log('LTE_NAS_EMM_OTA_Incoming_Packet')
    src.enable_log('LTE_NAS_EMM_OTA_Outgoing_Packet')
    src.enable_log('LTE_NAS_EMM_State')
    src.enable_log('LTE_NAS_ESM_OTA_Incoming_Packet')
    src.enable_log('LTE_NAS_ESM_OTA_Outgoing_Packet')
    src.enable_log('LTE_NAS_ESM_State')
    src.enable_log('LTE_PHY_BPLMN_Cell_Confirm')
    src.enable_log('LTE_PHY_BPLMN_Cell_Request')
    src.enable_log('LTE_PHY_Connected_Mode_Intra_Freq_Meas')
    src.enable_log('LTE_PHY_Connected_Mode_Neighbor_Measurement')
    src.enable_log('LTE_PHY_Idle_Neighbor_Cell_Meas')
    src.enable_log('LTE_PHY_Inter_RAT_CDMA_Measurement')
    src.enable_log('LTE_PHY_Inter_RAT_Measurement')
    src.enable_log('LTE_PHY_PDCCH_Decoding_Result')
    src.enable_log('LTE_PHY_PDSCH_Decoding_Result')
    src.enable_log('LTE_PHY_Serv_Cell_Measurement')
    src.enable_log('LTE_RRC_MIB_Message_Log_Packet')
    src.enable_log('LTE_RRC_MIB_Packet')
    src.enable_log('LTE_RRC_OTA_Packet')
    src.enable_log('LTE_RRC_Serv_Cell_Info')

    logger.save_decoded_msg_as(xml_file)
    logger.set_source(src)

    # Start the monitoring
    src.run()

@contextmanager
def stdout_redirected(new_stdout):
    save_stdout = sys.stdout
    sys.stdout = new_stdout
    try:
        yield None
    finally:
        sys.stdout = save_stdout

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print "Please provide path to mi2log_file and xml_file"
        exit(-1)
    decode(sys.argv[1], sys.argv[2])
