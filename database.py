#!/usr/local/bin/python
"""
An database interface providing APIs of MySQL database
operations. 

"""
import json
from pprint import pprint
import mysql.connector
from mysql.connector import errorcode
import logging

## Database schema: 
TABLES = {}
INITIAL_SQLS = {}
TABLES['user'] = (
        "CREATE TABLE IF NOT EXISTS `user` ("
        "  `id` int(11) NOT NULL AUTO_INCREMENT,"
        "  `hash_id` char(16) NOT NULL,"
        "  PRIMARY KEY (`id`),"
        "  UNIQUE KEY (`hash_id`)"
        ") ENGINE=InnoDB")

TABLES['event_system'] = (
        "CREATE TABLE IF NOT EXISTS `event_system` ("
        "  `user_id` int(11) unsigned NOT NULL,"
        "  `time` bigint NOT NULL,"
        "  `device` char(32) NOT NULL,"
        "  `model` char(32) NOT NULL,"
        "  `brand` char(32) NOT NULL,"
        "  `radio` char(64) NOT NULL,"
        "  `hardware` char(16) NOT NULL,"
        "  `host` char(16) NOT NULL,"
        "  `board` char(16) NOT NULL,"
        "  `tags` char(16) NOT NULL,"
        "  `type` char(16) NOT NULL,"
        "  `release` char(32) NOT NULL,"
        "  `sdk_int` tinyint  unsigned NOT NULL,"
        "  `build_id` char(16) NOT NULL,"
        "  `timezone` char(16) NOT NULL,"
        "  UNIQUE KEY (`user_id`, `device`, `model`, `brand`, `radio`, `hardware`,"
        "  `host`, `board`, `tags`, `type`, `release`, `sdk_int`, `build_id`, `timezone`)"
        ") ENGINE=InnoDB")

TABLES['event_cpu'] = (
        "CREATE TABLE IF NOT EXISTS `event_cpu` ("
        "  `user_id` int(11)  unsigned NOT NULL,"
        "  `start_time` bigint NOT NULL,"
        "  `end_time` bigint NOT NULL,"
        "  `app_id` int(11)  unsigned NOT NULL,"
        "  `user_time` int  unsigned NOT NULL,"
        "  `system_time` int  unsigned NOT NULL,"
        "  UNIQUE KEY (`user_id`, `start_time`, `app_id`),"
        "  KEY (`user_id`),"
        "  KEY (`app_id`),"
        "  KEY (`start_time`),"
        "  KEY (`end_time`)"
        ") ENGINE=InnoDB")

TABLES['app'] = (
        "create table if not exists `app` ("
        "  `id` int(11)  unsigned not null auto_increment,"
        "  `pkg_name` char(50) not null,"
        "  primary key (`id`),"
        "  unique key (`pkg_name`)"
        ") engine=innodb")

TABLES['event_location'] = (
        "CREATE TABLE IF NOT EXISTS `event_location` ("
        "  `user_id` int(11)  unsigned NOT NULL,"
        "  `time` bigint NOT NULL,"
        "  `value` text NOT NULL,"
        "  UNIQUE KEY (`user_id`, `time`),"
        "  KEY (`user_id`),"
        "  KEY (`time`)"
        ") ENGINE=InnoDB")

TABLES['event_user_activity'] = (
        "CREATE TABLE IF NOT EXISTS `event_user_activity` ("
        "  `user_id` int(11)  unsigned NOT NULL,"
        "  `time` bigint NOT NULL,"
        "  `type_id` tinyint unsigned NOT NULL,"
        "  `confidence` tinyint unsigned NOT NULL,"
        "  UNIQUE KEY (`user_id`, `time`),"
        "  KEY (`user_id`),"
        "  KEY (`time`)"
        ") ENGINE=InnoDB")

TABLES['type_event_user_activity'] = ( 
        "create table if not exists `type_event_user_activity` (" 
        "  `id` tinyint unsigned not null auto_increment," 
        "  `value` char(50) not null," 
        "  primary key (`id`)," 
        "  unique key (`value`)" 
        ") engine=innodb")

TABLES['app'] = ( 
        "create table if not exists `app` (" 
        "  `id` int(11)  unsigned not null auto_increment," 
        "  `pkg_name` char(50) not null," 
        "  primary key (`id`)," 
        "  unique key (`pkg_name`)" 
        ") engine=innodb")

TABLES['bssid'] = (
        "create table if not exists `bssid` ("
        "  `id` int(11)  unsigned not null auto_increment,"
        "  `value` char(32) not null,"
        "  primary key (`id`),"
        "  unique key (`value`)"
        ") engine=innodb")

"""
TABLES['modem_message'] = (
        "CREATE TABLE IF NOT EXISTS `modem_message` ("
        "  `id` smallint  unsigned NOT NULL AUTO_INCREMENT,"
        "  `value` char(128) NOT NULL,"
        "  PRIMARY KEY (`id`),"
        "  UNIQUE KEY (`value`)"
        ") ENGINE=InnoDB")

INITIAL_SQLS['insert_modem_message'] = (
        "insert ignore into modem_message (`id`, `value`) values "
        "(1, 'LTE_LL1_PCFICH_Decoding_Results'),"
        "(2, 'LTE_PHY_Connected_Mode_Neighbor_Measurement'),"
        "(3, 'LTE_PHY_Connected_Mode_Intra_Freq_Meas'),"
        "(4, 'LTE_PHY_Serv_Cell_Measurement'),"
        "(5, 'LTE_PHY_Inter_RAT_Measurement'),"
        "(6, 'LTE_PHY_PDSCH_Packet'),"
        "(7, 'LTE_MAC_DL_Transport_Block'),"
        "(8, 'LTE_MAC_UL_Transport_Block'),"
        "(9, 'LTE_RLC_DL_AM_All_PDU'),"
        "(10, 'LTE_RLC_UL_AM_All_PDU'),"
        "(11, 'LTE_RRC_Serv_Cell_Info'),"
        "(12, 'LTE_RRC_OTA_Packet'),"
        "(13, 'LTE_RRC_MIB_Packet'),"
        "(14, 'LTE_RRC_MIB_Message_Log_Packet'),"
        "(15, 'LTE_NAS_EMM_OTA_Incoming_Packet'),"
        "(16, 'LTE_NAS_EMM_OTA_Outgoing_Packet'),"
        "(17, 'LTE_NAS_EMM_State'),"
        "(18, 'LTE_NAS_ESM_OTA_Incoming_Packet'),"
        "(19, 'LTE_NAS_ESM_OTA_Outgoing_Packet'),"
        "(20, 'LTE_NAS_ESM_State'),"
        "(21, 'WCDMA_RRC_OTA_Packet'),"
        "(22, 'WCDMA_RRC_Serv_Cell_Info'),"
        "(23, 'UMTS_NAS_GMM_State'),"
        "(24, 'UMTS_NAS_MM_State'),"
        "(25, 'UMTS_NAS_MM_REG_State'),"
        "(26, 'UMTS_NAS_OTA_Packet')")
"""
TABLES['event_LTE_LL1_PCFICH_Decoding_Results'] = (
        "CREATE TABLE IF NOT EXISTS `event_LTE_LL1_PCFICH_Decoding_Results` ("
        "  `user_id` int(11)  unsigned NOT NULL,"
        "  `time` bigint NOT NULL,"
        "  `trust` tinyint DEFAULT NULL,"
        "  UNIQUE KEY (`user_id`, `time`),"
        "  KEY (`user_id`),"
        "  KEY (`time`)"
        ") ENGINE=InnoDB")

TABLES['event_LTE_PHY_Connected_Mode_Neighbor_Measurement'] = (
        "CREATE TABLE IF NOT EXISTS `event_LTE_PHY_Connected_Mode_Neighbor_Measurement` ("
        "  `user_id` int(11)  unsigned NOT NULL,"
        "  `time` bigint NOT NULL,"
        "  `trust` tinyint DEFAULT NULL,"
        "  UNIQUE KEY (`user_id`, `time`),"
        "  KEY (`user_id`),"
        "  KEY (`time`)"
        ") ENGINE=InnoDB")

TABLES['event_LTE_PHY_Connected_Mode_Intra_Freq_Meas'] = (
        "CREATE TABLE IF NOT EXISTS `event_LTE_PHY_Connected_Mode_Intra_Freq_Meas` ("
        "  `user_id` int(11)  unsigned NOT NULL,"
        "  `time` bigint NOT NULL,"
        "  `E_ARFCN` int NOT NULL,"
        "  `spcid` int NOT NULL,"
        "  `RSRP` float NOT NULL,"
        "  `RSRQ` float NOT NULL,"
        "  `neipcid` int DEFAULT NULL,"
        "  `neiRSRP` float DEFAULT NULL,"
        "  `neiRSRQ` float DEFAULT NULL,"
        "  `n_neic` smallint NOT NULL,"
        "  `n_detc` smallint NOT NULL,"
        "  `trust` tinyint DEFAULT NULL,"
        "  UNIQUE KEY (`user_id`, `time`),"
        "  KEY (`user_id`),"
        "  KEY (`time`)"
        ") ENGINE=InnoDB")

TABLES['event_LTE_PHY_Serv_Cell_Measurement'] = (
        "CREATE TABLE IF NOT EXISTS `event_LTE_PHY_Serv_Cell_Measurement` ("
        "  `user_id` int(11)  unsigned NOT NULL,"
        "  `time` bigint NOT NULL,"
        "  `trust` tinyint DEFAULT NULL,"
        "  UNIQUE KEY (`user_id`, `time`),"
        "  KEY (`user_id`),"
        "  KEY (`time`)"
        ") ENGINE=InnoDB")

TABLES['event_LTE_PHY_Inter_RAT_Measurement'] = (
        "CREATE TABLE IF NOT EXISTS `event_LTE_PHY_Inter_RAT_Measurement` ("
        "  `user_id` int(11)  unsigned NOT NULL,"
        "  `time` bigint NOT NULL,"
        "  `trust` tinyint DEFAULT NULL,"
        "  UNIQUE KEY (`user_id`, `time`),"
        "  KEY (`user_id`),"
        "  KEY (`time`)"
        ") ENGINE=InnoDB")

TABLES['event_LTE_PHY_PDSCH_Packet'] = (
        "CREATE TABLE IF NOT EXISTS `event_LTE_PHY_PDSCH_Packet` ("
        "  `user_id` int(11)  unsigned NOT NULL,"
        "  `time` bigint NOT NULL,"
        "  `scid` int NOT NULL,"
        "  `n_txant` tinyint NOT NULL,"
        "  `n_rxant` tinyint NOT NULL,"
        "  `MCS0` char(8) DEFAULT NULL,"
        "  `MCS1` char(8) DEFAULT NULL,"
        "  `trust` tinyint DEFAULT NULL,"
        "  UNIQUE KEY (`user_id`, `time`),"
        "  KEY (`user_id`),"
        "  KEY (`time`)"
        ") ENGINE=InnoDB")

TABLES['event_LTE_MAC_Transport_Block'] = (
        "CREATE TABLE IF NOT EXISTS `event_LTE_MAC_Transport_Block` ("
        "  `user_id` int(11)  unsigned NOT NULL,"
        "  `time` bigint NOT NULL,"
        "  `direction` char(3) NOT NULL,"
        "  `n_sample` tinyint NOT NULL,"
        "  `n_byte` mediumint NOT NULL,"
        "  `trust` tinyint DEFAULT NULL,"
        "  UNIQUE KEY (`user_id`, `time`, `direction`),"
        "  KEY (`user_id`),"
        "  KEY (`time`),"
        "  KEY (`direction`)"
        ") ENGINE=InnoDB")

TABLES['event_LTE_RLC_AM_All_PDU'] = (
        "CREATE TABLE IF NOT EXISTS `event_LTE_RLC_AM_All_PDU` ("
        "  `user_id` int(11)  unsigned NOT NULL,"
        "  `time` bigint NOT NULL,"
        "  `direction` char(3) NOT NULL,"
        "  `pdu_bytes` tinyint NOT NULL,"
        "  `trust` tinyint DEFAULT NULL,"
        "  UNIQUE KEY (`user_id`, `time`, `direction`),"
        "  KEY (`user_id`),"
        "  KEY (`time`),"
        "  KEY (`direction`)"
        ") ENGINE=InnoDB")


TABLES['event_LTE_RRC_Serv_Cell_Info'] = (
        "CREATE TABLE IF NOT EXISTS `event_LTE_RRC_Serv_Cell_Info` ("
        "  `user_id` int(11)  unsigned NOT NULL,"
        "  `time` bigint NOT NULL,"
        "  `cid` int NOT NULL,"
        "  `dl_freq` smallint unsigned NOT NULL,"
        "  `ul_freq` smallint unsigned NOT NULL,"
        "  `dl_bandwidth` smallint unsigned NOT NULL,"
        "  `ul_bandwidth` smallint unsigned NOT NULL,"
        "  `cell_identity` int NOT NULL,"
        "  `tac` int NOT NULL,"
        "  `mcc` int NOT NULL,"
        "  `mnc` int NOT NULL,"
        "  `trust` tinyint DEFAULT NULL,"
        "  UNIQUE KEY (`user_id`, `time`),"
        "  KEY (`user_id`),"
        "  KEY (`time`)"
        ") ENGINE=InnoDB")

TABLES['event_LTE_RRC_OTA_Packet'] = (
        "CREATE TABLE IF NOT EXISTS `event_LTE_RRC_OTA_Packet` ("
        "  `user_id` int(11)  unsigned NOT NULL,"
        "  `time` bigint NOT NULL,"
        "  `rrc_state_id` smallint unsigned NOT NULL,"
        "  `pcid` smallint DEFAULT 0,"
        "  `freq` int DEFAULT 0,"
        "  `sfnum` int DEFAULT -1,"
        "  `trust` tinyint DEFAULT NULL,"
        "  UNIQUE KEY (`user_id`, `time`),"
        "  KEY (`user_id`),"
        "  KEY (`time`)"
        ") ENGINE=InnoDB")

TABLES['lte_rrc_state'] = (
        "CREATE TABLE IF NOT EXISTS `lte_rrc_state` ("
        "  `id` smallint  unsigned NOT NULL AUTO_INCREMENT,"
        "  `value` char(128) NOT NULL,"
        "  PRIMARY KEY (`id`),"
        "  UNIQUE KEY (`value`)"
        ") ENGINE=InnoDB")

INITIAL_SQLS['insert_lte_rrc_state'] = (
        "insert ignore into lte_rrc_state (`id`, `value`) values "
        "(1, 'SIB1'),"
        "(2, 'SIBN'),"
        "(3, 'rrcConnectionRequest'),"
        "(4, 'rrcConnectionSetup'),"
        "(5, 'rrcConnectionSetupComplete'),"
        "(6, 'securityModeCommand'),"
        "(7, 'securityModeComplete'),"
        "(8, 'rrcConnectionReconfiguration'),"
        "(9, 'rrcConnectionReconfigurationComplete'),"
        "(10, 'rrcConnectionRelease'),"
        "(11, 'dlInformationTransfer'),"
        "(12, 'ulInformationTransfer'),"
        "(13, 'ueCapabilityEnquiry'),"
        "(14, 'ueCapabilityInformation'),"
        "(15, 'rrcConnectionReestablishmentRequest'),"
        "(16, 'rrcConnectionReestablishment'),"
        "(17, 'rrcConnectionReestablishmentComplete'),"
        "(18, 'PCCH-Message'),"
        "(19, 'measurementReport'),"
        "(20, 'mobilityFromEUTRACommand'),"
        "(21, 'rrcConnectionReestablishmentReject'),"
        "(22, 'rrcConnectionReject')"
        )

TABLES['event_LTE_RRC_MIB_Packet'] = (
        "CREATE TABLE IF NOT EXISTS `event_LTE_RRC_MIB_Packet` ("
        "  `user_id` int(11)  unsigned NOT NULL,"
        "  `time` bigint unsigned NOT NULL,"
        "  `pcid` int unsigned NOT NULL,"
        "  `freq` int unsigned NOT NULL,"
        "  `n_ant` tinyint unsigned NOT NULL,"
        "  `dl_bandwidth` smallint unsigned NOT NULL,"
        "  `trust` tinyint DEFAULT NULL,"
        "  UNIQUE KEY (`user_id`, `time`),"
        "  KEY (`user_id`),"
        "  KEY (`time`)"
        ") ENGINE=InnoDB")

TABLES['event_LTE_RRC_MIB_Message_Log_Packet'] = (
        "CREATE TABLE IF NOT EXISTS `event_LTE_RRC_MIB_Message_Log_Packet` ("
        "  `user_id` int(11)  unsigned NOT NULL,"
        "  `time` bigint NOT NULL,"
        "  `trust` tinyint DEFAULT NULL,"
        "  UNIQUE KEY (`user_id`, `time`),"
        "  KEY (`user_id`),"
        "  KEY (`time`)"
        ") ENGINE=InnoDB")

TABLES['event_LTE_NAS_EMM_OTA_Incoming_Packet'] = (
        "CREATE TABLE IF NOT EXISTS `event_LTE_NAS_EMM_OTA_Incoming_Packet` ("
        "  `user_id` int(11)  unsigned NOT NULL,"
        "  `time` bigint NOT NULL,"
        "  `trust` tinyint DEFAULT NULL,"
        "  `nas_emm_msg_type_id` int(11),"
        "  `extra` text,"
        "  UNIQUE KEY (`user_id`, `time`),"
        "  KEY (`user_id`),"
        "  KEY (`time`)"
        ") ENGINE=InnoDB")

TABLES['event_LTE_NAS_EMM_OTA_Outgoing_Packet'] = (
        "CREATE TABLE IF NOT EXISTS `event_LTE_NAS_EMM_OTA_Outgoing_Packet` ("
        "  `user_id` int(11)  unsigned NOT NULL,"
        "  `time` bigint NOT NULL,"
        "  `trust` tinyint DEFAULT NULL,"
        "  `nas_emm_msg_type_id` int(11),"
        "  `extra` text,"
        "  UNIQUE KEY (`user_id`, `time`),"
        "  KEY (`user_id`),"
        "  KEY (`time`)"
        ") ENGINE=InnoDB")

TABLES['event_LTE_NAS_EMM_State'] = (
        "CREATE TABLE IF NOT EXISTS `event_LTE_NAS_EMM_State` ("
        "  `user_id` int(11)  unsigned NOT NULL,"
        "  `time` bigint NOT NULL,"
        "  `emm_state_id` tinyint unsigned NOT NULL,"
        "  `emm_substate_id` tinyint unsigned NOT NULL,"
        "  `guti_valid` tinyint NOT NULL,"
        "  `guti_ueid` int unsigned NOT NULL,"
        "  `guti_plmn` char(16) NOT NULL,"
        "  `guti_mme_groupid` char(8) NOT NULL,"
        "  `guti_mme_code` char(8) NOT NULL,"
        "  `guti_m_tmsi` char(16) NOT NULL,"
        "  `trust` tinyint DEFAULT NULL,"
        "  UNIQUE KEY (`user_id`, `time`),"
        "  KEY (`user_id`),"
        "  KEY (`time`)"
        ") ENGINE=InnoDB")

TABLES['emm_state'] = (
        "CREATE TABLE IF NOT EXISTS `emm_state` ("
        "  `id` smallint  unsigned NOT NULL AUTO_INCREMENT,"
        "  `value` char(128) NOT NULL,"
        "  PRIMARY KEY (`id`),"
        "  UNIQUE KEY (`value`)"
        ") ENGINE=InnoDB")

TABLES['emm_substate'] = (
        "CREATE TABLE IF NOT EXISTS `emm_substate` ("
        "  `id` smallint  unsigned NOT NULL AUTO_INCREMENT,"
        "  `value` char(128) NOT NULL,"
        "  PRIMARY KEY (`id`),"
        "  UNIQUE KEY (`value`)"
        ") ENGINE=InnoDB")

TABLES['event_LTE_NAS_ESM_OTA_Incoming_Packet'] = (
        "CREATE TABLE IF NOT EXISTS `event_LTE_NAS_ESM_OTA_Incoming_Packet` ("
        "  `user_id` int(11)  unsigned NOT NULL,"
        "  `time` bigint NOT NULL,"
        "  `trust` tinyint DEFAULT NULL,"
        "  UNIQUE KEY (`user_id`, `time`),"
        "  KEY (`user_id`),"
        "  KEY (`time`)"
        ") ENGINE=InnoDB")

TABLES['event_LTE_NAS_ESM_OTA_Outgoing_Packet'] = (
        "CREATE TABLE IF NOT EXISTS `event_LTE_NAS_ESM_OTA_Outgoing_Packet` ("
        "  `user_id` int(11)  unsigned NOT NULL,"
        "  `time` bigint NOT NULL,"
        "  `trust` tinyint DEFAULT NULL,"
        "  UNIQUE KEY (`user_id`, `time`),"
        "  KEY (`user_id`),"
        "  KEY (`time`)"
        ") ENGINE=InnoDB")

TABLES['event_LTE_NAS_ESM_State'] = (
        "CREATE TABLE IF NOT EXISTS `event_LTE_NAS_ESM_State` ("
        "  `user_id` int(11)  unsigned NOT NULL,"
        "  `time` bigint NOT NULL,"
        "  `eps_bearer_type` smallint NOT NULL,"
        "  `eps_bearer_state` smallint NOT NULL,"
        "  `trust` tinyint DEFAULT NULL,"
        "  UNIQUE KEY (`user_id`, `time`),"
        "  KEY (`user_id`),"
        "  KEY (`time`)"
        ") ENGINE=InnoDB")

TABLES['event_WCDMA_RRC_OTA_Packet'] = (
        "CREATE TABLE IF NOT EXISTS `event_WCDMA_RRC_OTA_Packet` ("
        "  `user_id` int(11)  unsigned NOT NULL,"
        "  `time` bigint NOT NULL,"
        "  `rrc_state_id` smallint unsigned NOT NULL,"
        "  `trust` tinyint DEFAULT NULL,"
        "  UNIQUE KEY (`user_id`, `time`),"
        "  KEY (`user_id`),"
        "  KEY (`time`)"
        ") ENGINE=InnoDB")

TABLES['wcdma_rrc_state'] = (
        "CREATE TABLE IF NOT EXISTS `wcdma_rrc_state` ("
        "  `id` smallint  unsigned NOT NULL AUTO_INCREMENT,"
        "  `value` char(128) NOT NULL,"
        "  PRIMARY KEY (`id`),"
        "  UNIQUE KEY (`value`)"
        ") ENGINE=InnoDB")

INITIAL_SQLS['insert_wcdma_rrc_state'] = (
        "insert ignore into wcdma_rrc_state (`id`, `value`) values "
        "(1, 'completeSIB-List'),"
        "(2, 'firstSegment'),"
        "(3, 'subsequentSegment'),"
        "(4, 'lastSegmentShort'),"
        "(5, 'noSegment')"
        )

TABLES['event_WCDMA_RRC_Serv_Cell_Info'] = (
        "CREATE TABLE IF NOT EXISTS `event_WCDMA_RRC_Serv_Cell_Info` ("
        "  `user_id` int(11)  unsigned NOT NULL,"
        "  `time` bigint NOT NULL,"
        "  `cid` int unsigned NOT NULL,"
        "  `psc` int unsigned NOT NULL,"
        "  `plmn` char(16) NOT NULL,"
        "  `lac` int unsigned NOT NULL,"
        "  `rac` int unsigned NOT NULL,"
        "  `trust` tinyint DEFAULT NULL,"
        "  UNIQUE KEY (`user_id`, `time`),"
        "  KEY (`user_id`),"
        "  KEY (`time`)"
        ") ENGINE=InnoDB")

TABLES['event_UMTS_NAS_GMM_State'] = (
        "CREATE TABLE IF NOT EXISTS `event_UMTS_NAS_GMM_State` ("
        "  `user_id` int(11)  unsigned NOT NULL,"
        "  `time` bigint NOT NULL,"
        "  `gmm_state_id` smallint unsigned NOT NULL,"
        "  `gmm_substate_id` smallint unsigned NOT NULL,"
        "  `gmm_update_status_id` smallint unsigned NOT NULL,"
        "  `trust` tinyint DEFAULT NULL,"
        "  UNIQUE KEY (`user_id`, `time`),"
        "  KEY (`user_id`),"
        "  KEY (`time`)"
        ") ENGINE=InnoDB")

TABLES['gmm_state'] = (
        "CREATE TABLE IF NOT EXISTS `gmm_state` ("
        "  `id` smallint  unsigned NOT NULL AUTO_INCREMENT,"
        "  `value` char(128) NOT NULL,"
        "  PRIMARY KEY (`id`),"
        "  UNIQUE KEY (`value`)"
        ") ENGINE=InnoDB")

TABLES['gmm_substate'] = (
        "CREATE TABLE IF NOT EXISTS `gmm_substate` ("
        "  `id` smallint  unsigned NOT NULL AUTO_INCREMENT,"
        "  `value` char(128) NOT NULL,"
        "  PRIMARY KEY (`id`),"
        "  UNIQUE KEY (`value`)"
        ") ENGINE=InnoDB")

TABLES['gmm_update_status'] = (
        "CREATE TABLE IF NOT EXISTS `gmm_update_status` ("
        "  `id` smallint  unsigned NOT NULL AUTO_INCREMENT,"
        "  `value` char(128) NOT NULL,"
        "  PRIMARY KEY (`id`),"
        "  UNIQUE KEY (`value`)"
        ") ENGINE=InnoDB")

TABLES['event_UMTS_NAS_MM_State'] = (
        "CREATE TABLE IF NOT EXISTS `event_UMTS_NAS_MM_State` ("
        "  `user_id` int(11)  unsigned NOT NULL,"
        "  `time` bigint NOT NULL,"
        "  `mm_state_id` smallint unsigned NOT NULL,"
        "  `mm_substate_id` smallint unsigned NOT NULL,"
        "  `mm_update_status_id` smallint unsigned NOT NULL,"
        "  `trust` tinyint DEFAULT NULL,"
        "  UNIQUE KEY (`user_id`, `time`),"
        "  KEY (`user_id`),"
        "  KEY (`time`)"
        ") ENGINE=InnoDB")

TABLES['mm_state'] = (
        "CREATE TABLE IF NOT EXISTS `mm_state` ("
        "  `id` smallint  unsigned NOT NULL AUTO_INCREMENT,"
        "  `value` char(128) NOT NULL,"
        "  PRIMARY KEY (`id`),"
        "  UNIQUE KEY (`value`)"
        ") ENGINE=InnoDB")

TABLES['mm_substate'] = (
        "CREATE TABLE IF NOT EXISTS `mm_substate` ("
        "  `id` smallint  unsigned NOT NULL AUTO_INCREMENT,"
        "  `value` char(128) NOT NULL,"
        "  PRIMARY KEY (`id`),"
        "  UNIQUE KEY (`value`)"
        ") ENGINE=InnoDB")

TABLES['mm_update_status'] = (
        "CREATE TABLE IF NOT EXISTS `mm_update_status` ("
        "  `id` smallint  unsigned NOT NULL AUTO_INCREMENT,"
        "  `value` char(128) NOT NULL,"
        "  PRIMARY KEY (`id`),"
        "  UNIQUE KEY (`value`)"
        ") ENGINE=InnoDB")

TABLES['event_UMTS_NAS_MM_REG_State'] = (
        "CREATE TABLE IF NOT EXISTS `event_UMTS_NAS_MM_REG_State` ("
        "  `user_id` int(11)  unsigned NOT NULL,"
        "  `time` bigint NOT NULL,"
        "  `net_op_mode` smallint NOT NULL,"
        "  `plmn` char(16) NOT NULL,"
        "  `lac` int NOT NULL,"
        "  `rac` int NOT NULL,"
        "  `trust` tinyint DEFAULT NULL,"
        "  UNIQUE KEY (`user_id`, `time`),"
        "  KEY (`user_id`),"
        "  KEY (`time`)"
        ") ENGINE=InnoDB")

TABLES['event_UMTS_NAS_OTA_Packet'] = (
        "CREATE TABLE IF NOT EXISTS `event_UMTS_NAS_OTA_Packet` ("
        "  `user_id` int(11)  unsigned NOT NULL,"
        "  `time` bigint NOT NULL,"
        "  `trust` tinyint DEFAULT NULL,"
        "  UNIQUE KEY (`user_id`, `time`),"
        "  KEY (`user_id`),"
        "  KEY (`time`)"
        ") ENGINE=InnoDB")

TABLES['file_timestamp'] = (
        "CREATE TABLE IF NOT EXISTS `file_timestamp` ("
        "  `file_path` char(255) DEFAULT NULL,"
        "  `file_name` char(255) NOT NULL,"
        "  `user_id` int(11) unsigned NOT NULL,"
        "  `start_time` bigint NOT NULL,"
        "  `end_time` bigint NOT NULL,"
        "  PRIMARY KEY (`file_path`, `file_name`, `user_id`)"
        ") ENGINE=InnoDB")

TABLES['mi_log_sync'] = (
        "CREATE TABLE IF NOT EXISTS `mi_log_sync` ("
        "  `user_id` int(11) unsigned NOT NULL,"
        "  `file_path` char(255) NOT NULL,"
        "  `sync_time` int NOT NULL,"
        "  UNIQUE KEY (`file_path`)"
        ") ENGINE=InnoDB")

TABLES['mylog_running_time'] = (
        "CREATE TABLE IF NOT EXISTS `mylog_running_time` ("
        "  `user_id` int(11)  unsigned NOT NULL,"
        "  `start_time` bigint NOT NULL,"
        "  `end_time` bigint NOT NULL,"
        "  UNIQUE KEY (`user_id`, `start_time`),"
        "  KEY (`user_id`),"
        "  KEY (`start_time`),"
        "  KEY (`end_time`)"
        ") ENGINE=InnoDB")

TABLES['milog_running_time'] = (
        "CREATE TABLE IF NOT EXISTS `milog_running_time` ("
        "  `user_id` int(11)  unsigned NOT NULL,"
        "  `start_time` bigint NOT NULL,"
        "  `end_time` bigint NOT NULL,"
        "  UNIQUE KEY (`user_id`, `start_time`),"
        "  KEY (`user_id`),"
        "  KEY (`start_time`),"
        "  KEY (`end_time`)"
        ") ENGINE=InnoDB")

TABLES['running_time'] = (
        "CREATE TABLE IF NOT EXISTS `running_time` ("
        "  `user_id` int(11)  unsigned NOT NULL,"
        "  `start_time` bigint NOT NULL,"
        "  `end_time` bigint NOT NULL,"
        "  UNIQUE KEY (`user_id`, `start_time`),"
        "  KEY (`user_id`),"
        "  KEY (`start_time`),"
        "  KEY (`end_time`)"
        ") ENGINE=InnoDB")

TABLES['event_milog'] = (
        "CREATE TABLE IF NOT EXISTS `event_milog` ("
        "  `user_id` int(11)  unsigned NOT NULL,"
        "  `time` bigint NOT NULL,"
        "  `on` tinyint NOT NULL,"
        "  UNIQUE KEY (`user_id`, `time`),"
        "  KEY (`user_id`),"
        "  KEY (`time`)"
        ") ENGINE=InnoDB")

TABLES['event_tcpdumplog'] = (
        "CREATE TABLE IF NOT EXISTS `event_tcpdumplog` ("
        "  `user_id` int(11)  unsigned NOT NULL,"
        "  `time` bigint NOT NULL,"
        "  `on` tinyint NOT NULL,"
        "  UNIQUE KEY (`user_id`, `time`),"
        "  KEY (`user_id`),"
        "  KEY (`time`)"
        ") ENGINE=InnoDB")

TABLES['mi2log_files'] = (
        "CREATE TABLE IF NOT EXISTS `mi2log_files` ("
        "  `file_name` char(255) NOT NULL,"
        "  PRIMARY KEY (`file_name`)"
        ") ENGINE=InnoDB")

TABLES['emm_states'] = (
        "CREATE TABLE IF NOT EXISTS `emm_states` ("
        "  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,"
        "  `value` char(255) NOT NULL,"
        "  PRIMARY KEY (`id`),"
        "  UNIQUE KEY (`value`)"
        ") ENGINE=InnoDB AUTO_INCREMENT=1")

TABLES['emm_substates'] = (
        "CREATE TABLE IF NOT EXISTS `emm_substates` ("
        "  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,"
        "  `value` char(255) NOT NULL,"
        "  PRIMARY KEY (`id`),"
        "  UNIQUE KEY (`value`)"
        ") ENGINE=InnoDB AUTO_INCREMENT=1")

INITIAL_SQLS['insert_emm_states'] = '''INSERT IGNORE INTO `emm_states` VALUES 
(1,'EMM_REGISTERED'),
(2,'EMM_REGISTERED_INITIATED'),
(3,'EMM_DEREGISTERED'),
(4,'EMM_DEREGISTERED_INITIATED'),
(5,'EMM_SERVICE_REQUEST_INITIATED'),
(6,'EMM_TRACKING_AREA_UPDATING_INITIATED')
'''
INITIAL_SQLS['insert_emm_substates'] = '''INSERT IGNORE INTO `emm_substates` VALUES 
(1,'EMM_REGISTERED_NORMAL_SERVICE'),
(2,'EMM_REGISTERED_LIMITED_SERVICE'),
(3,'EMM_REGISTERED_PLMN_SEARCH'),
(4,'EMM_REGISTERED_NO_CELL_AVAILABLE'),
(5,'EMM_REGISTERED_UPDATE_NEEDED'),
(6,'EMM_REGISTERED_ATTEMPTING_TO_UPDATE'),
(7,'EMM_REGISTERED_ATTEMPTING_TO_UPDATE_MM'),

(8,'EMM_DEREGISTERED_NORMAL_SERVICE'),
(9,'EMM_DEREGISTERED_LIMITED_SERVICE'),
(10,'EMM_DEREGISTERED_PLMN_SEARCH'),
(11,'EMM_DEREGISTERED_NO_CELL_AVAILABLE'),
(12,'EMM_DEREGISTERED_ATTACH_NEEDED'),
(13,'EMM_DEREGISTERED_ATTEMPTING_TO_ATTACH'),
(14,'EMM_DEREGISTERED_NO_IMSI'),

(15,'EMM_WAITING_FOR_ESM_RESPONSE'),
(16,'EMM_WAITING_FOR_NW_RESPONSE'),

(17,'Unknown'),
(18,'Undefined')
'''

TABLES['messages'] = (
        "CREATE TABLE IF NOT EXISTS `messages` ("
        "  `user_id` int(11) unsigned NOT NULL,"
        "  `time` bigint NOT NULL,"
        "  `message_type_id` int NOT NULL,"
        "  `rrc_state` char(50) DEFAULT NULL,"
        "  `signal_strength` float DEFAULT 0,"
        "  `extra` text DEFAULT NULL,"
        "  `trust` tinyint DEFAULT NULL,"
        "  `emm_state_id` smallint DEFAULT NULL,"
        "  `emm_substate_id` smallint DEFAULT NULL,"
        "  UNIQUE KEY (`user_id`, `time`, `message_type_id`),"
        "  KEY (`user_id`),"
        "  KEY (`time`),"
        "  KEY (`message_type_id`),"
        "  KEY (`rrc_state`),"
        "  KEY (`emm_state_id`),"
        "  KEY (`emm_substate_id`)"
        ") ENGINE=InnoDB")

TABLES['message_types'] = (
        "CREATE TABLE IF NOT EXISTS `message_types` ("
        "  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,"
        "  `message_type` char(255) NOT NULL,"
        "  `message_sub_type` char(255) DEFAULT '',"
        "  PRIMARY KEY (`id`),"
        "  UNIQUE KEY (`message_type`, `message_sub_type`)"
        ") ENGINE=InnoDB AUTO_INCREMENT=1")

TABLES['events_velocity'] = (
        "CREATE TABLE IF NOT EXISTS `events_velocity` ("
        "  `user_id` int(11) unsigned NOT NULL,"
        "  `start_time` bigint NOT NULL,"
        "  `end_time` bigint NOT NULL,"
        "  `velocity` float DEFAULT -1.0,"
        "  UNIQUE KEY (`user_id`, `start_time`),"
        "  KEY (`user_id`),"
        "  KEY (`start_time`),"
        "  KEY (`end_time`)"
        ") ENGINE=InnoDB")

TABLES['nas_procedures'] = (
        "CREATE TABLE IF NOT EXISTS `nas_procedures` ("
        "  `user_id` int(11) unsigned NOT NULL,"
        "  `start_time` bigint NOT NULL,"
        "  `end_time` bigint NOT NULL,"
        "  `procedure_type` int(11) NOT NULL,"
        "  `status` int(11) NOT NULL,"
        "  `message_sequence` text DEFAULT NULL,"
        "  `message_direction` text DEFAULT NULL,"
        "  `message_time` text DEFAULT NULL,"
        "  `extra` text DEFAULT NULL,"
        "  UNIQUE KEY (`user_id`, `start_time`, `end_time`),"
        "  KEY (`user_id`),"
        "  KEY (`start_time`),"
        "  KEY (`end_time`)"
        ") ENGINE=InnoDB")

INITIAL_SQLS['insert_message_types'] = '''INSERT IGNORE INTO `message_types` VALUES 
(1,'LTE_LL1_PCFICH_Decoding_Results',''),
(2,'LTE_PHY_PDSCH_Packet',''),
(3,'LTE_MAC_DL_Transport_Block',''),
(4,'LTE_MAC_UL_Transport_Block',''),
(5,'LTE_RLC_DL_AM_All_PDU',''),
(6,'LTE_RLC_UL_AM_All_PDU',''),
(7,'LTE_PHY_Serv_Cell_Measurement',''),
(8,'LTE_PHY_Connected_Mode_Neighbor_Measurement',''),
(9,'LTE_PHY_Connected_Mode_Intra_Freq_Meas',''),
(10,'LTE_PHY_Inter_RAT_Measurement',''),
(11,'LTE_RRC_Serv_Cell_Info',''),
(12,'LTE_RRC_MIB_Packet',''),
(13,'LTE_RRC_MIB_Message_Log_Packet',''),
(14,'LTE_RRC_OTA_Packet','measurementReport'),
(15,'LTE_RRC_OTA_Packet','PCCH-Message'),
(16,'LTE_RRC_OTA_Packet','SIB1'),
(17,'LTE_RRC_OTA_Packet','SIBN'),
(18,'LTE_RRC_OTA_Packet','rrcConnectionRequest'),
(19,'LTE_RRC_OTA_Packet','rrcConnectionSetup'),
(20,'LTE_RRC_OTA_Packet','rrcConnectionSetupComplete'),
(21,'LTE_RRC_OTA_Packet','securityModeCommand'),
(22,'LTE_RRC_OTA_Packet','securityModeComplete'),
(23,'LTE_RRC_OTA_Packet','rrcConnectionReconfiguration'),
(24,'LTE_RRC_OTA_Packet','rrcConnectionReconfigurationComplete'),
(25,'LTE_RRC_OTA_Packet','dlInformationTransfer'),
(26,'LTE_RRC_OTA_Packet','ulInformationTransfer'),
(27,'LTE_RRC_OTA_Packet','ueCapabilityEnquiry'),
(28,'LTE_RRC_OTA_Packet','ueCapabilityInformation'),
(29,'LTE_RRC_OTA_Packet','rrcConnectionReestablishmentRequest'),
(30,'LTE_RRC_OTA_Packet','rrcConnectionReestablishment'),
(31,'LTE_RRC_OTA_Packet','rrcConnectionReestablishmentComplete'),
(32,'LTE_RRC_OTA_Packet','mobilityFromEUTRACommand'),
(33,'LTE_RRC_OTA_Packet','rrcConnectionReject'),
(34,'LTE_RRC_OTA_Packet','rrcConnectionReestablishmentReject'),
(35,'LTE_RRC_OTA_Packet','rrcConnectionRelease'),
(36,'LTE_NAS_EMM_OTA_Outgoing_Packet',''),
(37,'LTE_NAS_EMM_OTA_Incoming_Packet',''),
(38,'LTE_NAS_ESM_OTA_Outgoing_Packet',''),
(39,'LTE_NAS_ESM_OTA_Incoming_Packet',''),
(40,'LTE_NAS_ESM_State',''),
(41,'LTE_NAS_EMM_State','EMM_REGISTERED:EMM_REGISTERED_NORMAL_SERVICE'),
(42,'LTE_NAS_EMM_State','EMM_SERVICE_REQUEST_INITIATED:EMM_REGISTERED_NORMAL_SERVICE'),
(43,'LTE_MAC_Rach_Trigger',''),
(44,'LTE_MAC_Rach_Attempt',''),
(45,'LTE_MAC_Configuration',''),
(46,'LTE_RLC_DL_Config_Log_Packet',''),
(47,'LTE_RLC_UL_Config_Log_Packet',''),
(48,'LTE_NAS_EMM_State','EMM_REGISTERED:EMM_REGISTERED_NO_CELL_AVAILABLE'),
(49,'LTE_ML1_System_Scan_Results','')
'''

TABLES['nas_msg_emm_types'] = (
        "CREATE TABLE IF NOT EXISTS `nas_msg_emm_types` ("
        "  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,"
        "  `value` char(255) NOT NULL,"
        "  PRIMARY KEY (`id`),"
        "  UNIQUE KEY (`value`)"
        ") ENGINE=InnoDB AUTO_INCREMENT=1")
TABLES['nas_eps_emm_service_types'] = (
        "CREATE TABLE IF NOT EXISTS `nas_eps_emm_service_types` ("
        "  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,"
        "  `value` char(255) NOT NULL,"
        "  PRIMARY KEY (`id`),"
        "  UNIQUE KEY (`value`)"
        ") ENGINE=InnoDB AUTO_INCREMENT=1")
TABLES['nas_eps_security_header_types'] = (
        "CREATE TABLE IF NOT EXISTS `nas_eps_security_header_types` ("
        "  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,"
        "  `value` char(255) NOT NULL,"
        "  PRIMARY KEY (`id`),"
        "  UNIQUE KEY (`value`)"
        ") ENGINE=InnoDB AUTO_INCREMENT=1")

TABLES['nas_eps_emm_EPS_attach_results'] = (
        "CREATE TABLE IF NOT EXISTS `nas_eps_emm_EPS_attach_results` ("
        "  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,"
        "  `value` char(255) NOT NULL,"
        "  PRIMARY KEY (`id`),"
        "  UNIQUE KEY (`value`)"
        ") ENGINE=InnoDB AUTO_INCREMENT=1")

TABLES['nas_msg_esm_types'] = (
        "CREATE TABLE IF NOT EXISTS `nas_msg_esm_types` ("
        "  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,"
        "  `value` char(255) NOT NULL,"
        "  PRIMARY KEY (`id`),"
        "  UNIQUE KEY (`value`)"
        ") ENGINE=InnoDB AUTO_INCREMENT=1")

DB_NAME = 'modem'

class Database:

    def __init__(self, user='root', password='', host='localhost', unix_socket=None, db_name = DB_NAME):
        self.user = user
        self.password = password
        self.host = host
        self.unix_socket = unix_socket
        self.db_name = db_name
        if unix_socket is None:
            self.cnx = mysql.connector.connect(user=self.user, password=self.password,
                host=self.host)
        else:
            self.cnx = mysql.connector.connect(user=self.user, password=self.password,
                host=self.host, unix_socket=self.unix_socket)
        self.cursor = self.cnx.cursor()
        self.select_database()
        # print TABLES['nas_msg_esm_types']
        
    """
    Close the database instance. 
    """
    def close(self):
        self.cursor.close()
        self.cnx.commit()
        self.cnx.close()

    """
    Select database as the modem databaes (DB_NAME).
    If the database does not exist, create and 
    initiate the database.
    """
    def select_database(self):
        cnx = self.cnx
        cursor = self.cursor
        try:
            cnx.database = self.db_name
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_BAD_DB_ERROR:
                try:
                    cursor.execute("CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
                    self.init_database()
                except mysql.connector.Error as err:
                    logging.error("Failed creating database: {}".format(err))
                    cursor.close()
                    return False
                cnx.database = self.db_name
            else:
                logging.error(err)
                return False
    """
    Initiate the database using the database schema.
    """
    def init_database(self):
        cnx = self.cnx
        cursor = self.cursor
        cnx.database = self.db_name
        for name, ddl in TABLES.iteritems():
            try:
                logging.info("Creating table {}: ".format(name))
                cursor.execute(ddl)
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                    logging.error("already exists.")
                else:
                    logging.error(err.msg)
            else:
                logging.info("OK")
        cnx.commit()
        for name, ddl in INITIAL_SQLS.iteritems():
            try:
                logging.info("Initiate table {}: ".format(name))
                cursor.execute(ddl)
            except mysql.connector.Error as err:
                logging.error(err.msg)
            else:
                logging.info("OK")
        cnx.commit()
        return True

    """
    Get the databse cursor.
    """
    def get_cursor(self):
        if self.cnx is None:
            return None
        return self.cnx.cursor()

    """
    Select from databse accordign to the provided sql command 
    and only return the first row in the result.
    This function works for the sql which only has one row result.
    """
    def select_single_row(self, sql):
        cursor = self.cursor
        try:
            cursor.execute(sql)
        except mysql.connector.Error as err:
            logging.error(err.msg + ":" + sql)
        row = cursor.fetchone()
        if row is None:
            logging.error("row is None:" + sql)
            return None
        result = []
        for i in range(0, len(row)):
            if row[i] is None:
                result.append(0)
            else:
                result.append(row[i])
        return result

    """
    Select from databse accordign to the provided sql command 
    and only return all the rows in the result.
    """
    def select_multiple_row(self, sql):
        cursor = self.cursor
        try:
            cursor.execute(sql)
        except mysql.connector.Error as err:
            logging.error(err.msg + ":" + sql)
        rows = cursor.fetchall()
        if rows is None:
            logging.error("rows is None:" + sql)
            return []
        return rows

    """
    Insert values into table starting with 'event_'.
    """
    def insert_to_event_table(self, table, namevalues, exclude = []):
        name_str = ""
        value_str = ""
        with_value = False
        for name, value in namevalues.iteritems():
            if name in exclude:
                continue
            with_value = True
            if name in ['time', 'start_time', 'end_time']:
                value = "%.0f" % value
            name_str = ''' `%s`,''' % (name) + name_str
            value_str = ''' "%s",''' % (value) + value_str
        if with_value == False:
            return False
        name_str = name_str[:-1] 
        value_str = value_str[:-1]
        sql = '''insert ignore into `%s` (%s) values (%s)''' % (table, name_str, value_str)
        try:
            self.cursor.execute(sql)
            #self.cnx.commit()
        except mysql.connector.Error as err:
            logging.error(err.msg + ":" + sql)
            return False
        return True

    """
    Replace values into table starting with 'event_'.
    """
    def replace_to_event_table(self, table, namevalues, exclude = []):
        name_str = ""
        value_str = ""
        with_value = False
        for name, value in namevalues.iteritems():
            if name in exclude:
                continue
            with_value = True
            if name in ['time', 'start_time', 'end_time']:
                value = "%.0f" % value
            name_str = ''' `%s`,''' % (name) + name_str
            value_str = ''' "%s",''' % (value) + value_str
        if with_value == False:
            return False
        name_str = name_str[:-1] 
        value_str = value_str[:-1]
        sql = '''replace into `%s` (%s) values (%s)''' % (table, name_str, value_str)
        try:
            self.cursor.execute(sql)
            #self.cnx.commit()
        except mysql.connector.Error as err:
            logging.error(err.msg + ":" + sql)
            return False
        return True

    """
    Update values in table starting with 'event_'.
    """
    def update_to_event_table(self, table, namevalues, where_conditions):
        update_str = ""
        where_str = ""
        with_value = False
        for name, value in namevalues.iteritems():
            with_value = True
            if name in ['time', 'start_time', 'end_time']:
                value = "%.0f" % value
            update_str = ''' `%s` = "%s",''' % (name, value) + update_str
        for name, value in where_conditions.iteritems():
            if name in ['time', 'start_time', 'end_time']:
                value = "%.0f" % value
            where_str = ''' `%s` = "%s" and''' % (name, value) + where_str
        if with_value == False:
            return False
        update_str = update_str[:-1]
        where_str = where_str[:-3]
        sql = '''update `%s` set %s where %s''' % (table, update_str, where_str)
        try:
            self.cursor.execute(sql)
            #self.cnx.commit()
        except mysql.connector.Error as err:
            logging.error(err.msg + ":" + sql)
            return False
        return True

    """
    General execution without output.
    """
    def execute(self, sql):
        try:
            self.cursor.execute(sql)
            #self.cnx.commit()
        except mysql.connector.Error as err:
            logging.error(err.msg + ":" + sql)
            return False
        return True


    """
    Get id value from table which has a column called id, with specific conditions.
    """
    def get_id(self, table, conditions, exclude = []):
        cnx = self.cnx
        cursor = self.cursor
        if len(conditions) - len(exclude) <= 0:
            where_str = ''
        else:
            where_str = 'where '
            for name, value in conditions.iteritems():
                if name in exclude:
                    continue
                where_str = where_str + ''' `%s` = '%s' and ''' % (name, value)
            where_str = where_str[:-4] 
        sql = '''select id from %s %s''' % (table, where_str)
        try:
            cursor.execute(sql)
        except mysql.connector.Error as err:
            logging.error(err.msg + ":" + sql)
            return -1
        rows = cursor.fetchall()
        if len(rows) == 0:
            if len(conditions) > 0:
                name_str = ""
                value_str = ""
                for name, value in conditions.iteritems():
                    if name in exclude:
                        continue
                    name_str = ''' `%s`,''' % (name) + name_str
                    value_str = ''' '%s',''' % (value) + value_str
                name_str = name_str[:-1] 
                value_str = value_str[:-1]
                sql = '''insert ignore into `%s` (%s) values (%s)''' % (table, name_str, value_str)
                try:
                    cursor.execute(sql)
                    cnx.commit()
                except mysql.connector.Error as err:
                    logging.error(err.msg + ":" + sql)
                    return -1
                return cursor.lastrowid
        else:
            return rows[0][0]


    def delete_user(self, user_id):
        cnx = self.cnx
        cursor = self.cursor

        sql = "show tables where Tables_in_modem like 'event%'"
        try:
            cursor.execute(sql)
        except mysql.connector.Error as err:
            logging.error(err.msg + ":" + sql)
            return
        rows = cursor.fetchall()
        if rows is None:
            logging.error("rows is None:" + sql)
            return
        for row in rows:
            table_name = row[0]
            cursor.execute('delete from %s where user_id = %d' % (table_name, user_id))
        cursor.execute('delete from user where id = %d' % user_id)
        cnx.commit()

    def commit(self):
        self.cnx.commit()




