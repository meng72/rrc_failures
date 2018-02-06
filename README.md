# rrc_failures

1. Database schema and APIs
    database.py includes database schema and APIs to init database, insert/select rows from tables.

    Important tables for rrc failures include 
        1) a set of tables named event_* to store modem messages.
        2) table named messages to store all the modem messages in one table for the efficiency.
        3) table named rrc_success which stores rrc establishment success message sequence.
        4) table named rrc_failure which stores rrc establishment failure message sequence.
           Note: each row in the table records the sequence that starts from the first RRC Conn Request which fails the establishment procedure
               and ends with the last RRC Conn Request which succeeds.
        5) table named rrc_failure_sections which stores rrc failure sections. Each row in table rrc_failure covers one or more rrc_failure sections
        6) table named message_types to store all the types of modem messages and corresponding message_type_id.


2. Parse decoded modem messages and insert to tables.
    parse_upload_decoded.py enables to parse modem messages and insert them into tables with the prefix event_.
    For example, LTE_RRC_OTA_Packet messages are stored in table event_LTE_RRC_OTA_Packet. 

    After initing database, run 'python parse_upload_decoded.py 0'

    Remember to change the file path in the file as you need.

3. Refine raw modem messages.
    test.py enables to 
        1) merge modem messages in event_* tables to one table named messages.
        2) insert into rrc_success, rrc_failure and rrc_failure_sections.
        3) get more information for root cause analysis, e.g. the timestamp of the last MIB packet before rrc_success/rrc_failure_sections

4. Analyze:
    rrc_failure.py is for further analysis.

    Important functions for analysis include:
        1) classify_rrc_failure_for_users: The classification rule is listed before the function.
