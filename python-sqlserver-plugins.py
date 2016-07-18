#!/usr/bin/python
# -*- coding: utf-8 -*-
# Version : 1.0

import argparse 
from argparse import RawTextHelpFormatter
import pypyodbc
import sys
import csv
class SmartFormatter(argparse.HelpFormatter):

    def _split_lines(self, text, width):
        if text.startswith('R|'):
            return text[2:].splitlines()  
        # this is the RawTextHelpFormatter._split_lines
        return argparse.HelpFormatter._split_lines(self, text, width)

def GetArgs():
    #parser = ArgumentParser(description='test', formatter_class=SmartFormatter)
    parser = argparse.ArgumentParser(
        description="Nagios-like plugin to check SQLSERVER database.\n "
        "This plugin check connect to pre-installed database which contains stored procedures" , formatter_class=SmartFormatter)
    parser.add_argument("-H", "--host", action="store",
                        help="Server name")
    parser.add_argument("-d", "--dbsup", action="store", default="db_sup",help="R|Database name containing stored procedures")
    parser.add_argument("-D", "--db", action="store", default="DEFAULT",
                        help="DB name to check for : mode DBUSAGE")
    parser.add_argument("-p", "--port", action="store", default=1433,
                        help="SQLSERVER PORT")
    parser.add_argument("-A", "--authfile", action="store",
                        help="Authentification file format user:pass")
    parser.add_argument("-f", "--driver", action="store", default="FreeTDS",
                        help="Drivers /etc/odbcinst.ini")
    parser.add_argument("-m", "--mode", action="store",
                        help="R|Test SQL: \n"
         " BCKUP = Last Backup Status -M for minutes and -T for backup Type\n"
         " DBUSAGE = DB Space -w warning -c critical -D dbname\n"
         " ERRORLOG = xxxxxx "
         )
    parser.add_argument("-M", "--minutes", action="store", default="DEFAULT",
                         help="Nombre de minutes Utilisé avec mode BACKUP")
    parser.add_argument("-T", "--bcktype", action="store", default="DEFAULT",
                         help="Type de Backup F pour FULL , D pour DIFF et  L pour LOGS")
    args = parser.parse_args()

    return args



class db():
    def __init__(self, driver, host, port, user, password, base):
        dbcnx = pypyodbc.connect(driver=driver, server=host, port=port, uid=user, pwd=password, database=base)
        self.cur = dbcnx.cursor()

    def req_missing_backup(self, minutes, bcktype):
        query = "EXEC [DB_Cheops].dbo.ps_CheckDatabasesWithoutBackupOnPeriod @nbminutes=" + minutes + ",@backuptype=" + bcktype
    	self.cur.execute(query)
    	RESULT = self.cur.fetchone()
        STATUS = str(RESULT[0])
        OUTPUT = str(RESULT[1])
        PERFDATAS = str(RESULT[2])
        print OUTPUT + "|" + PERFDATAS
        EXIT = sys.exit( int(STATUS) )
        return EXIT

    def req_dbusage(self, database, warn , crit):
        query = "EXEC [DB_Cheops].dbo.ps_CheckDatabasesWithoutBackupOnPeriod @nbminutes=" + minutes + ",@backuptype=" + bcktype
        self.cur.execute(query)
        RESULT = self.cur.fetchone()
        STATUS = str(RESULT[0])
        OUTPUT = str(RESULT[1])
        PERFDATAS = str(RESULT[2])
        print OUTPUT + "|" + PERFDATAS
        EXIT = sys.exit( int(STATUS) )
        return EXIT
    	
def main():
    args = GetArgs()
    host = str(args.host)
    port = str(args.port)
    database_sup = str(args.dbsup)
    database = str(args.db)
    authfile = str(args.authfile)
    driver = str(args.driver)
    mode = str(args.mode)
    minutes = str(args.minutes)
    bcktype = str(args.bcktype)
    file = open(authfile,"rb")
    try:
        reader = csv.reader(file)
        for row in reader:
            user = row[0]
            passwd = row[1]
    finally:
        file.close()
    db_connect = db(driver, host, port, user, passwd, database_sup)
    if mode == "BACKUP":
            db_connect.req_missing_backup(minutes, bcktype)
    if mode == "DBUSAGE":
        db_connect.req_dbusage(warn, crit)

 
if __name__ == "__main__":
    main()

