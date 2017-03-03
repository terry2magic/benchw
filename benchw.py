# -- coding: utf-8 --

import os
import unittest
import timeit
import ConfigParser
import datetime
import logging

config_file="./benchw.conf"
log_file="./benchw_test.log"

dbcmd_template={ "PostgresSQL" : \
    { "InitDB": ['psql -c "drop database %(dbname)s"', 
                 'psql -c "drop tablespace %(ts_name)s"', 
                 '''psql -c "create tablespace %(ts_name)s location '%(ts_path)s'" ''', 
                 'psql -c "create database %(dbname)s template template0 tablespace %(ts_name)s" ' ], 
       "CreateTable": "psql -d %(dbname)s -f %(script_path)s/schema.sql",
       "LoadData": [ '''psql -d %(dbname)s -c "COPY dim0 FROM '%(script_path)s/dim0.dat' USING DELIMITERS ',' " ''',
                                 '''psql -d %(dbname)s -c "COPY dim1 FROM '%(script_path)s/dim1.dat' USING DELIMITERS ','" ''',
                                 '''psql -d %(dbname)s -c "COPY dim2 FROM '%(script_path)s/dim2.dat' USING DELIMITERS ','" ''',
                                 '''psql -d %(dbname)s -c "COPY fact0 FROM '%(script_path)s/fact0.dat' USING DELIMITERS ','" '''  ], 
        "CreateIndex": "psql -d %(dbname)s -f %(script_path)s/indexes.sql", 
        "OptimizeTable": "psql -d %(dbname)s -f %(script_path)s/analyze.sql", 
        "Query0": "psql -d %(dbname)s -f %(script_path)s/qtype0.sql", 
        "Query1": "psql -d %(dbname)s -f %(script_path)s/qtype1.sql", 
        "Query2": "psql -d %(dbname)s -f %(script_path)s/qtype2.sql", 
        "Query3": "psql -d %(dbname)s -f %(script_path)s/qtype3.sql", 
        "Query4": "psql -d %(dbname)s -f %(script_path)s/qtype4.sql" 
    } ,
    "Oracle": \
    { "InitDB": '''sqlplus "/ as sysdba" << ACCOUNT_EOF
create user %(dbname)s identified by %(dbpassword)s;
alter user %(dbname)s default tablespace %(ts_name)s quota unlimited on %(ts_name)s;
grant connect, resource to %(dbname)s;
ACCOUNT_EOF''', 
      "CreateTable": "sqlplus %(dbuser)s/%(dbpassword)s @%(script_path)s/schema.sql",
      "LoadData": [ 'sqlldr userid=%(dbuser)s/%(dbpassword)s control=%(script_path)s/dim0.ctl > /dev/null ',
                    'sqlldr userid=%(dbuser)s/%(dbpassword)s control=%(script_path)s/dim1.ctl > /dev/null ',
                    'sqlldr userid=%(dbuser)s/%(dbpassword)s control=%(script_path)s/dim2.ctl > /dev/null ',
                    'sqlldr userid=%(dbuser)s/%(dbpassword)s control=%(script_path)s/fact0.ctl > /dev/null '  ], 
      "CreateIndex": "sqlplus %(dbuser)s/%(dbpassword)s @%(script_path)s/indexes.sql", 
      "OptimizeTable": "sqlplus %(dbuser)s/%(dbpassword)s @%(script_path)s/analyze.sql", 
      "Query0": "sqlplus %(dbuser)s/%(dbpassword)s @%(script_path)s/qtype0.sql", 
      "Query1": "sqlplus %(dbuser)s/%(dbpassword)s @%(script_path)s/qtype1.sql", 
      "Query2": "sqlplus %(dbuser)s/%(dbpassword)s @%(script_path)s/qtype2.sql", 
      "Query3": "sqlplus %(dbuser)s/%(dbpassword)s @%(script_path)s/qtype3.sql", 
      "Query4": "sqlplus %(dbuser)s/%(dbpassword)s @%(script_path)s/qtype4.sql" 
      },
    "Informix": \
    { "InitDB": "export DBDATE=Y4MD-", 
      "CreateTable": "dbaccess %(dbname)s %(script_path)s/schema.sql",
      "LoadData": [ 'dbload -d %(dbname)s -c %(script_path)s/dim0.ctl -l dbload.dim0 -e 10 -n 1000 -r ',
                    'dbload -d %(dbname)s -c %(script_path)s/dim1.ctl -l dbload.dim1 -e 10 -n 1000 -r ',
                    'dbload -d %(dbname)s -c %(script_path)s/dim2.ctl -l dbload.dim2 -e 10 -n 1000 -r ',
                    'dbload -d %(dbname)s -c %(script_path)s/fact0.ctl -l dbload.fact0 -e 10 -n 1000 -r '  ], 
      "CreateIndex": "dbaccess %(dbname)s %(script_path)s/indexes.sql", 
      "OptimizeTable": "dbaccess %(dbname)s %(script_path)s/analyze.sql", 
      "Query0": "dbaccess %(dbname)s %(script_path)s/qtype0.sql", 
      "Query1": "dbaccess %(dbname)s %(script_path)s/qtype1.sql", 
      "Query2": "dbaccess %(dbname)s %(script_path)s/qtype2.sql", 
      "Query3": "dbaccess %(dbname)s %(script_path)s/qtype3.sql", 
      "Query4": "dbaccess %(dbname)s %(script_path)s/qtype4.sql" 
      }    
}

logging.se


def exec_cmd(cmd):
    ''' Print the command and execute it , it also can redirect the output to somewhere than stdout'''
    print ("Execute command: %s"%cmd)
    os.system(cmd)
    
#script_path={"Oracle": "./ora/", "PostgresSQL":"./pg/", "DB2": "./db2/", "Informix": "./infx/", "SQLServer": ".\\sqlsvr\\", "Sybase": "./sybase/" }

class BenchwTest(unittest.TestCase):
    DBSteps={}
    DBConfig={}
    
    def _parseDBSteps(self):
        '''
        Parse the steps from config and template, generate the dict DBSteps that can use for the specific DB
        '''
        for step, cmds in dbcmd_template[self.db_type].items():
            if type(cmds) == list:  # Identify the cmds is a list
                parse_cmds=[]
                for cmd in cmds:
                    parse_cmds.append(cmd % self.DBConfig)
            else:  # It's a string
                parse_cmds=cmds % self.DBConfig
            # Use the parse cmds to fill in the DBSteps dict
            self.DBSteps[step]=parse_cmds
    
    def _readConf(self, conf_file):
        '''
        Read configuration from a file
        '''
        config=ConfigParser.ConfigParser()
        with open(conf_file, "rb") as conf_fp:
            config.readfp(conf_fp)
            self.db_type=config.get("db","dbtype")
            self.DBConfig['dbname']=config.get("db", "name")
            self.DBConfig['dbuser']=config.get("db","username")
            self.DBConfig['dbpassword']=config.get("db","password")
            self.DBConfig['ts_name']=config.get("tablespace","name")
            self.DBConfig['ts_path']=config.get("tablespace","path")
            self.DBConfig['script_path']=config.get("script","path")
            
            
    def setUp(self):
        self._readConf(config_file)
        self._parseDBSteps()
        self.InitDB()
        print("Benchw test start.\n")
        
    
    def InitDB(self):
        cmds=self.DBSteps['InitDB']
        if type(cmds) == list:
            for cmd in cmds:
                exec_cmd(cmd)
        else:
            exec_cmd(cmds)
 
           
    def CreateSchema(self):
        cmd=self.DBSteps['CreateTable'] 
        exec_cmd(cmd)
    
    def LoadData(self):
        cmds=self.DBSteps['LoadData']
        for cmd in cmds:
            exec_cmd(cmd)
    
    def CreateIndex(self):
        cmd=self.DBSteps['CreateIndex'] 
        print(cmd)
        exec_cmd(cmd)
    
    def OptimizeTable(self):
        cmd=self.DBSteps['OptimizeTable'] 
        exec_cmd(cmd)
    
    def Query0(self):
        cmd=self.DBSteps['Query0'] 
        exec_cmd(cmd)
            
    def Query1(self):
        cmd=self.DBSteps['Query1'] 
        os.system(cmd)
    
    def Query2(self):
        cmd=self.DBSteps['Query2'] 
        exec_cmd(cmd)
    
           
    def Query3(self):
        cmd=self.DBSteps['Query3'] 
        exec_cmd(cmd)
    
    def Query4(self):
        cmd=self.DBSteps['Query4'] 
        exec_cmd(cmd)

    def test_Benchw(self):
        steps=[]
        steps.append(timeit.Timer(self.CreateSchema))
        steps.append(timeit.Timer(self.LoadData))
        steps.append(timeit.Timer(self.CreateIndex))
        steps.append(timeit.Timer(self.OptimizeTable))
        steps.append(timeit.Timer(self.Query0))
        steps.append(timeit.Timer(self.Query1))
        steps.append(timeit.Timer(self.Query2))
        steps.append(timeit.Timer(self.Query3))
        steps.append(timeit.Timer(self.Query4))
        for teststep in steps:
            print teststep.timeit(1)
      
    def tearDown(self):
        print ("Benchw test finish.\n")
    
        
if __name__ == "__main__":
    unittest.main()
    

     

