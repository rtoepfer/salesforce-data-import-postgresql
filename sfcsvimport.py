#!/usr/bin/env python3
import argparse
import logging
import os
import sys
import csv
import psycopg2
import io
import re
import base62
import unicodedata
import traceback


class Salesforce_to_PostgreSQL:

    # database
    _database = None

    def makeItPrintable(self, content):
        if str(content).isprintable():
            return content
        output = ""
        i = 0
        for line in content.splitlines():
            if not line.isprintable():
                newLine = ""
                for c in line:
                    if c.isprintable():
                        newLine += c
                line = newLine
            if i > 0:
                output += "\n"
            output += line
            i += 1
        return output

    def getFileContent(self, filePath):
        output = ""
        with open(filePath) as fp:
            output = fp.read()
        return output

    def escapeString(self, value):
        if value == "" or value == None:
            return ""
        try:
            value = value.decode()
        except (UnicodeDecodeError, AttributeError):
            pass
        strValue = str(value)        
        strValue = strValue.replace("\'", "''")
        return strValue

    def quoteTableOrColumn(self, value):
        if value == "" or value == None:
            raise Exception("Logic error")
        value = (str('"') + str(value) + str('"'))
        return value

    def insertData(self, tableName,fileContent,fields):
        
        logger.debug(' inserting data of %s', tableName)
        
        csvData = csv.DictReader(io.StringIO(fileContent))
        i = 0
        
        for row in csvData:
            sql = "INSERT INTO " + self.quoteTableOrColumn(tableName) + " "
            sqlCols = "(Id,sfId, "
            id = self.getSqlId(row["Id"])
            
            sqlVals = "VALUES (" + str(id) + ", '" + row["Id"]+ "', "
            for fieldName in csvData.fieldnames:
                if fieldName != "Id":
                    sqlCols += "" + self.quoteTableOrColumn(fieldName) + ", "
                    value = row[fieldName]
                    if value == "" or value == None:
                        value = "NULL"
                    
                    nonAposTypes = ["int","bool","float"]
                    
                    if value == "NULL" or fields[fieldName]["type"] in nonAposTypes:
                        sqlVals += value
                    else:
                        if fields[fieldName]["type"]=="id":
                            sqlVals += str(self.getSqlId(value))
                        elif fields[fieldName]["type"]=="datetime":
                            sqlVals += "'" + self.escapeString(value[:19]).decode("utf-8") + "'"
                        else:
                            sqlVals += "'" + self.escapeString(value).decode("utf-8") + "'"
                    sqlVals += ", "

            sql = sql + sqlCols[:-2] + ") " + sqlVals[:-2] + ");"
            try:
                #logger.debug(sql)
                cursor = self._database.cursor()
                cursor.execute(sql)
                self._database.commit()
                
            except (Exception) as e:
                logger.debug(sql)
                logger.error('error : %s', e)
                
                logger.error("Stack trace: ")
                traceback.print_tb(sys.exc_info()[2])

                sys.exit(-1)

            if args["test_data"] != None:
                if i >= args["test_data"]:
                    break
            i+=1

    def insertDataBulk(self, tableName, fileContent, fields):
        
        logger.debug(' inserting data of %s', tableName)
        
        csvData = csv.DictReader(io.StringIO(fileContent))
        i = 0
        sqlIns = "INSERT INTO " + self.quoteTableOrColumn(tableName) + " "
        sqlColsM = ""
        sqlValsM = ""
            
        for row in csvData:
            
            sqlCols = "(Id,sfId, "
            id = self.getSqlId(row["Id"])
            
            sqlVals = "(" + str(id) + ", '"+row["Id"]+ "', "
            for fieldName in csvData.fieldnames:
                if fieldName != "Id":
                    sqlCols += "" + self.quoteTableOrColumn(fieldName) + ", "
                    value = row[fieldName]
                    if value == "" or value == None:
                        value = "NULL"
                    
                    nonAposTypes = ["int","bool","float"]
                    
                    if value == "NULL" or fields[fieldName]["type"] in nonAposTypes:
                        sqlVals += value
                    else:
                        if fields[fieldName]["type"]=="id":
                            sqlVals += str(self.getSqlId(value))
                        elif fields[fieldName]["type"]=="datetime":
                            sqlVals += "'" + self.escapeString(value[:19]).decode("utf-8") + "'"
                        else:
                            v = unicodedata.normalize('NFKD', value).encode('ascii','ignore')
                            sqlVals += "'" + self.escapeString(v).decode("utf-8") + "'"
                    sqlVals += ", "
                
            if sqlColsM == "":
                sqlColsM=sqlCols[:-2] + ")"

            sqlValsM += sqlVals[:-2] + "),"

            i += 1

            if i % 100 == 0:
                sql = sqlIns + sqlColsM + " VALUES " + sqlValsM[:-1] + ";"
                #logger.debug("sql in loop")
                #logger.debug(sql)
                try:
                    cursor = self._database.cursor()
                    cursor.execute(sql)   
                    self._database.commit() 
                except (Exception) as e:
                    logger.debug(sql)
                    logger.error('error : %s', e)
                    
                    logger.error("Stack trace: ")
                    traceback.print_tb(sys.exc_info()[2])

                    sys.exit(-1)
                sqlValsM = ""

            if args["test_data"] != None:
                if i >= args["test_data"]:
                    break
            
        if i % 100 > 1:
            sql = sqlIns + sqlColsM +" VALUES " + sqlValsM[:-1]+";"
            #logger.debug("sql after loop")
            #logger.debug(sql)
            try:
                cursor = self._database.cursor()
                cursor.execute(sql)    
                self._database.commit()
            except (Exception) as e:
                logger.debug(sql)
                logger.error('SQL error : %s', e)
                
                logger.error("Stack trace: ")
                traceback.print_tb(sys.exc_info()[2])

                sys.exit(-1)
        
    def createSqlTable(self, filePath, fields):
        tableName = os.path.splitext(os.path.basename(filePath))[0]
        
        logger.debug(' generating sql create table for : %s', tableName)
        sql = "DROP TABLE IF EXISTS " + self.quoteTableOrColumn(tableName) + ";"
        cursor = self._database.cursor()
        cursor.execute(sql)

        sql = "CREATE TABLE " + self.quoteTableOrColumn(tableName) + " ("
        sql += "id BIGSERIAL, "
        sql += "sfId varchar(18) NULL, "

        for key in fields:
            if key != "Id":
                sql += self.quoteTableOrColumn(key)
                if fields[key]["type"] == "string":
                    if fields[key]["size"] < 255:
                        sql += " varchar(255) NULL, "
                    else:
                        sql += " text NULL, "

                if fields[key]["type"] == "int":
                    sql += " bigint NULL, "

                if fields[key]["type"] == "id":
                    sql += " bigint NULL, "

                if fields[key]["type"] == "datetime":
                    if args["database"].lower() == "mysql":
                        sql += " datetime NULL, "
                    else:
                        sql += " timestamp NULL, "

                if fields[key]["type"] == "bool":
                    sql += " tinyint NULL, "
                
                if fields[key]["type"] == "float":
                    sql += " decimal(15,2) NULL, "
                
        sql += "PRIMARY KEY (Id)"
        if args["database"].lower() == "mysql":
            sql += ") DEFAULT CHARSET=utf8;"
        else:
            sql += ");"
            
        try:
            logger.debug(sql) 
            cursor.execute(sql)
            self._database.commit()
        except (Exception) as e:
            logger.error(sql)
            logger.error('error : %s', e)
            
            logger.error("Stack trace: ")
            traceback.print_tb(sys.exc_info()[2])

            sys.exit(-1)

        logger.debug(' %s table created', tableName)
        
        return tableName

    def getSqlId(self, sfId):
        return base62.decode(sfId[5:15])

    def getFieldTypeByValue(self, fieldValue):
        
        match = re.findall(r'\D', fieldValue)
        if len(match) > 0:
            
            pattern = re.compile("^\d{4}\-\d{2}\-\d{2} \d{2}:\d{2}:\d{2}$")
            if pattern.match(fieldValue):
                return "datetime"
            
            pattern = re.compile("^\d{4}\-\d{2}\-\d{2} \d{2}:\d{2}:\d{2}.\d{1}$")
            if pattern.match(fieldValue):
                return "datetime"
            
            
            pattern = re.compile("^[0-9A-Za-z]{18}$")
            if pattern.match(fieldValue):
                return "id"
            
            return "string"

        try:
            i = int(fieldValue)
            if(i==0 or i==1):
                return "bool"
            return "int"
        except:
            pass
        
        try:
            float(fieldValue)
            return "float"
        except:
            pass
        
        return None

    def getFieldTypeByName(self, fieldName):
        if fieldName[-3:]=='__c':
            fieldName = fieldName[:-3]

        if fieldName[-2:] == "Id":
            return "id"
        
        if fieldName[:2] == "Is" and fieldName[2].isupper():
            return "bool"
        if fieldName[:3] == "Has" and fieldName[3].isupper():
            return "bool"
        
        if fieldName[-4:] == "Date":
            return "datetime"

        return None

    def checkInsertCount(self, tableName, dataCount):
        sql = "SELECT COUNT(*) as inserted FROM " + self.quoteTableOrColumn(tableName)
        logger.debug('Checking insert count for : %s', tableName)
        cursor = self._database.cursor()
        cursor.execute(sql)
        inserted = 0
        for x in cursor:
            inserted = x[0]
        logger.debug('total inserted : %s', inserted)
        logger.debug('total rows in file : %s', dataCount)
        if int(inserted) != dataCount:
            logger.error('inserted not equal to total rows')
        
    def resolveFile(self, filePath):
        logger.debug('Checking file: %s', filePath)

        if not os.path.isfile(filePath):
            logger.info('Please check file path for %s', filePath)
            logger.error('file not found: %s', filePath)

        else:
            logger.debug('Checking encoding errors for %s', filePath)
            
            logger.debug('Starting analyze for %s', filePath)

            # this function takes forever but we can't bypass, it formats the CSV correctly
            fileContent = self.makeItPrintable(self.getFileContent(filePath))

            csvData = csv.DictReader(io.StringIO(fileContent))
            
            totalRows = 0
            for row in csvData:
                totalRows += 1

            logger.debug('Analyzing fields')
            logger.debug('%s headers found', len(csvData.fieldnames))

            fields = {}

            for fieldName in csvData.fieldnames:
                fieldType = self.getFieldTypeByName(fieldName)

                if not fieldName in fields:
                    fields[fieldName] = {
                        "type":None,
                        "types":[],
                        "size":0
                    }
                if fieldType != None:
                    fields[fieldName]["types"].append(fieldType)
                
            logger.debug('      Start analyzing values')
            for row in csvData:
                for fieldName in csvData.fieldnames:
                    if row[fieldName] != "":
                        fieldType = self.getFieldTypeByValue(row[fieldName])
                        if not fieldType in fields[fieldName]["types"]:
                            if fieldType !=None:
                                fields[fieldName]["types"].append(fieldType)
                    fields[fieldName]["size"] = max(fields[fieldName]["size"], len(str(row[fieldName])))

                # only analyze first 2500 rows
                # note: this isn't adequate for some exports, causes "column can't be None error"
                # if totalRows > 2500:
                #    break
                
            # determine type by values in types string
            for fieldName in csvData.fieldnames:
                if len(fields[fieldName]["types"]) == 0:
                    fields[fieldName]["type"] = "string"
                elif len(fields[fieldName]["types"]) == 1:
                    fields[fieldName]["type"] = fields[fieldName]["types"][0]
                else:
                    if "string" in fields[fieldName]["types"]:
                        fields[fieldName]["type"] = "string"
                    else:
                        if "float" in fields[fieldName]["types"] and "int" in fields[fieldName]["types"]:
                            fields[fieldName]["type"] = "float"
                        else:
                            fields[fieldName]["type"] = "string"
                                
            for fieldName in csvData.fieldnames:
                logger.debug('  %s is a %s', fieldName, fields[fieldName]["type"])
                logger.debug('  types of %s are %s', fieldName, fields[fieldName]["types"])
                
            tableName = self.createSqlTable(filePath, fields)

            #insertData(tableName,fileContent,fields)
            self.insertDataBulk(tableName, fileContent, fields)
            if args["test_data"]==None:
                self.checkInsertCount(tableName,totalRows)
                        

    def resolveDirectory(self, dirPath):
        logger.debug('Checking directory: %s', dirPath)

        # get blacklist files
        blacklist = []
        if "blacklist_file" in args:
            blacklist_file = open(args["blacklist_file"], "r")
            blacklist = blacklist_file.readlines()
            blacklist = list(map(str.strip, blacklist))

        if not os.path.isdir(dirPath):
            logger.info('Please check directory path for %s', dirPath)
            logger.error('Directory not found: %s', dirPath)

        logger.debug(' reading directory(%s) to find csv files', dirPath)
        for fileName in sorted(os.listdir(dirPath)):
            filePath = os.path.join(dirPath,fileName)
            if filePath[-3:]=="csv":
                if fileName not in blacklist:
                    try:
                        self.resolveFile(filePath)
                    except Exception as ex:
                        logger.error("Couldn't import %s" % filePath)
                        
                        logger.error("Stack trace: ")
                        traceback.print_tb(sys.exc_info()[2])

                        logging.error(type(ex))
                        logging.error(ex)
        
        return True

if (__name__ == "__main__"):

    args = None
    logger = logging.getLogger("sf_csv_export_to_database.py")
    database = None
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--directory', help='path of csv directory')
    parser.add_argument('--file', help='path of csv file')
    parser.add_argument('--debug', help='debug',action='store_true')
    parser.add_argument('--test-data', help='test data size per file',type=int)
    parser.add_argument('--log-file', help='path of log file')
    parser.add_argument('--blacklist-file', help='path of blacklist file')

    args = vars(parser.parse_args())

    #print(args)
    #sys.exit(0)
    
    logLevel = logging.INFO

    if args["debug"]:
        logLevel = logging.DEBUG    

    if args["file"]==None and args["directory"]==None:
        parser.parse_args(['-h'])

    else:

        # log file
        if args["log_file"]:
            logging.basicConfig(filename=args["log_file"], filemode='w', level=logLevel)
        else:
            logging.basicConfig(level=logLevel)

        database = psycopg2.connect(
            host=args["sql_host"],
            port=args["sql_port"],
            user=args["sql_user"],
            password=args["sql_password"],
            database=args["sql_database"]
        )
        database.autocommit = True
        cursor = database.cursor()

        # hack for tinyint on PostgreSQL
        try:
            cursor.execute('CREATE DOMAIN "tinyint" AS smallint;')
        except:
            database.rollback()

        salesforce = Salesforce_to_PostgreSQL()
        salesforce._database = database

        if args["file"]==None:
            salesforce.resolveDirectory(args["directory"])
        else:
            salesforce.resolveFile(args["file"])
