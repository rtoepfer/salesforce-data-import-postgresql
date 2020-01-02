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

logger = logging.getLogger("sfcsvresolve")
myDb = None
args = None

def makeItPrintable(content):
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
        if i>0:
            output += "\n"
        output += line
        i +=1
    return output

def getFileContent(filePath):
    output = ""
    with open(filePath) as fp:
        output = fp.read()
    return output

def escapeString(value):
    strValue = strValue.replace("\'", "''")
    return strValue.encode("utf-8")

def quoteTableOrColumn(value):
    return (str('"') + str(value) + str('"'))

def insertData(tableName,fileContent,fields):
    
    logger.debug(' inserting data of %s', tableName)
    
    csvData = csv.DictReader(io.StringIO(fileContent))
    i = 0
    
    for row in csvData:
        sql = "INSERT INTO " + quoteTableOrColumn(tableName) + " "
        sqlCols = "(Id,sfId, "
        id = getSqlId(row["Id"])
        
        sqlVals = "VALUES (" + str(id) + ", '" + row["Id"]+ "', "
        for fieldName in csvData.fieldnames:
            if fieldName != "Id":
                sqlCols += "" + quoteTableOrColumn(fieldName) + ", "
                value = row[fieldName]
                if value == "" or value == None:
                    value = "NULL"
                
                nonAposTypes = ["int","bool","float"]
                
                if value == "NULL" or fields[fieldName]["type"] in nonAposTypes:
                    sqlVals += value
                else:
                    if fields[fieldName]["type"]=="id":
                        sqlVals += str(getSqlId(value))
                    elif fields[fieldName]["type"]=="datetime":
                        sqlVals += "'" + escapeString(value[:19]).decode("utf-8") + "'"
                    else:
                        sqlVals += "'" + escapeString(value).decode("utf-8") + "'"
                sqlVals += ", "

        sql = sql + sqlCols[:-2] + ") " + sqlVals[:-2] + ");"
        try:
            logger.debug(sql)
            myCursor = myDb.cursor()
            myCursor.execute(sql)
            myDb.commit()
            
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

def insertDataBulk(tableName,fileContent,fields):
    
    logger.debug(' inserting data of %s', tableName)
    
    csvData = csv.DictReader(io.StringIO(fileContent))
    i = 0
    sqlIns = "INSERT INTO " + quoteTableOrColumn(tableName) + " "
    sqlColsM = ""
    sqlValsM = ""
        
    for row in csvData:
        
        sqlCols = "(Id,sfId, "
        id = getSqlId(row["Id"])
        
        sqlVals = "(" + str(id) + ", '"+row["Id"]+ "', "
        for fieldName in csvData.fieldnames:
            if fieldName != "Id":
                sqlCols += "" + quoteTableOrColumn(fieldName) + ", "
                value = row[fieldName]
                if value=="" or value==None:
                    value = "NULL"
                
                nonAposTypes = ["int","bool","float"]
                
                if value == "NULL" or fields[fieldName]["type"] in nonAposTypes:
                    sqlVals += value
                else:
                    if fields[fieldName]["type"]=="id":
                        sqlVals += str(getSqlId(value))
                    elif fields[fieldName]["type"]=="datetime":
                        sqlVals += "'" + escapeString(value[:19]).decode("utf-8") + "'"
                    else:
                        v = unicodedata.normalize('NFKD', value).encode('ascii','ignore')
                        sqlVals += "'" + escapeString(v).decode("utf-8") + "'"
                sqlVals += ", "
            
        if sqlColsM == "":
            sqlColsM=sqlCols[:-2] + ")"

        sqlValsM += sqlVals[:-2] + "),"

        i+=1

        if i % 100 == 0:
            sql = sqlIns + sqlColsM + " VALUES " + sqlValsM[:-1] + ";"
            logger.debug("sql in loop")
            logger.debug(sql)
            try:
                myCursor = myDb.cursor()
                myCursor.execute(sql)   
                myDb.commit() 
            except (Exception) as e:
                logger.debug(sql)
                logger.error('error : %s', e)
                
                logger.error("Stack trace: ")
                traceback.print_tb(sys.exc_info()[2])

                sys.exit(-1)
            sqlValsM = ""

        if args["test_data"]!=None:
            if i>=args["test_data"]:
                break
        
    if i % 100 > 1:
        sql = sqlIns+sqlColsM +" VALUES " + sqlValsM[:-1]+";"
        logger.debug("sql after loop")
        logger.debug(sql)
        try:
            myCursor = myDb.cursor()
            myCursor.execute(sql)    
            myDb.commit()
        except (Exception) as e:
            logger.debug(sql)
            logger.error('SQL error : %s', e)
            
            logger.error("Stack trace: ")
            traceback.print_tb(sys.exc_info()[2])

            sys.exit(-1)
    
def createSqlTable(filePath,fields):
    tableName = os.path.splitext(os.path.basename(filePath))[0]
    
    logger.debug(' generating sql create table for : %s', tableName)
    sql = "DROP TABLE IF EXISTS " + quoteTableOrColumn(tableName) + ";"
    myCursor = myDb.cursor()
    myCursor.execute(sql)

    sql = "CREATE TABLE " + quoteTableOrColumn(tableName) + " ("
    sql += "id BIGSERIAL, "
    sql += "sfId varchar(18) NULL, "

    for key in fields:
        if key != "Id":
            sql += quoteTableOrColumn(key)
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
                sql += " timestamp NULL, "

            if fields[key]["type"] == "bool":
                sql += " tinyint NULL, "
            
            if fields[key]["type"] == "float":
                sql += " decimal(12,5) NULL, "
            
    sql += "PRIMARY KEY (Id)"
    sql += ");"
        
    try:
        logger.debug(sql) 
        myCursor.execute(sql)
        myDb.commit()
    except (Exception) as e:
        logger.error(sql)
        logger.error('error : %s', e)
        
        logger.error("Stack trace: ")
        traceback.print_tb(sys.exc_info()[2])

        sys.exit(-1)

    logger.debug(' %s table created', tableName)
    
    return tableName

def getSqlId(sfId):
    if args["use_base62"]:
        return base62.decode(sfId[5:15])
        
    myCursor = myDb.cursor()
    sql = "SELECT id FROM id_pool WHERE sfId='"+sfId+"';"
    myCursor.execute(sql)
    
    for x in myCursor:
        return x[0]
    
    myCursor.execute("INSERT INTO id_pool (id, sfId) VALUES (NULL, '"+sfId+"');")
    myDb.commit()

    myCursor.execute(sql)
    
    for x in myCursor:
        return x[0]    

def getFieldTypeByValue(fieldValue):
    
    match = re.findall(r'\D', fieldValue)
    if len(match)>0:
        
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

def getFieldTypeByName(fieldName):
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

def addPasswordField():
    defaultPassword = 'P@ssW0rD'
    sql = "ALTER TABLE User ADD COLUMN UserPassword varchar(255) DEFAULT '" + defaultPassword + "' AFTER Username;"
    myCursor = myDb.cursor()
    myCursor.execute(sql)

def checkInsertCount(tableName, dataCount):
    sql = "SELECT COUNT(*) as inserted FROM " + quoteTableOrColumn(tableName)
    logger.debug('Checking insert count for : %s', tableName)
    myCursor = myDb.cursor()
    myCursor.execute(sql)
    inserted = 0
    for x in myCursor:
        inserted = x[0]
    logger.debug('total inserted : %s', inserted)
    logger.debug('total rows in file : %s', dataCount)
    if int(inserted) != dataCount:
        logger.error('inserted not equal to total rows')
    
def resolveFile(filePath):
    logger.debug('Checking file: %s', filePath)

    if not os.path.isfile(filePath):
        logger.info('Please check file path for %s', filePath)
        logger.error('file not found: %s', filePath)

    else:
        logger.debug('Checking encoding errors for %s', filePath)
        
        logger.debug('Starting analyze for %s', filePath)
        
        fileContent = makeItPrintable(getFileContent(filePath))
        csvData = csv.DictReader(io.StringIO(fileContent))

        logger.debug('Analyzing fields')
        logger.debug('%s headers found',len(csvData.fieldnames))

        fields = {}

        for fieldName in csvData.fieldnames:
            fieldType = getFieldTypeByName(fieldName)

            if not fieldName in fields:
                fields[fieldName] = {
                    "type":None,
                    "types":[],
                    "size":0
                }
            if fieldType!=None:
                fields[fieldName]["types"].append(fieldType)
            
        logger.debug('      Start analyzing values')
        totalRows = 0
        for row in csvData:
            for fieldName in csvData.fieldnames:
                if row[fieldName] != "":
                    fieldType = getFieldTypeByValue(row[fieldName])
                    if not fieldType in fields[fieldName]["types"]:
                        if fieldType !=None:
                            fields[fieldName]["types"].append(fieldType)
                fields[fieldName]["size"] = max(fields[fieldName]["size"],len(str(row[fieldName])))
            totalRows += 1
        
        for fieldName in csvData.fieldnames:
            if len(fields[fieldName]["types"])==0:
                fields[fieldName]["type"] = "string"
            elif len(fields[fieldName]["types"])==1:
                fields[fieldName]["type"] = fields[fieldName]["types"][0]
            else:
                if "string" in fields[fieldName]["types"]:
                    fields[fieldName]["type"] = "string"
                else:
                    if "float" in fields[fieldName]["types"] and "int" in fields[fieldName]["types"]:
                        fields[fieldName]["type"] = "float"
                    else:
                        fields[fieldName]["type"] = "string"
            
            logger.debug('  %s is a %s',fieldName,fields[fieldName]["type"])
            logger.debug('  types of %s are %s',fieldName,fields[fieldName]["types"])
        
        tableName = createSqlTable(filePath,fields)

        #insertData(tableName,fileContent,fields)
        insertDataBulk(tableName,fileContent,fields)
        if args["test_data"]==None:
            checkInsertCount(tableName,totalRows)
                    

def resolveDirectory(dirPath):
    logger.debug('Checking directory: %s', dirPath)

    if not os.path.isdir(dirPath):
        logger.info('Please check directory path for %s', dirPath)
        logger.error('Directory not found: %s', dirPath)

    logger.debug(' reading directory(%s) to find csv files', dirPath)
    for fileName in sorted(os.listdir(dirPath)):
        filePath = os.path.join(dirPath,fileName)
        if filePath[-3:]=="csv":
            resolveFile(filePath)
    
    addPasswordField()
    return True

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    
    parser.add_argument('--directory', help='path of csv directory')
    parser.add_argument('--file', help='path of csv file')
    parser.add_argument('--debug', help='debug',action='store_true')
    parser.add_argument('--test-data', help='test data size per file',type=int)
    parser.add_argument('--log-file', help='path of log file')

    parser.add_argument('--sql-host', help='sql server address', default="127.0.0.1")
    parser.add_argument('--sql-port', help='sql server port',type=int, default=8989)  #pg 5432
    parser.add_argument('--sql-user', help='sql username', default="root")
    parser.add_argument('--sql-password', help='sql pasword')
    parser.add_argument('--sql-database', help='sql database', default="salesforce")
    parser.add_argument('--reset-id-pool', help='debug',action='store_true',default=False)
    parser.add_argument('--use-base62', help='debug',action='store_true',default=True)
    
    args = vars(parser.parse_args())
    #print(args)
    #sys.exit(0)
    
    logLevel = logging.INFO

    if args["debug"]:
        logLevel = logging.DEBUG    

    if args["file"]==None and args["directory"]==None:
        parser.parse_args(['-h'])

    else:
        if args["log_file"]:
            logging.basicConfig(filename=args["log_file"], filemode='w', level=logLevel)
        else:
            logging.basicConfig(level=logLevel)

        myDb = psycopg2.connect(host=args["sql_host"],port=args["sql_port"],user=args["sql_user"],password=args["sql_password"],database=args["sql_database"])
        myCursor = myDb.cursor()

        #create id pool table, used for new records inserted 
        logger.debug('Creating id_pool table')
        if args["reset_id_pool"]:
            sql = "DROP TABLE IF EXISTS id_pool;"
            myCursor.execute(sql)
        sql = "CREATE TABLE IF NOT EXISTS id_pool ( id BIGSERIAL, sfId varchar(255) NOT NULL, PRIMARY KEY (id), UNIQUE (id,sfId));"
        myCursor.execute(sql)

        # hack for tinyint on PostgreSQL
        try:
            myCursor.execute('CREATE DOMAIN "tinyint" AS smallint;')
        except:
            myDb.rollback()

        if args["file"]==None:
            resolveDirectory(args["directory"])
        else:
            resolveFile(args["file"])
