# -*- coding: utf-8 -*-
"""
Created on Fri Dec 09 08:50:36 2016

@author: christopher.brossman
"""
#import os
#path = 'C:/Users/christopher.brossman/Documents/Projects/work/KeboolaAPI/'
#os.chdir(path)
#f = 'kbcAPI_class.py'
#execfile(f)

import requests
import json
import pandas as pd
import boto3
import re

class KBC_API(object):
    'common class for all APIs'

    
    def __init__(self, key):
        
        self.HEADERS = {'X-StorageApi-Token': key}
        self.baseURLs = {
               'token' : 'https://connection.keboola.com/v2/storage/tokens/',
               'token_verify' : 'https://connection.keboola.com/v2/storage/tokens/verify/',
               'storage' : 'https://connection.keboola.com/v2/storage',
               'bucket' : 'https://connection.keboola.com/v2/storage/buckets/',
               'table' : 'https://connection.keboola.com/v2/storage/tables/',
               'jobs' : 'https://connection.keboola.com/v2/storage/jobs/',
               'workspaces' : 'https://connection.keboola.com/v2/storage/workspaces/',
               'files' : 'https://connection.keboola.com/v2/storage/files/',
               'components' : 'https://connection.keboola.com/v2/storage/components/',
               'stats' : 'https://connection.keboola.com/v2/storage/stats',
               'events' : 'https://connection.keboola.com/v3/storage/events'
            }

            
#########################################################################################
#       Utility Functions
#########################################################################################

    def printHeader(self):
        'prints objects headers'
        print self.HEADERS
    
    def printbaseURL(self, key = None):
        'prints dict of key-val pairs for base urls'
        if key == None:
            print self.baseURLs
        else:
            print self.baseURLs[key]

    def getKD(self, URL, request = "GET", data = None, params = None, files = None, headers = None):
        
        'handles all API requests using requests package'
        
        if headers == None:
            headers = self.HEADERS
        if data == None and params == None and files == None:
            response = requests.request(request, URL, headers=headers)
        elif data == None and files == None:
            response = requests.request(request, URL, headers=headers, params = params)
        elif params == None and files == None:
            response = requests.request(request, URL, headers=headers, data = data)
        elif params == None and data == None:
            response = requests.request(request, URL, headers=headers,files = files)
        elif params == None:
            response = requests.request(request, URL, headers=headers, data = data, files = files)
        elif files == None:
            response = requests.request(request, URL, headers=headers, params = params, data = data)
        elif data == None:
            response = requests.request(request, URL, headers=headers, params = params, files = files)
        else:
            response = requests.request(request, URL, headers=headers, params = params, data = data, files = files)
        
        print response
        return response
    

#########################################################################################
#       Tokens http://docs.keboola.apiary.io/#reference/tokens-and-permissions/tokens-collection/list-all-tokens
#########################################################################################    
    
    def getAllTokens(self, tokenId = None):
        'returns df, based on token URL'
        if tokenId == None:
            url = self.baseURLs['token']
        else:
            url = self.baseURLs['token'] + str(tokenId)
        r = self.getKD(url)
        data = json.loads(r.text)
        df = pd.DataFrame(data)
        return(df)
        
    def createToken(self, description, bucketPermissions, componentAccess,
                    canManageBuckets = None, canReadAllFileUploads = None, expiresIn = None):
        'CAUTION -- NEEDS TESTING'
        
        url = self.baseURLs['token']
        PAYLOAD = {'description' : description, 'bucketPermissions' : bucketPermissions,
                   'componentAccess' : componentAccess}
        
        if canManageBuckets == None:
            PAYLOAD['canManageBuckets'] = canManageBuckets
        if canReadAllFileUploads == None:
            PAYLOAD['canReadAllFileUploads'] = canReadAllFileUploads
        if expiresIn == None:
            PAYLOAD['expiresIn'] = expiresIn

        r = self.getKD(url, params = PAYLOAD, request = "POST")
        data = json.loads(r.text)
        df = pd.DataFrame(data)
        return(df)
    
    def updateToken(self, token_id, description, bucketPermissions, componentAccess, canReadAllFileUploads = None):
        
        'CAUTION -- NEEDS TESTING'
        
        url = self.baseURLs['token'] + token_id

        PAYLOAD = {'description' : description, 'bucketPermissions' : bucketPermissions, 'componentAccess' : componentAccess}
        if canReadAllFileUploads == None:
            PAYLOAD['canReadAllFileUploads'] = canReadAllFileUploads
       
        r = self.getKD(url, params = PAYLOAD, request = "PUT")
        data = json.loads(r.text)
        df = pd.DataFrame(data)
        return(df)
        
    
    def shareToken(self, token_id, recipientEmail, message):
        
        'CAUTION -- NEEDS TESTING'
        
        url = self.baseURLs['token'] + token_id + '/share'
        PAYLOAD = {'recipientEmail' : recipientEmail, 'message' : message}
        HEADERS = self.HEADERS
        HEADERS['Content-Type'] = 'application/x-www-form-urlencoded'    
        r = self.getKD(url, params = PAYLOAD, headers = HEADERS, request = "POST")
        data = json.loads(r.text)
        df = pd.DataFrame(data)
        return(df)
    
    def getTokenVerification(self):
        
        'CAUTION -- NEEDS TESTING'
        
        url = self.baseURLs['token_verify']
        r = self.getKD(url)
        data = json.loads(r.text)
        df = pd.DataFrame(data)
        return(df)
        
    def getTokenDetailEmail(self, email):
        'returns df, based on email supplied'
        #email = 'christopher.brossman@dominionmarinemedia.com'
        df = self.getAllTokens()
        index = df[df['description'] == email].index.tolist()[0]
        key_id = df.ix[index]['id']
        url = self.baseURLs['token'] + key_id
        r = self.getKD(url)
        data = json.loads(r.text)
        df2 = pd.DataFrame(data)
        return df2
    
#########################################################################################
#       Misc: http://docs.keboola.apiary.io/#reference/miscellaneous/api-index/get
#########################################################################################   
        
    def getMaintenance(self):
        'returns df, available components list'
        url = self.baseURLs['storage']
        r = self.getKD(url)
        data = json.loads(r.text)
        df = pd.DataFrame(data['components'])
        return(df)
        
#########################################################################################
#       Buckets http://docs.keboola.apiary.io/#reference/buckets/bucket-detail
#########################################################################################        
        
    def getBuckets(self, params = None, bucket_id = None):
        'returns df, table detail based on bucket. supply bucket_id for specifics'
        if bucket_id == None:
            url = self.baseURLs['bucket']
        else:
            url = self.baseURLs['bucket'] + bucket_id
        r = self.getKD(url, params = params)
        data = json.loads(r.text)
        df = pd.DataFrame(data)
        return df  
        
    def createBucket(self, name, stage, description = None, backend = None):
        'return dict, creates bucket. name = name of bucket, stage is in/out'
        url = self.baseURLs['bucket']
        PAYLOAD = {
                   'name': name
                    ,'stage': stage
            }
        if description != None:
            PAYLOAD['description'] = description
        if backend != None:
            PAYLOAD['backend'] = backend
    
        r = self.getKD(URL = url, request = "POST", data = PAYLOAD)
        return json.loads(r.text)
        
    def deleteBucket(self,bucket_id):
        'returns nothing, deletes bucket'
        url = self.baseURLs['bucket'] + bucket_id
        r = self.getKD(URL = url, request = "DELETE")
        
#########################################################################################
#       Tables http://docs.keboola.apiary.io/#reference/tables/tables-in-bucket
#########################################################################################

    def getTablesInBucket(self, bucket = None, params = None):
        'returns dict, table detail based on bucket or credentials'
        if bucket == None:
            url = self.baseURLs['table']
        else:
            url = self.baseURLs['bucket'] + bucket + '/tables'
        r = self.getKD(url, params = params)
        data = json.loads(r.text)
        return data  
        
    def createTable(self, file_path, bucket_id, name, delimiter = None, 
                    enclosure = None, escapedBy = None, primaryKey = None):
        
        'returns dict, creates table from file on computer'
        
        url = self.baseURLs['bucket'] + bucket_id + '/tables'
        PAYLOAD = {
                   'name': name
            }
            
        FILES = {'data': open(file_path)}
            
        HEADERS = self.HEADERS
        HEADERS ['Content-Type'] = 'multipart/form-data; boundary=----WebKitFormBoundaryfIBRqd05C6Na2Lvl'
        
        if delimiter != None:
            PAYLOAD['delimiter'] = delimiter
        if enclosure != None:
            PAYLOAD['enclosure'] = enclosure
        if enclosure != None:
            PAYLOAD['escapedBy'] = escapedBy
        if enclosure != None:
            PAYLOAD['primaryKey'] = primaryKey
    
        r = self.getKD(URL = url, request = "POST", headers = HEADERS, data = PAYLOAD, files = FILES)
        return json.loads(r.text)
        
    
    def createTableAsync(self, file_path, table_id, name, delimiter = None, 
                    enclosure = None, escapedBy = None, primaryKey = None):
        'CAUTION -- NEEDS TESTING'
        
        'returns dict, creates table from file on computer'
        
        url = self.baseURLs['table'] + table_id + '/import-async'
        PAYLOAD = {
                   'name': name
            }
        if delimiter == None:
            PAYLOAD['delimiter'] = delimiter
        if enclosure == None:
            PAYLOAD['enclosure'] = enclosure
        if escapedBy == None:
            PAYLOAD['escapedBy'] = escapedBy
        if primaryKey == None:
            PAYLOAD['primaryKey'] = primaryKey
            
        FILES = {'data': open(file_path)}
            
        HEADERS = self.HEADERS
        HEADERS ['Content-Type'] = 'multipart/form-data; boundary=----WebKitFormBoundaryU2xN082HVaIRptvd'
        
        if delimiter != None:
            PAYLOAD['delimiter'] = delimiter
        if enclosure != None:
            PAYLOAD['enclosure'] = enclosure
        if enclosure != None:
            PAYLOAD['escapedBy'] = escapedBy
        if enclosure != None:
            PAYLOAD['primaryKey'] = primaryKey
    
        r = self.getKD(URL = url, request = "POST", headers = HEADERS, data = PAYLOAD, files = FILES)
        return json.loads(r.text)
        
        
    def downloadTableAsync(self,table_id, limit = None, formats = None, changedSince = None,
                  changedUntil = None, columns = None, whereColumn = None,
                  whereValues = None, whereOperator = None, gzip = None):
        
        'returns dict, async starts job to download table'
        
        URL = self.baseURLs['table'] + table_id + '/export-async'
        HEADERS = self.HEADERS
        HEADERS['Accept-encoding'] = 'gzip'
        
        PAYLOAD = {}
        if limit != None:
            PAYLOAD['limit'] = limit
        if formats != None:
            PAYLOAD['format'] = formats
        if changedSince != None:
            PAYLOAD['changedSince'] = changedSince
        if changedUntil != None:
            PAYLOAD['changedUntil'] = changedUntil
        if columns != None:
            PAYLOAD['columns'] = columns
        if whereColumn != None:
            PAYLOAD['whereColumn'] = whereColumn
        if whereValues != None:
            PAYLOAD['whereValues'] = whereValues
        if whereOperator != None:
            PAYLOAD['whereOperator'] = whereOperator
    
        r = self.getKD(URL = URL, request = "POST", params = PAYLOAD, headers = HEADERS)
        return json.loads(r.text)
        
    def getTableDetail(self, table_id):
        'returns dict, gets table details'
        url = self.baseURLs['table'] + table_id
        r = self.getKD(url)
        data = json.loads(r.text)
        return data
        
    def deleteTable(self,table_id):
        'returns nothing, deletes table'
        url = self.baseURLs['tables'] + table_id
        r = self.getKD(URL = url, request = "DELETE")
        
    def createPrimaryKey(self, table_id, columns):
        'CAUTION -- NEEDS TESTING'
        url = self.baseURLs['tables'] + table_id + '/primary-key/' 
        PAYLOAD = {'columns[]': columns}
        r = self.getKD(URL = url, request = "POST", params = PAYLOAD)
    
    def removePrimaryKey(self, table_id):
        'CAUTION -- NEEDS TESTING'
        url = self.baseURLs['tables'] + table_id + '/primary-key/' 
        r = self.getKD(URL = url, request = "DELETE")
        
    def createColumnIndex(self, table_id, name):
        'CAUTION -- NEEDS TESTING'
        url = self.baseURLs['bucket'] + table_id + '/indexed-columns/'
        HEADERS = self.HEADERS
        HEADERS['Content-Type'] = 'application/x-www-form-urlencoded'    
        PAYLOAD = {'name' : name}    
        r = self.getKD(url, params = PAYLOAD, headers = HEADERS, request = "POST")
        data = json.loads(r.text)
        return data  
        
    def removeColumnIndex(self, table_id, column_name):
        'CAUTION -- NEEDS TESTING'
        url = self.baseURLs['tables'] + table_id + '/indexed-columns/' + column_name
        r = self.getKD(URL = url, request = "DELETE")
    
    def deleteTableRows(self, table_id, filterString = None, changedSince = None):
        'CAUTION -- NEEDS TESTING'
        url = self.baseURLs['tables'] + table_id + '/rows'
        HEADERS = self.HEADERS
        HEADERS ['Accept-encoding'] = 'gzip'
        
        if filterString != None and changedSince != None:
            PAYLOAD = {}
            if filterString != None:
                PAYLOAD['filterString'] = filterString
            if changedSince != None:
                PAYLOAD['changedSince'] = changedSince
            r = self.getKD(URL = url, request = "DELETE", headers = HEADERS, params = PAYLOAD)
        else:
            r = self.getKD(URL = url, request = "DELETE", headers = HEADERS)
            
#########################################################################################
#       Table Simple Aliases: http://docs.keboola.apiary.io/#reference/table-simple-aliases/remove-alias-filter
#########################################################################################
    
    def createTableAlias(self,bucket_id, tableId, name, aliasFilter = None, aliasColumns = None):
        
        'returns dict, creates table alias'
        
        URL = self.baseURLs['bucket'] + bucket_id + '/table-aliases'
                
        PAYLOAD = {
               'sourceTable': tableId,
               'name' : name
        }
        
        if aliasFilter != None:
            PAYLOAD['aliasFilter'] = aliasFilter
        if aliasColumns != None:
            PAYLOAD['aliasColumns'] = aliasColumns
  
        r = self.getKD(URL = URL, request = "POST", params = PAYLOAD)
        return json.loads(r.text)
        
    def updateAliasFilter(self, table_id, column, operator, values):
        'CAUTION -- NEEDS TESTING'
        URL = self.baseURLs['tables'] + table_id + '/alias-filter'
        PAYLOAD = {'column' : column, 'operator' : operator, 'values' : values}
        r = self.getKD(URL = URL, request = "POST", params = PAYLOAD)
        return json.loads(r.text)
    
    def removeAliasFilter(self):
        'CAUTION -- NEEDS TESTING'
        URL = self.baseURLs['tables'] + table_id + '/alias-filter'
        r = self.getKD(URL = URL, request = "DELETE")    
    
#########################################################################################
#       Workspaces: http://docs.keboola.apiary.io/#reference/workspaces/remove-alias-filter
#########################################################################################
    def getWorkspaceDetail(self,workspace_id = None):
        ' returns DF, get workspace detail for a workspace or if null, then returns all workspaces'
        #url = urlDict['workspaces'] + '/13021'
        if workspace_id == None:
            url = self.baseURLs['workspaces']
        else:
            url = self.baseURLs['workspaces'] + workspace_id
        r = self.getKD(url)
        data = json.loads(r.text)
        df = pd.DataFrame(data)
        return df

        
    def createWorkspace(self, backend = None, statementTimeoutSeconds = None):
        
        'returns dict, creates workspace'
        
        URL = self.baseURLs['workspaces']

        if backend != None or statementTimeoutSeconds != None:
            PAYLOAD = {}
            if backend != None:
                PAYLOAD['backend'] = backend
            if statementTimeoutSeconds != None:
                PAYLOAD['statementTimeoutSeconds'] = statementTimeoutSeconds
            r = self.getKD(URL = URL, request = "POST", params = PAYLOAD)
        
        r = self.getKD(URL = URL, request = "POST")
        return json.loads(response.text)

        
    def deleteWorkspace(workspace_id):
        
        'returns nothing, deletes workspace'
        
        URL = self.baseURLs['workspaces'] + workspace_id
        r = self.getKD(URL = URL, request = "DELETE")
        
        
    def createInputListFromDF(df):
        
        """purpose of this helper function to load data into a workspace.
        ...
        	source (required) Full table identifier of source table (eg: in.c-bucket.mytable)
        	destination (required) Destination table name
        	rows (optional) Limit the number of returned rows
        	seconds (optional) Return rows created or updated in the last X seconds
        	columns (optional) comma separated list of columns to export, by default all columns are exported
        	whereColumn, whereValues, whereOperator (optional) rows filtering, see more in rows filtering section
        	datatypes (optional) key/value array of column name / type to be applied
        	sortKey (optional - Redshift only) Column(s) to be used as sort key
        	distStyle (optional - Redshift only) Distribution style (even, all, or key)
        	distKey (optional - Redshift only) Column(s) to use for key distribution style"""
        
        PAYLOAD = {}
        df2 = df.notnull()
        for i in range(0,df.shape[0]):
            for c in df.columns:
                if df2.ix[i][c]:
                    k = 'input[' + str(i) + '][' + str(c) + ']'
                    v = str(df.ix[i][c])
                    PAYLOAD[k] = v
        return PAYLOAD
        
        
    def loadDataInWorkspace(self, inputList, workspace_id = None,  preserve = None):
        
        'returns dict, loads data into workspaces'
        
        #default CB workspace
        if workspace_id == None:
            workspace_id = '13468'
    
        URL = self.baseURLs['workspaces'] + workspace_id + '/load'
        
        if preserve != None:
            inputList['preserve'] = preserve

        r = self.getKD(URL = URL, request = "POST", data = inputList)
        return json.loads(r.text)
        
#########################################################################################
#       Events: http://docs.keboola.apiary.io/#reference/events/create-events/create-event
#########################################################################################

    def createEvent(self, message, componentName, description = None, typeOfEvent = None, configurationId = None,
                    params = None, results = None, duration = None, runId = None):
        'CAUTION -- NEEDS TESTING'
        
        url = self.baseURLs['events']
        
        PAYLOAD = {'message' : message, 'componentName' : componentName}
        if description != None:
            PAYLOAD['description'] = description
        if typeOfEvent != None:
            PAYLOAD['typeOfEvent'] = typeOfEvent
        if configurationId != None:
            PAYLOAD['configurationId'] = configurationId
        if params != None:
            PAYLOAD['params'] = params
        if results != None:
            PAYLOAD['results'] = results
        if duration != None:
            PAYLOAD['duration'] = duration
        if runId != None:
            PAYLOAD['runId'] = runId
        
        r = self.getKD(URL = url, request = "POST", params = PAYLOAD)
        data = json.loads(r.text)
        return data
    
    def listEvents(self):
        'CAUTION -- NEEDS TESTING'
        url = self.baseURLs['events']
        r = self.getKD(URL = url)
        data = json.loads(r.text)
        return data
        
    def listBucketEvents(self, bucket_id):
        'CAUTION -- NEEDS TESTING'
        url = self.baseURLs['bucket'] + bucket_id + '/events'
        r = self.getKD(URL = url)
        data = json.loads(r.text)
        return data
    
    def listTableEvents(self, table_id):
        'CAUTION -- NEEDS TESTING'
        url = self.baseURLs['table'] + table_id +  '/events'
        r = self.getKD(URL = url)
        data = json.loads(r.text)
        return data
    
    
#########################################################################################
#       Attributes: http://docs.keboola.apiary.io/#reference/attributes/create-or-manage-bucket-attributes/set-bucket-attribute
#########################################################################################
        
    def setBucketAttribute(self, bucket_id, attribute_key, value, protected = None):
        url = self.baseURLs['bucket'] + '/attributes/' + attribute_key
        'CAUTION -- NEEDS TESTING'
        PAYLOAD = {'value' : value }
        if protected != None:
            PAYLOAD['protected'] = protected
        HEADERS = self.HEADERS
        HEADERS ['Content-Type'] = 'application/x-www-form-urlencoded'        
        r = self.getKD(url, params = PAYLOAD, headers = HEADERS, request = "POST")
        data = json.loads(r.text)
        return data  
        
    def deleteBucketAttribute(self, bucket_id, attribute_key):
        'CAUTION -- NEEDS TESTING'
        url = self.baseURLs['bucket'] + '/attributes/' + attribute_key
        r = self.getKD(url, request = "DELETE")

    def replaceBucketAttribute(self, bucket_id, name, value, protected):
        'CAUTION -- NEEDS TESTING'
        url = self.baseURLs['bucket'] + bucket_id + '/attributes'
        HEADERS = self.HEADERS
        HEADERS['Content-type'] = 'application/x-www-form-urlencoded'
        PAYLOAD = {'name' : name, 'value' : value, 'protected' : protected}
        r = self.getKD(url, params = PAYLOAD, headers = HEADERS, request = "POST")
        data = json.loads(r.text)
        return data         
    
    def listBucketAttributes(self, bucket_id):
        'CAUTION -- NEEDS TESTING'
        url = self.baseURLs['bucket'] + '/attributes/'
        r = self.getKD(url)
        data = json.loads(r.text)
        return data  

#########################################################################################
#       Jobs: http://docs.keboola.apiary.io/#reference/jobs/list-jobs/jobs-list
#########################################################################################

    def getAllJobs(self, jobId = None):
        'CAUTION -- NEEDS TESTING'
        if jobId == None:
            url = self.baseURLs['jobs']
        else:
            url = self.baseURLs['jobs'] + jobId
        r = self.getKD(url)
        data = json.loads(r.text)
        df = pd.DataFrame(data)
        return df
    
#########################################################################################
#       Files: http://docs.keboola.apiary.io/#reference/files/upload-file/upload-arbitrary-file-to-keboola
#########################################################################################
    def uploadArbFileToKeboola(self, name, sizeBytes = None, contentType = None, isPublic = None, isPermanent = None, 
                               notify = None, tag = None, federationToken = None, sliced = None, isEncrypted = None):
        'needs more research'
        
        'returns dict, This method allows direct upload to backend S3 storage.'
        
        #create new file resource
        #upload file to s3
        pass
    
    def listFiles(self):
        'CAUTION -- NEEDS TESTING'
        url = self.baseURLs['files']
        r = self.getKD(url)
        data = json.loads(r.text)
        return data

    def getFilesDetail(self, file_id):
        'returns dict, gets file details for file on s3'
        url = self.baseURLs['files'] + str(file_id) + '?federationToken=true'
        r = self.getKD(url)
        data = json.loads(r.text)
        return data
        
    def deleteSingleFile(self, file_id):
        'CAUTION -- NEEDS TESTING'
        url = self.baseURLs['files'] + str(file_id)
        r = self.getKD(url, request = "DELETE")
        
    def downloadAsyncFileFromS3(self,file_id):
        'returns df, downloads AysncFile s3 server'
        data = self.getFilesDetail(file_id)
        csvManifestUrl = data['url']
        ACCESS_KEY = data['credentials']['AccessKeyId']
        SECRET_KEY = data['credentials']['SecretAccessKey']
        SESSION_TOKEN = data['credentials']['SessionToken']

        BUCKET = data['s3Path']['bucket']
        KEY = data['s3Path']['key']

        request = requests.request("GET",csvManifestUrl)
        csvManifest = json.loads(request.text)
        
        matchString = 's3://' + BUCKET + '/'
        
        fileList = [re.sub(matchString, '',f['url']) for f in csvManifest['entries']]
        

        client = boto3.client(
            's3',
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            aws_session_token=SESSION_TOKEN,
        )
        
        #download first file - accessDenied
        dfList = []
        for f in fileList:
            obj = client.get_object(Bucket=BUCKET, Key=f)
            df = pd.read_csv(obj['Body'], header=None)
            dfList.append(df)
            
        df = pd.concat(dfList)
        
        #old methods
        #client.download_file(BUCKET,fileList[0],'firstFile.csv')
        #obj = client.get_object(Bucket=BUCKET, Key=fileList[0])
        #df = pd.read_csv(obj['Body'], header=None)
        return df
        
        
    def createFileTag(self, file_id, tag):
        'CAUTION -- NEEDS TESTING'
        url = self.baseURLs['files'] + str(file_id) + '/tags'
        
        PAYLOAD = {'tag' : tag}
        HEADERS = self.HEADERS
        HEADERS['Content-Type'] = 'application/x-www-form-urlencoded'
        
        r = self.getKD(url, params = PAYLOAD, headers = HEADERS, request = "POST")
        data = json.loads(r.text)
        return data       

    def deleteTags(self, file_id, tag_name):
        'CAUTION -- NEEDS TESTING'
        url = self.baseURLs['files'] + str(file_id) + '/tags/' + tag_name
        r = self.getKD(url, request = "DELETE")
        
#########################################################################################
#       Config: http://docs.keboola.apiary.io/#reference/component-configurations/component-configs/list-configs
#########################################################################################

    def listConfigs(self, component_id = None, config_id = None):
        'CAUTION -- NEEDS TESTING'
        if component_id == None and config_id == None:
            url = self.baseURLs['components']
        elif  config_id == None:
            url = self.baseURLs['components'] + str(component_id) + '/configs'
        else:
            url = self.baseURLs['components'] + str(component_id) + '/configs/' + str(config_id)
        r = self.getKD(url)
        data = json.loads(r.text)
        return data
        
    def createConfig(self, component_id, name, configurationId = None, description = None, configuration = None,
                     state = None, changeDescription = None):
        
        'CAUTION -- NEEDS TESTING'
        
        url = self.baseURLs['components'] + str(component_id) + '/configs'
        
        PAYLOAD = {}
        if configurationId is not None or description is not None or configuration is not None or state is not None or changeDescription is not None:
            PAYLOAD = {}
            if configurationId != None:
                PAYLOAD['configurationId'] = configurationId
            if description != None:
                PAYLOAD['description'] = description
            if configuration != None:
                PAYLOAD['configuration'] = configuration
            if state != None:
                PAYLOAD['state'] = state
            if changeDescription != None:
                PAYLOAD['changeDescription'] = changeDescription
            r = self.getKD(url, params = PAYLOAD, request = "POST")
        else:
            r = self.getKD(url, request = "POST")
            
        data = json.loads(r.text)
        return data
            
    
    def deleteConfig(self,component_id,config_id ):
        'CAUTION -- NEEDS TESTING'
        url = self.baseURLs['components'] + str(component_id) + '/configs/' + str(config_id)
        r = self.getKD(url, request = "DELETE")
    
    def updateConfig(self, component_id, config_id, name, description, configuration, state, changeDescription):
        'CAUTION -- NEEDS TESTING'
        url = self.baseURLs['components'] + str(component_id) + '/configs/' + str(config_id)
        PAYLOAD = {'name' : name, 'description' : description, 'configuration' : configuration, 'state' : state, 'changeDescription' : changeDescription}
        r = self.getKD(url, request = "PUT", params = PAYLOAD)
        data = json.loads(r.text)
        return data       
    
    def publishConfig(self, component_id, config_id, description):
        'CAUTION -- NEEDS TESTING'
        url = self.baseURLs['components'] + str(component_id) + '/configs/' + str(config_id) + '/publish'
        PAYLOAD = {'description' : description}
        r = self.getKD(url, request = "POST", params = PAYLOAD)
        data = json.loads(r.text)
        return data
    
    def getconfigsVersions(self, component_id, config_id, include = None, limit = 100, offset = None):
        'CAUTION -- NEEDS TESTING'
        
        url = self.baseURLs['components'] + str(component_id) + '/configs/' + str(config_id) + '/versions'
        PAYLOAD = {'limit' : limit}
        if include == None:
            PAYLOAD['include'] = include
        if offset == None:
            PAYLOAD['offset'] = offset
        
        r = self.getKD(url, params = PAYLOAD)
        data = json.loads(r.text)
        return data
        
    def getconfigsInfoByVersion(self, component_id, config_id, version_id):
        'CAUTION -- NEEDS TESTING'
        url = self.baseURLs['components'] + str(component_id) + '/configs/' + str(config_id) + '/versions/' + str(version_id)
        r = self.getKD(url)
        data = json.loads(r.text)
        return data
    
    def rollbackVersion(self, component_id, config_id, version_id, changeDescription = None):
        'CAUTION -- NEEDS TESTING'
        url = self.baseURLs['components'] + str(component_id) + '/configs/' + str(config_id) + '/versions/' + str(version_id) + '/rollback'
        if changeDescription == None:
            r = self.getKD(url, request = "POST")
        else:
            PAYLOAD = {'changeDescription' : changeDescription}
            r = self.getKD(url, request = "POST", params = PAYLOAD)
        data = json.loads(r.text)
        return data
        
    def createConfigCopy(self, component_id, config_id, version_id, name = None, description = None, changeDescription = None):
       'CAUTION -- NEEDS TESTING'
        url = self.baseURLs['components'] + str(component_id) + '/configs/' + str(config_id) + '/versions/' + str(version_id) + '/create'
        if changeDescription == None and description == None and name == None:
            r = self.getKD(url, request = "POST")
        else:
            PAYLOAD = {}
            if changeDescription != None:
                PAYLOAD['changeDescription'] = changeDescription
            if description != None:
                PAYLOAD['description'] = description
            if name != None:
                PAYLOAD['name'] = name
            r = self.getKD(url, request = "POST", params = PAYLOAD)
        data = json.loads(r.text)
        return data


    def createConfigRow(self, component_id, config_id, rowId = None, configuration = None, changeDescription = None):
        'CAUTION -- NEEDS TESTING'
        url = self.baseURLs['components'] + str(component_id) + '/configs/' + str(config_id) + '/rows'
        
        if rowId == None and configuration == None and changeDescription == None:
            r = self.getKD(url, request = "POST")
        else:
            PAYLOAD = {}
            if rowId != None:
                PAYLOAD['rowId'] = rowId
            if configuration != None:
                PAYLOAD['configuration'] = configuration
            if changeDescription != None:
                PAYLOAD['changeDescription'] = changeDescription
            r = self.getKD(url, request = "POST", params = PAYLOAD)
        data = json.loads(r.text)
        return data
    
    def getConfigRows(self, component_id, config_id):
        'CAUTION -- NEEDS TESTING'
        url = self.baseURLs['components'] + str(component_id) + '/configs/' + str(config_id) + '/rows'
        r = self.getKD(url)
        data = json.loads(r.text)
        return data       
        
    def updateConfigRow(self, component_id, config_id, row_id, configuration, changeDescription = None):
        
        'CAUTION -- NEEDS TESTING'
        url = self.baseURLs['components'] + str(component_id) + '/configs/' + str(config_id) + '/rows/' + str(row_id)
        
        PAYLOAD = {'configuration' : configuration}
        
        if changeDescription == None:
            r = self.getKD(url, request = "PUT")
        else:
            PAYLOAD['changeDescription'] = changeDescription
            r = self.getKD(url, request = "PUT", params = PAYLOAD)
        
        data = json.loads(r.text)
        return data 
    
    def deleteConfigRow(self, component_id, config_id, row_id, changeDescription = None):
        'CAUTION -- NEEDS TESTING'
        url = self.baseURLs['components'] + str(component_id) + '/configs/' + str(config_id) + '/rows/' + str(row_id)
        if changeDescription == None:
            r = self.getKD(url, request = "DELETE")
        else:
            PAYLOAD = {'changeDescription' : changeDescription}
            r = self.getKD(url, request = "DELETE", params = PAYLOAD)
            
    def listConfigRowVersion(self, component_id, config_id, row_id, include = None, limit = 100, offset = None):
        'CAUTION -- NEEDS TESTING'
        url = self.baseURLs['components'] + str(component_id) + '/configs/' + str(config_id) + '/rows/' + str(row_id) + '/versions'
        
        PAYLOAD = {'limit' : limit}
        if include != None:
            PAYLOAD['include'] = include
        if offset !=None:
            PAYLOAD['offset'] = offset

        r = self.getKD(url, params = PAYLOAD)
        data = json.loads(r.text)
        return data            
        
    def getConfigRowVersion(self, component_id, config_id, row_id, version_id):
        'CAUTION -- NEEDS TESTING'
        url = self.baseURLs['components'] + str(component_id) + '/configs/' + str(config_id) + '/rows/' + str(row_id) + '/versions/' + str(version_id)
        r = self.getKD(url)
        data = json.loads(r.text)
        return data
        
    def rollbackConfigRowVersion(self, component_id, config_id, row_id, version_id, changeDescription = None):
        'CAUTION -- NEEDS TESTING'
        
        url = self.baseURLs['components'] + str(component_id) + '/configs/' + str(config_id) + '/rows/' + str(row_id) + '/versions/' + str(version_id) + '/rollback'
        
        if changeDescription == None:
            r = self.getKD(url, request = "POST")
        else:
            PAYLOAD = {'changeDescription' : changeDescription}
            r = self.getKD(url, request = "POST", params = PAYLOAD)
          
        data = json.loads(r.text)
        return data          
        
        
    def copyConfigRow(self, component_id, config_id, row_id, version_id, targetConfigId = None, changeDescription = None):
        'CAUTION -- NEEDS TESTING'
        
        url = self.baseURLs['components'] + str(component_id) + '/configs/' + str(config_id) + '/rows/' + str(row_id) + '/versions/' + str(version_id) + '/create'
                
        if targetConfigId == None and changeDescription == None:
            r = self.getKD(url, request = "POST")
        else:
            PAYLOAD = {}
            if targetConfigId != None:
                PAYLOAD['targetConfigId'] = targetConfigId
            if changeDescription != None:
                PAYLOAD['changeDescription'] = changeDescription
            r = self.getKD(url, request = "POST", params = PAYLOAD)
        data = json.loads(r.text)
        return data         


#########################################################################################
#       Stats: http://docs.keboola.apiary.io/#reference/stats/runid/stats-detail
#########################################################################################

    def getRunIdStats(self, runId):
        'CAUTION -- NEEDS TESTING'
        url = self.baseURLs['stats'] + '?runId=' + str(runId)
        r = self.getKD(url)
        data = json.loads(r.text)
        return data    