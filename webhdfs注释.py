import sys, os
import stat
import httplib
import urlparse
import json
import logging

#设置日志
logging.basicConfig(level=logging.DEBUG, datefmt='%m/%d/%Y %I:%M:%S %p',
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(name='webhdfs')
#WebHDFS的URL目录根
WEBHDFS_CONTEXT_ROOT="/webhdfs/v1"

#使用WebHDFS REST API进行HDFS操作的类
class WebHDFS(object): 
    #初始化HDFS的基本信息：主机号host,端口号port和用户username
    def __init__(self, namenode_host, namenode_port, hdfs_username):
        self.namenode_host=namenode_host
        self.namenode_port = namenode_port
        self.username = hdfs_username
        
    
    ######################################################################
    def mkDir(self, path):
        url_path = WEBHDFS_CONTEXT_ROOT + path +'?op=MKDIRS&user.name='+self.username
        logger.debug("Create directory: " + url_path)
        httpClient = self.__getNameNodeHTTPClient()
        httpClient.request('PUT', url_path , headers={})
        response = httpClient.getresponse()
        logger.debug("HTTP Response: %d, %s"%(response.status, response.reason))
        httpClient.close()
        CheckResponseError(response)
        
    ######################################################################
    def delete(self, path, recursive = False):
        url_path = WEBHDFS_CONTEXT_ROOT + path +'?op=DELETE&recursive=' + ('true' if recursive else 'false') + '&user.name='+self.username
        logger.debug("Delete directory: " + url_path)
        httpClient = self.__getNameNodeHTTPClient()
        httpClient.request('DELETE', url_path , headers={})
        response = httpClient.getresponse()
        logger.debug("HTTP Response: %d, %s"%(response.status, response.reason))
        httpClient.close()
        CheckResponseError(response)
        
    ######################################################################
    def rmDir(self, path):
        self.delete(path, recursive = True)
     
    ######################################################################
    def copyToHDFS(self, source_path, target_path, replication=1, overwrite=False):
        #WebHDFS访问URL，设置操作op为CREATE
        url_path = WEBHDFS_CONTEXT_ROOT + target_path + '?op=CREATE&overwrite=' + ('true' if overwrite else 'false') +\
                                                        '&replication=' + str(replication) + '&user.name='+self.username
        httpClient = self.__getNameNodeHTTPClient()
        httpClient.request('PUT', url_path , headers={})#设置HTTP PUT操作
        response = httpClient.getresponse()
        logger.debug("HTTP Response: response.status = '%d',  response.reason = '%s', response.msg = '%s'"%
            (response.status, response.reason, response.msg))
        #获得重定向后的目的文件地址，并对其进行解析
        redirect_location = response.msg["location"]
        logger.debug("HTTP Location: %s"%(redirect_location))
        result = urlparse.urlparse(redirect_location)
        redirect_host = result.netloc[:result.netloc.index(":")]
        redirect_port = result.netloc[(result.netloc.index(":")+1):]
        redirect_path = result.path + "?" + result.query 
        #输出重定向调试信息
        logger.debug("Send redirect to: host: %s, port: %s, path: %s "%(redirect_host, redirect_port, redirect_path))
        #将本地文件传输通过HTTP PUT传输至远程HDFS
        fileUploadClient = httplib.HTTPConnection(redirect_host, redirect_port, timeout=600)
        fileUploadClient.request('PUT', redirect_path, open(source_path, "rb"), headers={})
        response = fileUploadClient.getresponse()
        logger.debug("HTTP Response: %d, %s"%(response.status, response.reason))
        httpClient.close()
        fileUploadClient.close()
        CheckResponseError(response)

    ######################################################################
    def appendToHDFS(self, source_path, target_path):
        #WebHDFS访问URL，设置操作op为APPEND
        url_path = WEBHDFS_CONTEXT_ROOT + target_path + '?op=APPEND&user.name='+self.username        
        httpClient = self.__getNameNodeHTTPClient()
        httpClient.request('POST', url_path , headers={})
        response = httpClient.getresponse()
        logger.debug("HTTP Response: response.status = '%d',  response.reason = '%s', response.msg = '%s'"%
            (response.status, response.reason, response.msg))
        #获得重定向后的目的文件地址，并对其进行解析
        redirect_location = response.msg["location"]
        logger.debug("HTTP Location: %s"%(redirect_location))
        result = urlparse.urlparse(redirect_location)
        redirect_host = result.netloc[:result.netloc.index(":")]
        redirect_port = result.netloc[(result.netloc.index(":")+1):]
        redirect_path = result.path + "?" + result.query
        #输出重定向调试信息
        logger.debug("Send redirect to: host: %s, port: %s, path: %s "%(redirect_host, redirect_port, redirect_path))
        #将本地文件传输通过HTTP POST传输至远程HDFS
        fileUploadClient = httplib.HTTPConnection(redirect_host,redirect_port, timeout=600)
        fileUploadClient.request('POST', redirect_path, open(source_path, "rb"), headers={})
        response = fileUploadClient.getresponse()
        logger.debug("HTTP Response: %d, %s"%(response.status, response.reason))
        httpClient.close()
        fileUploadClient.close()
        CheckResponseError(response)

    ######################################################################
    def copyFromHDFS(self, source_path, target_path, overwrite=False):
        if os.path.isfile(target_path) and overwrite == False:#文件已存在报错
            raise WebHDFSError("File '" + target_path + "' already exists")            
        #WebHDFS访问URL，设置操作op为OPEN
        url_path = WEBHDFS_CONTEXT_ROOT + source_path+'?op=OPEN&user.name='+self.username
        logger.debug("GET URL: %s"%url_path)
        httpClient = self.__getNameNodeHTTPClient()
        httpClient.request('GET', url_path , headers={})
        response = httpClient.getresponse()
        #如果文件不为空
        if response.length != None: 
            msg = response.msg #获得重定向后的目的文件地址，并对其进行解析
            redirect_location = msg["location"]
            logger.debug("HTTP Response: %d, %s"%(response.status, response.reason))
            logger.debug("HTTP Location: %s"%(redirect_location))
            result = urlparse.urlparse(redirect_location)
            redirect_host = result.netloc[:result.netloc.index(":")]
            redirect_port = result.netloc[(result.netloc.index(":")+1):]            
            redirect_path = result.path + "?" + result.query  
            #输出重定向调试信息
            logger.debug("Send redirect to: host: %s, port: %s, path: %s "%(redirect_host, redirect_port, redirect_path))
            #通过HTTP GET获得文件数据
            fileDownloadClient = httplib.HTTPConnection(redirect_host, redirect_port, timeout=600)            
            fileDownloadClient.request('GET', redirect_path, headers={})
            response = fileDownloadClient.getresponse()
            logger.debug("HTTP Response: %d, %s"%(response.status, response.reason))
            #将远程获得的数据按缓存块大小写入本地文件
            rcv_buf_size = 1024*1024            
            target_file = open(target_path, "wb")
            while True : 
                resp = response.read(rcv_buf_size)
                if len(resp) == 0 :
                    break
                target_file.write(resp)                
            target_file.close()
            fileDownloadClient.close()
        else: #如果文件为空返回回复为length == NONE并且没有重定向的URL，不写本地文件
            target_file = open(target_path, "wb")
            target_file.close()
        httpClient.close()        
        CheckResponseError(response)
     
    ######################################################################
    def getFileStatus (self, path):
        url_path = WEBHDFS_CONTEXT_ROOT + path + '?op=GETFILESTATUS&user.name=' + self.username
        httpClient = self.__getNameNodeHTTPClient()
        httpClient.request('GET', url_path , headers={})
        response = httpClient.getresponse()
        data_dict = json.loads(response.read())
        httpClient.close()
        CheckResponseError(response)
        
        try :
            return data_dict['FileStatus']
        except:
            return ''

    ######################################################################
    def listDir(self, path):
        url_path = WEBHDFS_CONTEXT_ROOT +path+'?op=LISTSTATUS&user.name='+self.username
        logger.debug("List directory: " + url_path)
        httpClient = self.__getNameNodeHTTPClient()
        httpClient.request('GET', url_path , headers={})
        response = httpClient.getresponse()
        logger.debug("HTTP Response: %d, %s"%(response.status, response.reason))
        data_dict = json.loads(response.read())
        httpClient.close()
        CheckResponseError(response)
        
        logger.debug("Data: " + str(data_dict))
        files=[]      
        try :
            for i in data_dict["FileStatuses"]["FileStatus"]:
                logger.debug(i["type"] + ": " + i["pathSuffix"]) 
                files.append(i["pathSuffix"])        
        except:
            pass
        return files

    ######################################################################
    def listDirEx(self, path):
        url_path = WEBHDFS_CONTEXT_ROOT +path+'?op=LISTSTATUS&user.name='+self.username
        logger.debug("List directory: " + url_path)
        httpClient = self.__getNameNodeHTTPClient()
        httpClient.request('GET', url_path , headers={})
        response = httpClient.getresponse()
        logger.debug("HTTP Response: %d, %s"%(response.status, response.reason))
        data_dict = json.loads(response.read())
        httpClient.close()
        CheckResponseError(response)
        
        logger.debug("Data: " + str(data_dict))
        try :
            return  data_dict["FileStatuses"]["FileStatus"]
        except:
            return  []

    ######################################################################
    def getHomeDir (self):
        url_path = WEBHDFS_CONTEXT_ROOT + '?op=GETHOMEDIRECTORY&user.name='+self.username
        httpClient = self.__getNameNodeHTTPClient()
        httpClient.request('GET', url_path , headers={})
        response = httpClient.getresponse()
        data_dict = json.loads(response.read())
        httpClient.close()
        CheckResponseError(response)
        
        try :
            return data_dict['Path']
        except:
            return ''
    
    ######################################################################
    def __getNameNodeHTTPClient(self):
        httpClient = httplib.HTTPConnection(self.namenode_host, self.namenode_port, timeout=600)
        return httpClient
    
    
######################################################################
######################################################################
######################################################################
class WebHDFSError(Exception):
    reason = ''
    def __init__(self, reason):
        self.reason = reason
    def __str__(self):
        return self.reason

######################################################################
def CheckResponseError(response):
    if response != None and response.status >= 400 :
        raise WebHDFSError('HTTP ERROR {0}. Reason: {1}'.format(response.status, response.reason))


######################################################################
######################################################################
######################################################################
if __name__ == '__main__':      
    try:
        webhdfs = WebHDFS('storm0', 50070, 'azhigimont')
        webhdfs.mkDir('/user/azhigimont/tmp')
        resp = webhdfs.copyToHDFS('c:/temp/test.json', '/user/azhigimont/tmp/test.json', overwrite = True)
        webhdfs.copyFromHDFS('/user/azhigimont/tmp/test.json',  'c:/temp/test1.json', overwrite = True)
        webhdfs.listDir('/user/azhigimont/tmp')
        webhdfs.delete('/user/azhigimont/tmp', recursive = True)
    except WebHDFSError as whe:
        print whe
    except:
        print "Unexpected error:" + str(sys.exc_info())
    else:
        print '__main__ test completed without errors'