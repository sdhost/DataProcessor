'''
Created on Jul 27, 2015

@author: SDH
'''
import redis
from DataStructure.Algorithm import Algorithm


def initCluster():
    hosts = ["192.168.1.102",
             "192.168.1.103",
             "192.168.1.104",
             "192.168.1.106",
             "192.168.1.107",
             "192.168.1.108"]
    serverPort = 6379
    setMaxConnections = 200
    servers = []
    for url in hosts:
        pool = redis.ConnectionPool(host=url,port=serverPort,db=0,max_connections=setMaxConnections)
        r = redis.Redis(connection_pool=pool)
        servers.append(r)
    return servers

def localCluster():
    hosts=["127.0.0.1"]
    serverPort = 6379
    setMaxConnections = 200
    servers = []
    for url in hosts:
        pool = redis.ConnectionPool(host=url,port=serverPort,db=0,max_connections=setMaxConnections)
        r = redis.Redis(connection_pool=pool)
        servers.append(r)
    return servers

class RedisClusterBuilder(object):
    '''
    Routing to each redis server
    '''

    def __init__(self, servers):
        '''
        Input
            List of Redis with a connection pool set
        Store
            server_count, Poll dictionary <Integer,RedisPool>
        '''
        self.server_count = len(servers)
        for i in range(self.server_count):
            self.pool[i] = servers[i]
            
    def routing(self,viewName):
        '''
        Input
            String viewName
        Output
            Redis, the server that could execute the query
        '''
        return self.pool[hash(viewName)%self.server_count]

def queryView(redisServer,viewName):
    returnResult = redisServer.zrange(name=viewName,start=0,end=-1,withscores=True)
    #May need to process the return result before return to clients
    return returnResult

def updateList(redisServer,listName,updateMessage):
    returnResult = redisServer.rpush(listName,updateMessage)
    return returnResult

def updateOneView(redisServer,viewName,timestamp,updateMessage):
    returnResult = redisServer.zadd(viewName,timestamp,updateMessage)
    redisServer.zremrangebyrank(viewName,0,0 - Algorithm.TOPN -1)
    return returnResult

def updateMultiView(redisServer,viewNames,timestamp,updateMessage):
    returnResults = []
    for viewName in viewNames:
        returnResult = updateOneView(redisServer, viewName, timestamp, updateMessage)
        returnResults.append(returnResult)
    return returnResults