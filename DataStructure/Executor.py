'''
Created on Jul 24, 2015

@author: SDH
'''
from _collections import defaultdict
from DataStructure.RedisClient import queryView, updateOneView


#TODO: Change view format from [grid_id]=list(View) to [feed_id]=set(View)


            

class ViewMaintainer(object):
    '''
    Emit event for feeds
    '''
    DEFAULT_TICK=10 #unit milliseconds
    
    def __init__(self,feeds,views,cluster):
        '''
        Input:
            feeds:  dictionary from feed_id(String) to FeedInfo
            views:  dictionary from feed_id(String) to set of View
            cluster: RedisClusterBuilder object, used for routing
        Output:
            timeSchedulesFeed: dictionary from time_step(int) to list of FeedInfo
            updateGraph: dictionary from feed_id(String) to set of Views
        '''
        self.timeSchedulesFeed = defaultdict(list)
        self.cluster = cluster
        for feedid,feed in feeds.items():
            #Assume feed.update_intv is integer
            #if feed.update_intv not in self.timeSchedulesFeed:
            #    self.timeSchedulesFeed[feed.update_intv]=[]
            self.timeSchedulesFeed[feed.update_intv].append(feed)
        self.updateGraph = views
                
    def execute(self,running_counter):
        '''
        Execute view maintainer for totally running_counter intervals
        '''
        for tick in xrange(1,running_counter):
                for interval,feedlist in self.timeSchedulesFeed.items():
                    if tick % interval == 0:
                        for feed in feedlist:
                            for view in self.updateGraph[feed.feed_id]:
                                redisServer = self.cluster.routing(view.view_id)
                                result = updateOneView(redisServer,view.view_id,tick,tick)
                                result=hash(result)
    def sim(self,running_counter,algorithm):
        '''
            Generate running log under given algorithm and user movements
        '''
        #TODO
        
class QueryProcessor(object):
    '''
    Pull from views
    '''
    DEFAULT_TICK=ViewMaintainer.DEFAULT_TICK #unit milliseconds

    def __init__(self,users,feeds,queryPlan,cluster):
        '''
        Input:
            feeds:  dictionary from feed_id(String) to FeedInfo
            queryPlan:  dictionary from UserInfo to set of View
            users:  dictionary from user_id(String) to UserInfo
            cluster: RedisClusterBuilder object, used for routing
        Output:
            timeSchedulesPull: dictionary from time_step(int) to list of UserInfo
            queryGraph: dictionary from user_id(String) to set of Views or FeedInfo
        '''
        self.timeSchedulesPull = defaultdict(list)
        self.cluster = cluster
        self.queryGraph = queryPlan
        for userid,user in users.items():
            self.timeSchedulesPull[user.query_intv].append(user)
                    
    def execute(self,running_counter):
        '''
        Execute query processor for totally running_counter intervals
        '''
        for tick in xrange(1,running_counter):
                for interval,userlist in self.timeSchedulesPull.items():
                    if tick % interval == 0:
                        for user in userlist:
                            for view in self.queryGraph[user]:
                                redisServer = self.cluster.routing(view.view_id)
                                result = queryView(redisServer,view.view_id)
                                result=hash(result)
                        
            #TODO
        

if __name__ == '__main__':
    pass