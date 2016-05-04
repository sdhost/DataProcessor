'''
Created on Jul 23, 2015

@author: SDH
'''
from DataStructure.Dataset import View, Location, UserInfo, Transaction
from _collections import defaultdict
import collections
import math

TOPN = 5

class GridView(object):
    '''
    Algorithm for grid-view basic solution
    '''


    def __init__(self, feeds,users,userint=None):
        '''
        feeds <-- Dictionary of String(feed_id) to FeedInfo
        users <-- Dictionary of String(user_id) to UserInfo
        user_movement <-- Defaultdict, String(user_id) to List of LocationRecord
        '''
        if feeds:
            self.feeds = feeds
        if users:
            self.users = users
        if userint:
            self.userint = userint
            
    @classmethod        
    def search(cls,feeds,users,dynamic=False):
        '''
        Return the view selection plan using grid-view algorithm
        Assume user query multiple tags, feed also generate event with one tag
        '''
        Algorithm.userFollowDict = defaultdict(set)
        if not (feeds and users):
            print("Not initialized")
            return None
        gridfeedMap = defaultdict(set)
        updateMap = {}
        feedviewMap = defaultdict(set)
        queryMap = {}
        userQueryPlan = defaultdict(set)
        
        
        for feedid,feed in feeds.items():
            #Assign feed to grid
            if feed.feed_id != feedid:
                print("Inconsistent feed id")
            if (feed.loc,feed.tag) not in gridfeedMap:
                gridfeedMap[(feed.loc,feed.tag)] = set()
            gridfeedMap[(feed.loc,feed.tag)].add(feed)
            if (feed.loc,feed.tag) in updateMap:
                updateMap[(feed.loc,feed.tag)] += float(1.0 / feed.update_intv)
            else:
                updateMap[(feed.loc,feed.tag)] = float(1.0 / feed.update_intv)
                
            
        gridToServiceSet = defaultdict(set)
        total_follow = 0
        active_user = 0
        to_remove = []
        for userid,user in users.items():
            #Assign user to feed's service list
            if userid != user.user_id or userid != user.locRecord.uid:
                print("Inconsistent user id: " + userid + " " + user.user_id + " " + user.locRecord.uid)
#             user_grid=user.locRecord
            #query_range=Location.queryGrids(user_grid,user.query_range)
            #Get a list of locations, the user will follow all the feeds within the range contains the tag
            user_tags = user.tags
            #avg_range = 0
            followedfeeds = set()
            for gid,feedset in gridfeedMap.items():
                for tag in user_tags:
                    trange = user.locRecord.getRange(gid[0])
                    #avg_range += trange
                    if(gid[1] == tag and trange < user.query_range):
                        if gid not in gridToServiceSet:
                            gridToServiceSet[gid] = set()
                        gridToServiceSet[gid].add(user)
                        if gid in queryMap:
                            queryMap[gid] += float(1.0 / user.query_intv)
                        else:
                            queryMap[gid] = float(1.0 / user.query_intv)
                        total_follow += len(feedset)
                        followedfeeds = followedfeeds | feedset
            if len(followedfeeds) != 0:
                Algorithm.userFollowDict[user.user_id] = followedfeeds
                active_user += 1
            else:
                if dynamic == False:
                    del users[userid]
                else:
                    Algorithm.userFollowDict[user.user_id] = set()

        viewMap = {}
        view_set = set()
        total_serviced = 0
        active_feeds = 0
        i=0
        for gid,feedset in gridfeedMap.items():
            if gid in gridToServiceSet:
                tmp_view = View("V" + str(i),gridfeedMap[gid],gridToServiceSet[gid])
                tmp_view.locations.add(gid)
                tmp_view.isFeed = True
                tmp_view.setFreq(updateMap[gid],queryMap[gid])
                total_serviced += len(tmp_view.service_set)
                if tmp_view in view_set:
                    print("Error! Duplicate Grid View!")
                view_set.add(tmp_view)
                viewMap["V"+str(i)]=tmp_view
                i += 1
                active_feeds += len(feedset)
                for feed in feedset:
                    if feed.feed_id not in feedviewMap:
                        feedviewMap[feed.feed_id] = set()
                    feedviewMap[feed.feed_id].add(tmp_view)
                for user in gridToServiceSet[gid]:
                    if user not in userQueryPlan:
                        userQueryPlan[user]=set()
                    userQueryPlan[user].add(tmp_view)
            else:
                if dynamic == False:
                    del gridfeedMap[gid]
                    for feed in feedset:
                        del feeds[feed.feed_id]
                else:
                    tmp_view = View("V" + str(i),feedset,set())
                    tmp_view.locations.add(gid)
                    tmp_view.isFeed = True
                    tmp_view.setFreq(updateMap[gid],float(0.0))
                    if tmp_view in view_set:
                        print("Error! Duplicate No Follow Grid View!")
                    view_set.add(tmp_view)
                    viewMap["V"+str(i)]=tmp_view
                    i += 1
                    for feed in feedset:
                        if feed.feed_id not in feedviewMap:
                            feedviewMap[feed.feed_id] = set()
                        feedviewMap[feed.feed_id].add(tmp_view)
        
        #print("Total Follow: " + str(total_follow) + " Total Serviced: " + str(total_serviced))
        #print("Active Users: " + str(active_user) + " Active Feeds: " + str(active_feeds) + "Total Feeds: " + str(i))
        #print("Total View: " + str(len(viewMap.items())) + "\t" + str(len(feedviewMap.items())))
        
        Plan = collections.namedtuple('Plan',['ViewMap','FeedMap','UserMap','FeedViewMap','QueryPlan'])
        P = Plan(ViewMap=viewMap,FeedMap=feeds,UserMap=users,FeedViewMap=feedviewMap,QueryPlan=userQueryPlan)
        
        
        return P
    
    def viewSelectionDynamic(self, tot_interval, filename):
        '''
            Adaptive GridView Algorithm
            Take user_movement(HashMap<UserId,Tuple(Loc,Location)>)
            as input, keep the view selection plan and assign native
            views to users.
            
            Generate execution transactions over time.
            (Assume the timestamp of each location record is normalized)
            Filename+view for update
            Filename+query for processing
        '''
        update_record = open(filename+".view",'w')
        query_record = open(filename+".query",'w')
        cost_record = open(filename+".cost","w")
        '''
            1. Prepare information
                UserLocation initialize (Location.userBegin need to be updated)
                GridView generation
                GridQueryPlan transformation
        '''
        gridStatistic = {}
        userQueryFreq = {}
        userLocStore = {}
#         locOperations = {}
        tot_new = 0
        tot_add = 0
        print("Initial user location size: " + str(len(self.userint[0])))
        for locr in self.userint[0]:
            if locr.uid not in userQueryFreq:
                qfreq = float(1.0 / self.users[locr.uid].query_intv)
                userQueryFreq[locr.uid] = qfreq
            else:
                qfreq = userQueryFreq[locr.uid]
            
            if qfreq < 0:
                print("Negative Raw: " + str(qfreq))
            
            self.users[locr.uid].locRecord = locr
            loc = Location(locr.latitude,locr.longitude)
            if loc in gridStatistic:
                gridStatistic[loc] += qfreq
#                 locOperations[loc] += 1
                tot_add += 1
            else:
                gridStatistic[loc] = qfreq
#                 locOperations[loc] = 1
                tot_new += 1
                
            userLocStore[locr.uid] = loc
            
        print("Total statistic Records: " + str(len(gridStatistic)))
        print("Total New " + str(tot_new) + " Tot Add " + str(tot_add))
        #Update user location to int 0 value, initialize gridStatistic
        
        #InitialPlan
        initialRaw = GridView.search(self.feeds, self.users,True)
        
#         print("User Size: " + str(len(self.users)) + " Feed Size: " + str(len(self.feeds)))
        
        gView = dict()
        viewMap = dict()
        gridViewMap = defaultdict(set)
        for fid,viewset in initialRaw.FeedViewMap.items():
            if len(viewset) == 0:
                print("Empty FeedView Error")
            else:
                loc = initialRaw.FeedMap[fid].loc
                if len(viewset) > 1:
                    print("Multiple View for one Feed")
                for view in viewset:
                    view.setRegion(Location.queryGrids(loc,UserInfo.default_range))
                    #print("Region Size: " + str(len(view.coverageRegion)))
                    gView[loc] = view
                    viewMap[view.view_id] = view
                    for grid in view.coverageRegion:
#                         if grid in gridStatistic:
                        if grid not in gridViewMap:
                            vset = set()
                            gridViewMap[grid] = vset
                        gridViewMap[grid].add(view)
#         print("After Process gView size: " + str(len(gView.items())))
#         print("Grid View Map size: " + str(len(gridViewMap.items())))
#         print("ViewMap size: " + str(len(viewMap.items())))
        gridPlan = defaultdict(set)
        for user,viewset in initialRaw.QueryPlan.items():
            loc = Location(user.locRecord.latitude,user.locRecord.longitude)
            gridPlan[loc] = viewset
        print("Grid Plan size: " + str(len(gridPlan.items())))
        
            
            
        DPlan = collections.namedtuple('Plan',['ViewMap','GridPlan','GridStatistic','GridView','GridViewMap','FeedViewMap'])   
        cplan = DPlan(ViewMap=viewMap,GridPlan=gridPlan,GridStatistic=gridStatistic,GridView=gView,GridViewMap=gridViewMap,FeedViewMap=initialRaw.FeedViewMap)
        
        planCost = Algorithm.calcDynamicCost(cplan, gridStatistic)
        print("Initial GridView Cost: Mat = " + str(planCost[0]) + " Eva: " + str(planCost[1]) + " Total: " + str(planCost[2]))
        cline = "Interval\tPushCost\tPullCost\tTotalCost"
        cost_record.write(cline+"\n")
        cline = str(0) + "\t" + str(planCost[0]) + "\t" + str(planCost[1]) + "\t" + str(planCost[2])
        cost_record.write(cline+"\n")
        
        '''
            Update timeSchedulesQuery, HashMap from Interval to List of String(UserID)
            Update timeSchedulesUpdate, HashMap from Interval to List of String(ViewID)
        '''
        timeSchedulesQuery = defaultdict(list)
        timeSchedulesUpdate = defaultdict(list)
        
        count = 0
        maxint = 0
        totint = 0
        
        for uid,user in self.users.items():
            uint = int(user.query_intv)
            totint += uint
            count += 1
            if uint > maxint:
                maxint = uint
            if uint not in timeSchedulesQuery:
                timeSchedulesQuery[uint] = list()
            timeSchedulesQuery[uint].append(user.user_id)
       
        count = 0
        maxint = 0
        totint = 0
        
        for fid,feed in self.feeds.items():
            fint = int(feed.update_intv)
            totint += fint
            count += 1
            if fint > maxint:
                maxint = fint
            if fint not in timeSchedulesUpdate:
                timeSchedulesUpdate[fint] = list()
            timeSchedulesUpdate[fint].append(feed.feed_id)
                            
        
        count = 0
        maxint = 0
        loop = 1
        size = len(self.userint) - 1
        querycount = 0
        updatecount = 0
        tot_change_dist = 0
        for i in range(1,tot_interval+1):
            ChangedPlans = defaultdict(list)
            if (i % 500 == 0):
                print("Interval :" + str(i) + "/" + str(tot_interval))
                print("Write :" + str(updatecount) + "\t" + "Query: " + str(querycount))
                print("Tot Dist: " + str(tot_change_dist))
            
            for uintf,flist in timeSchedulesUpdate.items():
                if i % uintf == 0:
                    for fid in flist:
                        vlist = list()
                        for view in cplan.FeedViewMap[fid]:
                            vlist.append(view.view_id)
                        if len(vlist) == 0:
                            print("Empty Update List!")
                        t = Transaction(i,vlist,str(i),True)
                        updatecount += 1
                        update_record.write(t.toStr()+"\n")
                        
            
            for qintu,ulist in timeSchedulesQuery.items():
                if i % qintu == 0:
                    for uid in ulist:
                        uloc = userLocStore[uid]
                        vlist = list()
                        if uloc not in cplan.GridPlan and uloc in gridStatistic and gridStatistic[uloc] > 0:
                            oldplan = list()
                            oldplan.append(uid+"old")
                            t = Transaction(i,oldplan,"Planning",True)
                            querycount += 1
                            query_record.write(t.toStr()+"\n")
                            cplan.GridPlan[uloc] = Algorithm.queryPlanningGrid(uloc, self.users[uid].query_range, cplan.GridViewMap)
                            oldplan.remove(uid+"old")
                            oldplan.append(uid+"new")
                            t = Transaction(i,oldplan,"Planning",True)
                            querycount += 1
                            query_record.write(t.toStr()+"\n")
                        elif uloc not in cplan.GridPlan:
                            print("No Statistic User!")
                            continue
                        for view in cplan.GridPlan[uloc]:
                            vlist.append(view.view_id)
                        if len(vlist) == 0:
                            continue 
                        t = Transaction(i,vlist)
                        querycount += 1
                        query_record.write(t.toStr()+"\n")
                            
            
            #UpdatingView Selection Plan
            if count >= size:
                modi = i % maxint
                loop += 1
                count = 0
                print("Finished at: " + str(count) + ", Generated Update" + str(updatecount) + "\t" + "Query: " + str(querycount))
            else:
                modi = i
            
                
            
            if modi in self.userint:
                count += 1
                if count >= size and loop == 1:
                    maxint = i
                tot_new = 0
                tot_add = 0
                tot_rec = 0    
                for locr in self.userint[modi]:
                    qloc = userLocStore[locr.uid]
                    self.users[locr.uid].locRecord = locr
                    loc = Location(locr.latitude,locr.longitude)
                    userLocStore[locr.uid] = loc
                    tot_change_dist += loc.getRange(qloc)
                    
                gridStatistic.clear()
                for uid,loc in userLocStore.items():
                    if loc not in gridStatistic:
                        gridStatistic[loc] = 0
                    gridStatistic[loc] += userQueryFreq[uid]
                tot_rec = len(gridStatistic)
                
#                 print("Total New " + str(tot_new) + " Tot Add " + str(tot_add) + " Tot Sub " + str(tot_sub))
                
#                 if newCost[2] / lastCost[2] >= GreedyAlgorithm.ChangeThreshold:
                if tot_change_dist > Algorithm.CHANGED_DISTANCE:
                    print("Changed Distance: " + str(tot_change_dist) + " in interval " + str(i))
                    print("Total Statistic Grids: " + str(tot_rec))
                    lastCost = Algorithm.calcDynamicCost(cplan,gridStatistic)
                    tot_change_dist = 0
                    print("Current Plan Cost: Mat = " + str(lastCost[0]) + " Eva: " + str(lastCost[1]) + " Total: " + str(lastCost[2]))
                    cline = str(i) + "\t" + str(lastCost[0]) + "\t" + str(lastCost[1]) + "\t" + str(lastCost[2])
                    cost_record.write(cline+"\n")
                    

class Algorithm(object):
    
    TOPN = 5
    PushPullRatio = float(2.83)
    planDict = {}
    userFollowDict = defaultdict(set)
    gridFollowDict = defaultdict(set)
    int_vid_counter = 0
    CHANGED_DISTANCE = 25000
     
    def __init__(self, feeds,users,userint=None):
        '''
        feeds <-- Dictionary of String(feed_id) to FeedInfo
        users <-- Dictionary of String(user_id) to UserInfo
        userint <-- Defaultdict, Interval to List of LocationRecord
        '''
        if feeds:
            self.feeds = feeds
        if users:
            self.users = users
        if userint:
            self.userint = userint
            
    @classmethod
    def queryPlanning(cls,user,feedMap,feedviewMap):
        '''
        user     <--  UserInfo Object
        feedMap  <--  Dictionary of tuple (gid,tag) (Location,String) to set of FeedInfo
        feedviewMap  <--  Dictionary of String(feed_id) to set of View maintaining the feed
                      Sorting by the number of feeds maintained in view
        '''
        user_grid=user.locRecord
        #query_range=Location.queryGrids(user_grid,user.query_range)
        user_tags = user.tags
        wholeset = set()
        coverset = set()
        queryPlan = set()
        #print("Begin cover")
        if user.user_id in Algorithm.userFollowDict:
            wholeset = Algorithm.userFollowDict[user.user_id]
        else:
            for fid,feedset in feedMap.items():
                if fid[1] in user_tags and fid[0].getRange(user_grid) < user.query_range:
                    for feed in feedset:
                        wholeset.add(feed)
            Algorithm.userFollowDict[user.user_id] = wholeset
                    #print("FeedID: " + str(feed.feed_id))
        #Assign the feed set followed by this user
        #print("Candidate set size:" + str(len(wholeset)))
        for feed in wholeset:
            if coverset == wholeset:
                break
            if feed in coverset:
                continue           
            for view in sorted(feedviewMap[feed.feed_id],key=lambda t:len(t.feed_set),reverse=True):
                #print("ViewID: " + str(view.view_id))
                if view.feed_set.issubset(wholeset) and not view.feed_set.issubset(coverset):
                    coverset = coverset.union(view.feed_set)
                    queryPlan.add(view)
                    continue
        #print("Covered set size:" + str(len(coverset)))
        if coverset == wholeset:
            return queryPlan
        else:
            print("Planning Error!")
            return None
    
    @classmethod
    def queryPlanningGrid(cls,grid, qrange, gridviewMap):
        '''
        grid     <--  Location Object
        qrange    <--  Query range of the users within grid
        gridviewMap  <--  Dictionary of Location(grid) to set of View covering the grid
                      Sorting by the number of feeds maintained in view
        '''
        if grid in Algorithm.gridFollowDict:
            wholeset = Algorithm.gridFollowDict[grid]
        else:
            wholeset = set()
            for view in gridviewMap[grid]:
                #wholeset |= view.feed_set
                wholeset |= view.locations
            Algorithm.gridFollowDict[grid] = wholeset
            
        coverset = set()
        queryPlan = set()

#         size = 1000
#         for feed in wholeset:
        #for view in sorted(gridviewMap[grid],key=lambda t:len(t.feed_set),reverse=True):
        for view in sorted(gridviewMap[grid],key=lambda t:len(t.locations),reverse=True):
#             if len(view.feed_set) <= size:
#                 size = len(view.feed_set)
#             else:
#                 print("Order Error, last size: " + str(size) + "\t Current Size: " + str(len(view.feed_set)))
            if coverset == wholeset:
                break         
            #if view.feed_set.issubset(wholeset) and not view.feed_set.issubset(coverset):
            if view.locations.issubset(wholeset) and not view.locations.issubset(coverset):
                #coverset = coverset.union(view.feed_set)
                coverset = coverset.union(view.locations)
                queryPlan.add(view)
                continue
                
        if coverset == wholeset:
#             if len(queryPlan) != 0:
#                 print("Successful cover with size: " + str(len(queryPlan)))
            return queryPlan
        else:
            print("Grid Planning Error! to Cover Size: " + str(len(wholeset)) + " Covered Size: " + str(len(coverset)))
            #for feed in wholeset - coverset:
            #    print("Uncovered feed: " + feed.toStr())
            return None
    
#     @classmethod
#     def assignUser(cls,views,gridFreq):
#         '''
#             Generate service lists with a given gridFreq for current materialized views
#             TODO
#         '''
    @classmethod
    def calcBenefitGridByPlan(cls, view, gridviews, gridStatistic, gridPlan):
        matCost = view.update_freq
        mevaCost = float(0)
        newGridView = gridviews.copy()
        for grid in view.coverageRegion:
            if grid not in gridStatistic or gridStatistic[grid] <= 0:
                continue
            elif grid not in gridPlan:
                continue
            newGridView[grid].add(view)
            preSize = len(gridPlan[grid])
            newPlan = Algorithm.queryPlanningGrid(grid, UserInfo.default_range, newGridView)
            afterSize = len(newPlan)
            if afterSize < preSize:
                if afterSize == 1:
                    mevaCost += preSize * gridStatistic[grid]
                else:
                    mevaCost += (preSize - afterSize) * gridStatistic[grid]
                
        return mevaCost-matCost*Algorithm.PushPullRatio 
    
    @classmethod
    def calcBenefitGrid(cls, view, gridviews, gridStatistic, gridPlan):
        matCost = view.update_freq
        mevaCost = float(0)
        for grid in view.coverageRegion:
            if grid not in gridStatistic or gridStatistic[grid] <= 0:
                continue
            elif grid not in gridPlan:
                continue
            toRemove = set()
            for vi in gridPlan[grid]:
                if vi.feed_set.issubset(view.feed_set):
                    toRemove.add(vi)
            if toRemove == gridPlan[grid]:
                mevaCost += len(toRemove) * gridStatistic[grid]
            else:
                mevaCost += (len(toRemove) - 1.0) * gridStatistic[grid]
                
        return mevaCost-matCost*Algorithm.PushPullRatio 
    
    @classmethod
    def calcBenefitOpt(cls,view,queryPlan):
        '''
            Calculate the benefit of view under the queryPlan
            Optimized Version O(MaxFollowedPerUser)
        '''
        matCost = view.update_freq
        mevaCost = float(0)
        
#         ifmap = feedviewMap.copy()
#         for feed in view.feed_set:
#             ifmap[feed.feed_id].add(view)
#             if view in feedviewMap[feed.feed_id]:
#                 print("Error! Copy failed")
        
        for user in view.service_set:
#             cPlan = queryPlan[user]
#             ifPlan = Algorithm.queryPlanning(user, feedMap, ifmap)
#             improve = len(cPlan-ifPlan)
            toRemove = set()
            for vi in queryPlan[user]:
                if vi.feed_set.issubset(view.feed_set):
                    toRemove.add(vi)
            if toRemove == queryPlan[user]:
#             if improve > 0 and len(ifPlan) == 1:
                #User query pushed
                mevaCost += len(toRemove) * float(1.0/user.query_intv)
            elif len(toRemove) > 0:
                mevaCost += float(len(toRemove) - 1.0) * float(1.0/user.query_intv)
        
        return mevaCost-matCost*Algorithm.PushPullRatio 
    
    @classmethod
    def calcBenefitApx(cls,view):
        '''
            Calculate the benefit of view under the queryPlan
            Approximation Version O(1)
        '''
        matCost = view.update_freq
        mevaCost = view.query_freq * float((len(view.feed_set) - 1))
        
        return mevaCost-matCost*Algorithm.PushPullRatio 
    
    @classmethod
    def calcCost(cls,plan):
        views = plan.ViewMap
        queryPlans = plan.QueryPlan
        matCost = 0
        evaCost = 0
        for vid,view in views.items():
            matCost += view.update_freq
        
        for user,links in queryPlans.items():
            if len(links) > 1:
                evaCost += float(1.0/user.query_intv) * len(links)
        
        return (matCost,evaCost,matCost * Algorithm.PushPullRatio + evaCost)
    
    @classmethod
    def calcDynamicCost(cls, plan, stat):
        views = plan.ViewMap
        matCost = 0
        evaCost = 0
        
        for vid,view in views.items():
            matCost += view.update_freq
            
        for grid,links in plan.GridPlan.items():
            if len(links) > 1:
                if grid in stat:
                    evaCost += stat[grid] * float(len(links))
                else:
                    continue
                
        return (matCost,evaCost,matCost * Algorithm.PushPullRatio + evaCost)
        

class FeedingFrenzy(Algorithm):
    '''
        Algorithm for FeedingFrenzy per-pair algorithm
        use our H/L cost ratio
    '''
    def __init__(self,feeds,users,user_int=None):
        Algorithm.__init__(self, feeds, users, user_int)
    
    def viewSelection(self):
        '''
            Steps:
               1. Create native views from feed set
               2. Create user view based on the cost function and epsilon
               3. Assign query plan for users
        '''
        Algorithm.userFollowDict = defaultdict(set)
        viewMap = {}
        all_view_set = set()
        #gridfeedMap = defaultdict(set)
        feedviewMap = defaultdict(set)
        queryPlan = defaultdict(set)
        #all_feeds = set()
        #avg_size = 0
        
        #updateMap = {}
        
        
        for feedid,feed in self.feeds.items():
            service_set = set()
            ufreq = float(1.0/feed.update_intv)
            feedset = set()
            feedset.add(feed)
            new_view = View("V"+str(feedid),feedset,service_set,ufreq)
            if new_view in all_view_set:
                print("Duplicate Feed!")
            all_view_set.add(new_view)
            viewMap[new_view.view_id]=new_view
            viewset = set()
            viewset.add(new_view)
            for feed in feedset:
                if feed not in feedviewMap:
                    feedviewMap[feed.feed_id] = viewset
                else:
                    print("Feed in multiple grids")
            #Step1 finished
        emptyplan = 0 
        for uid,user in self.users.items():
#             user_grid=user.loc
            user_tags = user.tags
            followedfeeds = set()
            for fid,feed in self.feeds.items():
                trange = user.locRecord.getRange(feed.loc)
                if(trange < user.query_range):
                    followedfeeds.add(feed)
            pullset = set()
            pushset = set()
            upull_cost = float(1.0/user.query_intv)
            viewset = set()#Query plans
            for feed in followedfeeds:
                upush_cost = float(1.0/feed.update_intv)
                if(upull_cost/upush_cost >= Algorithm.PushPullRatio):
                    pushset.add(feed)
                else:
                    pullset.add(feed)
                    viewset.add(viewMap["V"+str(feed.feed_id)])
                    #viewMap["V"+str(feed.feed_id)].service_set.add(user)
            if len(pushset) != 0:
                service_set = set()
                service_set.add(user)
                ufreq = upull_cost
                new_view = View("VU"+str(uid),pushset,service_set,ufreq)
                if new_view in all_view_set:
                    for view in all_view_set:
                        if new_view == view:
                            view.service_set.add(user)
                            viewset.add(viewMap[view.view_id])
                            viewMap[view.view_id].query_freq += ufreq
                else:
                    new_view.query_freq = ufreq
                    viewset.add(new_view)
                    all_view_set.add(new_view)
                    viewMap[new_view.view_id]=new_view
                    for feed in pushset:
                        feedviewMap[feed.feed_id].add(new_view)
                       
            if len(followedfeeds) != 0:
                queryPlan[user]=viewset    
                Algorithm.userFollowDict[user.user_id] = followedfeeds
            else:
                emptyplan += 1
                queryPlan[user]=set()
                Algorithm.userFollowDict[user.user_id] = set()
        
#         print("GeoFeed Active Users: " + str(len(self.users)) + "\tActive Feeds" + str(len(all_feeds)))
#         print("GeoFeed Total views: " + str(len(viewMap)))
        print('Empty Plans: ' + str(emptyplan))        
        Plan = collections.namedtuple('Plan',['ViewMap','FeedMap','UserMap','FeedViewMap','QueryPlan'])
        P = Plan(ViewMap=viewMap,FeedMap=self.feeds,UserMap=self.users,FeedViewMap=feedviewMap,QueryPlan=queryPlan)
        planCost = Algorithm.calcCost(P)
        print("FeedingFrenzy Cost: Mat = " + str(planCost[0]) + " Eva: " + str(planCost[1]) + " Total: " + str(planCost[2]))
        
        return P
    
    def viewSelectionDynamic(self, tot_interval, filename):
        '''
            Adaptive FeedingFrenzy Algorithm
            Take user_movement(HashMap<UserId,Tuple(Loc,Location)>)
            as input, Adaptively change the view selection plan and assign native
            views to users.
            
            Generate execution transactions over time.
            (Assume the timestamp of each location record is normalized)
            Filename+view for update
            Filename+query for processing
        '''
        update_record = open(filename+".view",'w')
        query_record = open(filename+".query",'w')
        '''
            1. Prepare information
                UserLocation initialize (Location.userBegin need to be updated)
                GridView generation
                GridQueryPlan transformation
        '''
        gridStatistic = {}
        userQueryFreq = {}
        userLocStore = {}
#         locOperations = {}
        tot_new = 0
        tot_add = 0
        print("Initial user location size: " + str(len(self.userint[0])))
        for locr in self.userint[0]:
            if locr.uid not in userQueryFreq:
                qfreq = float(1.0 / self.users[locr.uid].query_intv)
                userQueryFreq[locr.uid] = qfreq
            else:
                qfreq = userQueryFreq[locr.uid]
            
            if qfreq < 0:
                print("Negative Raw: " + str(qfreq))
            
            self.users[locr.uid].locRecord = locr
            loc = Location(locr.latitude,locr.longitude)
            if loc in gridStatistic:
                gridStatistic[loc] += qfreq
#                 locOperations[loc] += 1
                tot_add += 1
            else:
                gridStatistic[loc] = qfreq
#                 locOperations[loc] = 1
                tot_new += 1
                
            userLocStore[locr.uid] = loc
            
        print("Total statistic Records: " + str(len(gridStatistic)))
        print("Total New " + str(tot_new) + " Tot Add " + str(tot_add))
        #Update user location to int 0 value, initialize gridStatistic
        
        #InitialPlan
        alg = FeedingFrenzy(self.feeds, self.users)
        cplan = alg.viewSelection()
#         print("After Process gView size: " + str(len(gView.items())))
#         print("Grid View Map size: " + str(len(gridViewMap.items())))
#         print("ViewMap size: " + str(len(viewMap.items())))
        #gridPlan = defaultdict(set)
        #for user,viewset in initialRaw.QueryPlan.items():
        #    loc = Location(user.locRecord.latitude,user.locRecord.longitude)
        #    gridPlan[loc] = viewset
        #print("Grid Plan size: " + str(len(gridPlan.items())))
        
            
            
        '''
            Use user-centric query plans in FeedingFrenzy solution
        '''
        
        '''
            Update timeSchedulesQuery, HashMap from Interval to List of String(UserID)
            Update timeSchedulesUpdate, HashMap from Interval to List of String(ViewID)
        '''
        timeSchedulesQuery = defaultdict(list)
        timeSchedulesUpdate = defaultdict(list)
        
        count = 0
        maxint = 0
        totint = 0
        
        for uid,user in self.users.items():
            uint = int(user.query_intv)
            totint += uint
            count += 1
            if uint > maxint:
                maxint = uint
            if uint not in timeSchedulesQuery:
                timeSchedulesQuery[uint] = list()
            timeSchedulesQuery[uint].append(user.user_id)
       
        count = 0
        maxint = 0
        totint = 0
        
        for fid,feed in self.feeds.items():
            fint = int(feed.update_intv)
            totint += fint
            count += 1
            if fint > maxint:
                maxint = fint
            if fint not in timeSchedulesUpdate:
                timeSchedulesUpdate[fint] = list()
            timeSchedulesUpdate[fint].append(feed.feed_id)
                            
        
        count = 0
        maxint = 0
        last_search = 1
        loop = 1
        size = len(self.userint) - 1
        querycount = 0
        updatecount = 0
        tot_change_dist = 0
        for i in range(1,tot_interval+1):
            if (i % 100 == 0):
                print("Interval :" + str(i) + "/" + str(tot_interval))
                print("Write :" + str(updatecount) + "\t" + "Query: " + str(querycount))
                print("Tot Dist: " + str(tot_change_dist))
            
            for uintf,flist in timeSchedulesUpdate.items():
                if i % uintf == 0:
                    for fid in flist:
                        vlist = list()
                        for view in cplan.FeedViewMap[fid]:
                            vlist.append(view.view_id)
                        if len(vlist) == 0:
                            print("Empty Update List!")
                        t = Transaction(i,vlist,str(i),True)
                        updatecount += 1
                        update_record.write(t.toStr()+"\n")
                        
            
            for qintu,ulist in timeSchedulesQuery.items():
                if i % qintu == 0:
                    for uid in ulist:
                        vlist = list()
                        if self.users[uid].locRecord == userLocStore[uid]:
                            for view in cplan.QueryPlan[self.users[uid]]:
                                vlist.append(view.view_id)
                        else:
                            oldplan = list()
                            oldplan.append(uid+"old")
                            t = Transaction(i,oldplan,"Planning",True)
                            querycount += 1
                            query_record.write(t.toStr()+"\n")
                            followedfeeds = set()
                            for fid,feed in self.feeds.items():
                                trange = user.locRecord.getRange(feed.loc)
                                if(trange < user.query_range):
                                    followedfeeds.add(feed)
                                    vlist.append(cplan.ViewMap["V"+str(fid)])
                            oldplan.remove(uid+"old")
                            oldplan.append(uid+"new")
                            t = Transaction(i,oldplan,"Planning",True)
                            querycount += 1
                            query_record.write(t.toStr()+"\n")
                            cplan.QueryPlan[self.users[uid]] = set(vlist)
                            del vlist[:]
                            for view in cplan.QueryPlan[self.users[uid]]:
                                vlist.append(view.view_id)
                            userLocStore[uid] = Location(self.users[uid].locRecord.latitude,self.users[uid].locRecord.longitude)
                        if len(vlist) <= 1:
                            continue 
                        t = Transaction(i,vlist)
                        querycount += 1
                        query_record.write(t.toStr()+"\n")
                            
            
            #UpdatingView Selection Plan
            if count >= size:
                modi = i % maxint
                loop += 1
                count = 0
                print("Finished at: " + str(count) + ", Generated Update" + str(updatecount) + "\t" + "Query: " + str(querycount))
            else:
                modi = i
            
                
           #
           #    Comment for simulate nomovement FF static
           # 
            if modi in self.userint:
                count += 1
                if count >= size and loop == 1:
                    maxint = i
                tot_new = 0
                tot_add = 0
                tot_rec = 0    
                for locr in self.userint[modi]:
                    qloc = userLocStore[locr.uid]
                    self.users[locr.uid].locRecord = locr
                    loc = Location(locr.latitude,locr.longitude)
                    tot_change_dist += loc.getRange(qloc)
                     
                gridStatistic.clear()
                for uid,loc in userLocStore.items():
                    if loc not in gridStatistic:
                        gridStatistic[loc] = 0
                    gridStatistic[loc] += userQueryFreq[uid]
                tot_rec = len(gridStatistic)
                
                #print("Total New " + str(tot_new) + " Tot Add " + str(tot_add) + " Tot Sub " + str(tot_sub))
                
#                 if newCost[2] / lastCost[2] >= GreedyAlgorithm.ChangeThreshold:
                if tot_change_dist > Algorithm.CHANGED_DISTANCE and last_search - i >= 10:
                    print("Changed Distance: " + str(tot_change_dist) + " in interval " + str(i))
                    print("Total Statistic Grids: " + str(tot_rec))
                    lastCost = Algorithm.calcCost(cplan)
                    print("Current Plan Cost: Mat = " + str(lastCost[0]) + " Eva: " + str(lastCost[1]) + " Total: " + str(lastCost[2]))
                    tot_change_dist = 0
                    alg = FeedingFrenzy(self.users,self.feeds)
                    cplan = alg.viewSelection()
                    lastCost = Algorithm.calcCost(cplan)
                    print("New Plan Cost: Mat = " + str(lastCost[0]) + " Eva: " + str(lastCost[1]) + " Total: " + str(lastCost[2]))
                    last_search = i

class GeoFeedAlgorithm(Algorithm):
    '''
        Algorithm for GeoFeed algorithm
        with a parameter epsilon as the user's latency requirement
    '''
    def __init__(self,feeds,users,epsilon = None,userint=None):
        Algorithm.__init__(self, feeds, users, userint)
        self.epsilon = epsilon#None stand for infinite, only minimize the system overhead

    
    def viewSelection(self):
        '''
            Steps:
                1. Create native views from feed set
                2. Assign query plan for users
        '''
        Algorithm.userFollowDict = defaultdict(set)
        viewMap = {}
        all_view_set = set()
        gridfeedMap = defaultdict(set)
        feedviewMap = defaultdict(set)
        queryPlan = defaultdict(set)
        all_feeds = set()
        avg_size = 0
        
        updateMap = {}
        
        
        for feedid,feed in self.feeds.items():
            service_set = set()
            ufreq = float(1.0/feed.update_intv)
            feedset = set()
            feedset.add(feed)
            new_view = View("V"+str(feedid),feedset,service_set,ufreq)

            new_view.isFeed = True
            if new_view in all_view_set:
                print("Duplicate Feed!")
            all_view_set.add(new_view)
            viewMap[new_view.view_id]=new_view
            viewset = set()
            viewset.add(new_view)
            for feed in feedset:
                if feed not in feedviewMap:
                    feedviewMap[feed.feed_id] = viewset
                else:
                    print("Feed in multiple grids")
            
        for uid,user in self.users.items():
#             user_grid=user.loc
            user_tags = user.tags
            followedfeeds = set()
            for fid,feed in self.feeds.items():
                trange = user.locRecord.getRange(feed.loc)
                if(trange < user.query_range):
                    followedfeeds.add(feed)
            if len(followedfeeds) != 0:
                coveredView = set()
                view_set = set()
                for feed in followedfeeds:
                    fvid = "V"+str(feed.feed_id)
                    viewMap[fvid].query_freq += float(1.0/user.query_intv)
                    viewMap[fvid].service_set.add(user)
                    view_set.add(viewMap[fvid])
                queryPlan[user]=view_set    
                Algorithm.userFollowDict[user.user_id] = followedfeeds
            else:
                queryPlan[user]=set()
                Algorithm.userFollowDict[user.user_id] = set()
        
#         print("GeoFeed Active Users: " + str(len(self.users)) + "\tActive Feeds" + str(len(all_feeds)))
#         print("GeoFeed Total views: " + str(len(viewMap)))
                
        Plan = collections.namedtuple('Plan',['ViewMap','FeedMap','UserMap','FeedViewMap','QueryPlan'])
        P = Plan(ViewMap=viewMap,FeedMap=self.feeds,UserMap=self.users,FeedViewMap=feedviewMap,QueryPlan=queryPlan)
        #planCost = Algorithm.calcCost(P)
        #print("GeoFeed Cost: Mat = " + str(planCost[0]) + " Eva: " + str(planCost[1]) + " Total: " + str(planCost[2]))
        
        return P
    
   
    def viewSelectionDynamic(self, tot_interval, filename):
        '''
            Adaptive GeoFeed Algorithm
            Take user_movement(HashMap<UserId,Tuple(Loc,Location)>)
            as input, Adaptively change the view selection plan and assign native
            views to users.
            
            Generate execution transactions over time.
            (Assume the timestamp of each location record is normalized)
            Filename+view for update
            Filename+query for processing
        '''
        update_record = open(filename+".view",'w')
        query_record = open(filename+".query",'w')
        '''
            1. Prepare information
                UserLocation initialize (Location.userBegin need to be updated)
                GridView generation
                GridQueryPlan transformation
        '''
        #gridStatistic = {}
        userQueryFreq = {}
        userLocStore = {}
#         locOperations = {}
        tot_new = 0
        tot_add = 0
        print("Initial user location size: " + str(len(self.userint[0])))
        for locr in self.userint[0]:
            if locr.uid not in userQueryFreq:
                qfreq = float(1.0 / self.users[locr.uid].query_intv)
                userQueryFreq[locr.uid] = qfreq
            else:
                qfreq = userQueryFreq[locr.uid]
            
            if qfreq < 0:
                print("Negative Raw: " + str(qfreq))
            
            self.users[locr.uid].locRecord = locr
            loc = Location(locr.latitude,locr.longitude)
            #if loc in gridStatistic:
            #    gridStatistic[loc] += qfreq
#                 locOperations[loc] += 1
            #    tot_add += 1
            #else:
            #    gridStatistic[loc] = qfreq
#                 locOperations[loc] = 1
            #    tot_new += 1
                
            userLocStore[locr.uid] = loc
            
        #print("Total statistic Records: " + str(len(gridStatistic)))
        #print("Total New " + str(tot_new) + " Tot Add " + str(tot_add))
        #Update user location to int 0 value, initialize gridStatistic
        
        #InitialPlan
        alg = GeoFeedAlgorithm(self.feeds, self.users)
        cplan = alg.viewSelection()
        
#         print("User Size: " + str(len(self.users)) + " Feed Size: " + str(len(self.feeds)))
        
#         gViewSet = defaultdict(set)
#         viewMap = dict()
#         gridViewMap = defaultdict(set)
#         for fid,viewset in initialRaw.FeedViewMap.items():
#             if len(viewset) == 0:
#                 print("Empty FeedView Error")
#             else:
#                 loc = initialRaw.FeedMap[fid].loc
# #                 if len(viewset) > 1:
# #                     print("Multiple View for one Feed")
#                 for view in viewset:
#                     view.setRegion(Location.queryGrids(loc,UserInfo.default_range))
#                     #print("Region Size: " + str(len(view.coverageRegion)))
#                     if view.isFeed:
#                         if loc not in gViewSet:
#                             gViewSet[loc] = set()
#                         gViewSet[loc].add(view)
#                     viewMap[view.view_id] = view
#                     for grid in view.coverageRegion:
# #                         if grid in gridStatistic:
#                         if grid not in gridViewMap:
#                             vset = set()
#                             gridViewMap[grid] = vset
#                         gridViewMap[grid].add(view)
# #         print("After Process gView size: " + str(len(gView.items())))
# #         print("Grid View Map size: " + str(len(gridViewMap.items())))
# #         print("ViewMap size: " + str(len(viewMap.items())))
#         gridPlan = defaultdict(set)
#         for user,viewset in initialRaw.QueryPlan.items():
#             loc = Location(user.locRecord.latitude,user.locRecord.longitude)
#             gridPlan[loc] = viewset
#         print("Grid Plan size: " + str(len(gridPlan.items())))
        
            
            
#         DPlan = collections.namedtuple('Plan',['ViewMap','GridPlan','GridStatistic','GridView','GridViewMap','FeedViewMap'])   
#         cplan = DPlan(ViewMap=viewMap,GridPlan=gridPlan,GridStatistic=gridStatistic,GridView=gViewSet,GridViewMap=gridViewMap,FeedViewMap=initialRaw.FeedViewMap)
        
        
        '''
            Update timeSchedulesQuery, HashMap from Interval to List of String(UserID)
            Update timeSchedulesUpdate, HashMap from Interval to List of String(ViewID)
        '''
        timeSchedulesQuery = defaultdict(list)
        timeSchedulesUpdate = defaultdict(list)
        
        count = 0
        maxint = 0
        totint = 0
        
        for uid,user in self.users.items():
            uint = int(user.query_intv)
            totint += uint
            count += 1
            if uint > maxint:
                maxint = uint
            if uint not in timeSchedulesQuery:
                timeSchedulesQuery[uint] = list()
            timeSchedulesQuery[uint].append(user.user_id)
       
        count = 0
        maxint = 0
        totint = 0
        
        for fid,feed in self.feeds.items():
            fint = int(feed.update_intv)
            totint += fint
            count += 1
            if fint > maxint:
                maxint = fint
            if fint not in timeSchedulesUpdate:
                timeSchedulesUpdate[fint] = list()
            timeSchedulesUpdate[fint].append(feed.feed_id)
                            
        
        count = 0
        maxint = 0
        loop = 1
        size = len(self.userint) - 1
        querycount = 0
        updatecount = 0
        tot_change_dist = 0
        for i in range(1,tot_interval+1):
            if (i % 500 == 0):
                print("Interval :" + str(i) + "/" + str(tot_interval))
                print("Write :" + str(updatecount) + "\t" + "Query: " + str(querycount))
                print("Tot Dist: " + str(tot_change_dist))
            
            for uintf,flist in timeSchedulesUpdate.items():
                if i % uintf == 0:
                    for fid in flist:
                        vlist = list()
                        for view in cplan.FeedViewMap[fid]:
                            vlist.append(view.view_id)
                        if len(vlist) == 0:
                            print("Empty Update List!")
                        t = Transaction(i,vlist,str(i),True)
                        updatecount += 1
                        update_record.write(t.toStr()+"\n")
                        
            
            for qintu,ulist in timeSchedulesQuery.items():
                if i % qintu == 0:
                    for uid in ulist:
                        vlist = list()
                        if self.users[uid].locRecord == userLocStore[uid]:
                            for view in cplan.QueryPlan[self.users[uid]]:
                                vlist.append(view.view_id)
                        else:
                            oldplan = list()
                            oldplan.append(uid+"old")
                            t = Transaction(i,oldplan,"Planning",True)
                            querycount += 1
                            query_record.write(t.toStr()+"\n")
                            followedfeeds = set()
                            for fid,feed in self.feeds.items():
                                trange = user.locRecord.getRange(feed.loc)
                                if(trange < user.query_range):
                                    followedfeeds.add(feed)
                                    vlist.append(cplan.ViewMap["V"+str(fid)])
                            oldplan.remove(uid+"old")
                            oldplan.append(uid+"new")
                            t = Transaction(i,oldplan,"Planning",True)
                            querycount += 1
                            query_record.write(t.toStr()+"\n")
                            cplan.QueryPlan[self.users[uid]] = set(vlist)
                            del vlist[:]
                            for view in cplan.QueryPlan[self.users[uid]]:
                                vlist.append(view.view_id)
                            userLocStore[uid] = Location(self.users[uid].locRecord.latitude,self.users[uid].locRecord.longitude)
                        if len(vlist) <= 1:
                            continue 
                        t = Transaction(i,vlist)
                        querycount += 1
                        query_record.write(t.toStr()+"\n")
                            
            
            #UpdatingView Selection Plan
            if count >= size:
                modi = i % maxint
                loop += 1
                count = 0
                print("Finished at: " + str(count) + ", Generated Update" + str(updatecount) + "\t" + "Query: " + str(querycount))
            else:
                modi = i
            
                
            
            if modi in self.userint:
                count += 1
                if count >= size and loop == 1:
                    maxint = i  
                for locr in self.userint[modi]:
                    qloc = userLocStore[locr.uid]
                    self.users[locr.uid].locRecord = locr
                    loc = Location(locr.latitude,locr.longitude)
                    userLocStore[locr.uid] = loc
                    tot_change_dist += loc.getRange(qloc)
                    
#                 gridStatistic.clear()
#                 for uid,loc in userLocStore.items():
#                     if loc not in gridStatistic:
#                         gridStatistic[loc] = 0
#                     gridStatistic[loc] += userQueryFreq[uid]
#                 tot_rec = len(gridStatistic)
                
#                 print("Total New " + str(tot_new) + " Tot Add " + str(tot_add) + " Tot Sub " + str(tot_sub))
                
#                 if newCost[2] / lastCost[2] >= GreedyAlgorithm.ChangeThreshold:
                if tot_change_dist > Algorithm.CHANGED_DISTANCE:
                    print("Changed Distance: " + str(tot_change_dist) + " in interval " + str(i))
                    #print("Total Statistic Grids: " + str(tot_rec))
                    lastCost = Algorithm.calcCost(cplan)
                    tot_change_dist = 0
                    print("Current Plan Cost: Mat = " + str(lastCost[0]) + " Eva: " + str(lastCost[1]) + " Total: " + str(lastCost[2]))
                
            

class GreedyAlgorithm(Algorithm):
    '''
        Algorithm for Greedy Algorithm
    '''
    
    ChangeThreshold = 1
    
    def __init__(self, feeds,users,userint=None):
        Algorithm.__init__(self, feeds, users, userint)
    
    def viewSelectionDynamic(self, tot_interval, filename):
        '''
            Adaptive Greedy Algorithm
            Take user_movement(HashMap<UserId,Tuple(Loc,Location)>)
            as input, dynamically change the view selection plan.
            
            Generate execution transactions over time.
            (Assume the timestamp of each location record is normalized)
            Filename+view for update
            Filename+query for processing
        '''
        update_record = open(filename+".view",'w')
        query_record = open(filename+".query",'w')
        cost_record = open(filename+".cost","w")
        '''
            1. Prepare information
                UserLocation initialize (Location.userBegin need to be updated)
                GridView generation
                GridQueryPlan transformation
        '''
        gridStatistic = {}
        userQueryFreq = {}
        userLocStore = {}
#         locOperations = {}
        tot_new = 0
        tot_add = 0
        print("Initial user location size: " + str(len(self.userint[0])))
        for locr in self.userint[0]:
            if locr.uid not in userQueryFreq:
                qfreq = float(1.0 / self.users[locr.uid].query_intv)
                userQueryFreq[locr.uid] = qfreq
            else:
                qfreq = userQueryFreq[locr.uid]
            
            if qfreq < 0:
                print("Negative Raw: " + str(qfreq))
            
            self.users[locr.uid].locRecord = locr
            loc = Location(locr.latitude,locr.longitude)
            if loc in gridStatistic:
                gridStatistic[loc] += qfreq
#                 locOperations[loc] += 1
                tot_add += 1
            else:
                gridStatistic[loc] = qfreq
#                 locOperations[loc] = 1
                tot_new += 1
                
            userLocStore[locr.uid] = loc
            
        print("Total statistic Records: " + str(len(gridStatistic)))
        print("Total New " + str(tot_new) + " Tot Add " + str(tot_add))
        #Update user location to int 0 value, initialize gridStatistic
        
        #InitialPlan
        initialRaw = GridView.search(self.feeds, self.users,True)
        
#         print("User Size: " + str(len(self.users)) + " Feed Size: " + str(len(self.feeds)))
        
        gView = dict()
        viewMap = dict()
        gridViewMap = defaultdict(set)
        for fid,viewset in initialRaw.FeedViewMap.items():
            if len(viewset) == 0:
                print("Empty FeedView Error")
            else:
                loc = initialRaw.FeedMap[fid].loc
                if len(viewset) > 1:
                    print("Multiple View for one Feed")
                for view in viewset:
                    view.setRegion(Location.queryGrids(loc,UserInfo.default_range))
                    #print("Region Size: " + str(len(view.coverageRegion)))
                    gView[loc] = view
                    viewMap[view.view_id] = view
                    for grid in view.coverageRegion:
#                         if grid in gridStatistic:
                        if grid not in gridViewMap:
                            vset = set()
                            gridViewMap[grid] = vset
                        gridViewMap[grid].add(view)
#         print("After Process gView size: " + str(len(gView.items())))
#         print("Grid View Map size: " + str(len(gridViewMap.items())))
#         print("ViewMap size: " + str(len(viewMap.items())))
        gridPlan = defaultdict(set)
        for user,viewset in initialRaw.QueryPlan.items():
            loc = Location(user.locRecord.latitude,user.locRecord.longitude)
            gridPlan[loc] = viewset
        print("Grid Plan size: " + str(len(gridPlan.items())))
        
            
            
        DPlan = collections.namedtuple('Plan',['ViewMap','GridPlan','GridStatistic','GridView','GridViewMap','FeedViewMap'])   
        P = DPlan(ViewMap=viewMap,GridPlan=gridPlan,GridStatistic=gridStatistic,GridView=gView,GridViewMap=gridViewMap,FeedViewMap=initialRaw.FeedViewMap)
        
        cplan = GreedyAlgorithm.gridViewSelection(gridStatistic, P)
        lastCost = None
        
        for iteration in range(1,3):
            cplan = GreedyAlgorithm.gridViewSelection(gridStatistic, P)
            lastCost = Algorithm.calcDynamicCost(cplan, cplan.GridStatistic)
            print("Cost: Mat = " + str(lastCost[0]) + " Eva: " + str(lastCost[1]) + " Total: " + str(lastCost[2]))
        
        
        cline = "Interval\tPushCost\tPullCost\tTotalCost"
        cost_record.write(cline+"\n")
        cline = str(0) + "\t" + str(lastCost[0]) + "\t" + str(lastCost[1]) + "\t" + str(lastCost[2])
        cost_record.write(cline+"\n")
        
        
        
        '''
            Update timeSchedulesQuery, HashMap from Interval to List of String(UserID)
            Update timeSchedulesUpdate, HashMap from Interval to List of String(ViewID)
        '''
        timeSchedulesQuery = defaultdict(list)
        timeSchedulesUpdate = defaultdict(list)
        
        count = 0
        maxint = 0
        totint = 0
        minint = 9999
        
        for uid,user in self.users.items():
            uint = int(user.query_intv)
            totint += uint
            count += 1
            if uint < minint:
                minint = uint
            if uint > maxint:
                maxint = uint
            if uint not in timeSchedulesQuery:
                timeSchedulesQuery[uint] = list()
            timeSchedulesQuery[uint].append(user.user_id)
        print("Max Uint: " + str(maxint) + "\tMin: " + str(minint) + "\tAverage: " + str(float(totint)/count))
        
        count = 0
        maxint = 0
        totint = 0
        minint = 9999
        
        for fid,feed in self.feeds.items():
            fint = int(feed.update_intv)
            totint += fint
            count += 1
            if fint < minint:
                minint = fint
            if fint > maxint:
                maxint = fint
            if fint not in timeSchedulesUpdate:
                timeSchedulesUpdate[fint] = list()
            timeSchedulesUpdate[fint].append(feed.feed_id)
            
        print("Max Uint: " + str(maxint) + "\tMin: " + str(minint) + "\tAverage: " + str(float(totint)/count))
                
        
        #UpdateTransactionList = defaultdict(list())
        #QueryTransactionList = defaultdict(list())
        #HashMap from time interval to list of Transactions
        count = 0
        maxint = 0
        loop = 1
        size = len(self.userint) - 1
        querycount = 0
        updatecount = 0
        tot_change_dist = 0
        last_search = 0
        for i in range(1,tot_interval+1):
            if (i % 500 == 0):
                print("Interval :" + str(i) + "/" + str(tot_interval))
                print("Write :" + str(updatecount) + "\t" + "Query: " + str(querycount))
                print("Tot Dist: " + str(tot_change_dist))
            
            #if(writecount % 500 == 0):
            #    print("Written: " + str(writecount)+"\n")
            #Processing
            for uintf,flist in timeSchedulesUpdate.items():
                if i % uintf == 0:
                    for fid in flist:
                        vlist = list()
                        for view in cplan.FeedViewMap[fid]:
                            vlist.append(view.view_id)
                        if len(vlist) == 0:
                            print("Empty Update List!")
                        t = Transaction(i,vlist,str(i),True)
                        updatecount += 1
                        update_record.write(t.toStr()+"\n")
                        
            
            for qintu,ulist in timeSchedulesQuery.items():
                if i % qintu == 0:
                    for uid in ulist:
                        uloc = userLocStore[uid]
                        vlist = list()
                        if uloc not in cplan.GridPlan and uloc in gridStatistic and gridStatistic[uloc] > 0:
                            oldplan = list()
                            oldplan.append(uid+"old")
                            t = Transaction(i,oldplan,"Planning",True)
                            querycount += 1
                            query_record.write(t.toStr()+"\n")
                            cplan.GridPlan[uloc] = Algorithm.queryPlanningGrid(uloc, self.users[uid].query_range, cplan.GridViewMap)
                            oldplan.remove(uid+"old")
                            oldplan.append(uid+"new")
                            t = Transaction(i,oldplan,"Planning",True)
                            querycount += 1
                            query_record.write(t.toStr()+"\n")
                        elif uloc not in cplan.GridPlan:
                            continue
                        for view in cplan.GridPlan[uloc]:
                            vlist.append(view.view_id)
                        if len(vlist) == 0 or len(vlist) == 1:
                            continue 
                        t = Transaction(i,vlist)
                        querycount += 1
                        query_record.write(t.toStr()+"\n")
                            
            
            #UpdatingView Selection Plan
            if count >= size:
                modi = i % maxint
                loop += 1
                count = 0
                print("Finished at: " + str(count) + ", Generated Update" + str(updatecount) + "\t" + "Query: " + str(querycount))
            else:
                modi = i
            
                
            
            if modi in self.userint:
                count += 1
                if count >= size and loop == 1:
                    maxint = i
                tot_new = 0
                tot_add = 0
                tot_rec = 0    
                for locr in self.userint[modi]:
                    qloc = userLocStore[locr.uid]
                    self.users[locr.uid].locRecord = locr
                    loc = Location(locr.latitude,locr.longitude)
                    userLocStore[locr.uid] = loc
                    tot_change_dist += loc.getRange(qloc)
                    
                gridStatistic.clear()
                for uid,loc in userLocStore.items():
                    if loc not in gridStatistic:
                        gridStatistic[loc] = 0
                    gridStatistic[loc] += userQueryFreq[uid]
                tot_rec = len(gridStatistic)
                
                lastCost = Algorithm.calcDynamicCost(cplan,gridStatistic)
#                 print("Total New " + str(tot_new) + " Tot Add " + str(tot_add) + " Tot Sub " + str(tot_sub))
                
#                 if newCost[2] / lastCost[2] >= GreedyAlgorithm.ChangeThreshold:
                if tot_change_dist > Algorithm.CHANGED_DISTANCE and last_search - i >= 10:
                    print("Changed Distance: " + str(tot_change_dist) + " in interval " + str(i))
                    print("Total Statistic Grids: " + str(tot_rec))
                    tot_change_dist = 0
                    cplan = GreedyAlgorithm.gridViewSelection(gridStatistic, cplan)
                    newCost = Algorithm.calcDynamicCost(cplan,gridStatistic)
                    print("Current Plan Cost: Mat = " + str(lastCost[0]) + " Eva: " + str(lastCost[1]) + " Total: " + str(lastCost[2]))
                    lastCost = newCost
                    print("ReSelected, new Cost: Mat = " + str(lastCost[0]) + " Eva: " + str(lastCost[1]) + " Total: " + str(lastCost[2]))
                    print("New Grid Plan Size: " + str(len(cplan.GridPlan)))
                    print("New Plan View Count: " + str(len(cplan.ViewMap)))
                    cline = str(i) + "\t" + str(lastCost[0]) + "\t" + str(lastCost[1]) + "\t" + str(lastCost[2])
                    cost_record.write(cline+"\n")
                    last_search = i
                    
    
    @classmethod
    def UpdateRemovedPlan(cls,view,gridviewMap,gridPlan,gridStatistic,qrange=UserInfo.default_range):
        for grid in view.coverageRegion:
            if grid in gridPlan:
                if view in gridPlan[grid]:
                    preSize = len(gridPlan[grid])
                    gridPlan[grid] = Algorithm.queryPlanningGrid(grid, qrange, gridviewMap)
                    if gridPlan[grid] is None:
                        print("Fail Delete View: " + view.toStr())
                    afterSize = len(gridPlan[grid])
#                     if afterSize > preSize:
#                         print("Bad Update, Old:" + str(preSize) + "\tNew:" + str(afterSize))
            elif grid in gridStatistic and gridStatistic[grid] > 0:
#                 continue
                gridPlan[grid] = Algorithm.queryPlanningGrid(grid, qrange, gridviewMap)
#                 print("Add New Plan on Remove")
        
        
    @classmethod
    def UpdateQueryPlan(cls,view,gridviewMap,gridPlan,gridStatistic,qrange=UserInfo.default_range):
        matC = view.update_freq
        evaC = 0
        for grid in view.coverageRegion:
            if grid in gridStatistic and gridStatistic[grid] > 0.0:
                if grid in gridPlan:
                    preSize = len(gridPlan[grid])
                    newPlan = Algorithm.queryPlanningGrid(grid, qrange, gridviewMap)
                    afterSize = len(newPlan)
                    if afterSize > preSize:
                        continue
                    else:
#                         print("Updated Plan")
                        if afterSize == 1:
                            evaC += gridStatistic[grid] * len(view.feed_set)
                        else:
                            evaC += gridStatistic[grid] * (len(view.feed_set)-1)
                        gridPlan[grid] = newPlan
                else:
                    gridPlan[grid] = Algorithm.queryPlanningGrid(grid, qrange, gridviewMap)
#                     print("Add New Plan on update")
        if evaC - matC*Algorithm.PushPullRatio < 0:
            print("Bad materialized view")
            
            
    @classmethod        
    def gridViewSelection(cls,gridStatistic,currentPlan):      
        '''
            Calculate new view selection plan under new gridStatistic with given gridView
            Return a new view selection plan with the parameter as follows,
                ViewMap, HashMap from view id to materialized view
                GridPlan, HashMap from Location to set of view from ViewMap
                gridStatistic, HashMap from Location to float (query frequency)
                gridView, HashMap from Location to view, all time materialized native view
        '''
#         Algorithm.gridFollowDict.clear()
        feedViews = currentPlan.FeedViewMap
        gridviews = currentPlan.GridViewMap
        candidates = set()
        procView = {}
        
#         print("ViewMap Size: " + str(len(currentPlan.ViewMap.items())))
        
        for vid,view in currentPlan.ViewMap.items():
            qfreq = 0.0
            for grid,vset in currentPlan.GridPlan.items():
                if view in vset:
                    if grid in gridStatistic:
                        qfreq += gridStatistic[grid]
                    else:
                        del currentPlan.GridPlan[grid]
#             if view.query_freq != qfreq:
#                     print("Query Frequency Changed In Raw! Old: " + str(view.query_freq) + " New: " + str(qfreq))
            view.query_freq = qfreq
            procView[view.view_id] = view
            if view.isFeed:
                continue
            else:
                candidates.add(view)
#         print("Total ProcView: " + str(len(procView.items())))    
        
            
        count = 0
        
#         print("Total gridView: " + str(len(currentPlan.gridView.items())))
            
        for grid,feed_view in currentPlan.GridView.items():
            if feed_view.view_id not in procView:
                #print("Continue of not used")
                continue
            else:
                del procView[feed_view.view_id]
            
            benefit_max = Algorithm.calcBenefitApx(feed_view)
            v_combine = None
            v_source = None
            
            if len(feed_view.coverageRegion) == 0:
                print("Coverage Region not set feed view")
                
            for vid,vj in procView.items():
                if vj == feed_view:
                    continue
                v_t = View.combineView(feed_view,vj)
                if len(v_t.coverageRegion) == 0:
                    continue
                else:
                    qfreq = float(0)
                    for cgrid,vset in currentPlan.GridPlan.items():
                        if cgrid in v_t.coverageRegion:
                            if cgrid in gridStatistic:
                                qfreq += gridStatistic[cgrid]
                            else:
                                del currentPlan.GridPlan[cgrid]
                            
#                     if v_t.query_freq != qfreq:
#                         print("Query Frequency Changed! Old: " + str(v_t.query_freq) + " New: " + str(qfreq))
                    v_t.query_freq = qfreq
                    b_t = Algorithm.calcBenefitApx(v_t)
                    if b_t > benefit_max:
                        v_combine = v_t
                        v_source = vj
                        benefit_max = b_t
            if v_combine is not None:
                v_combine.benefit = benefit_max
                v_combine.view_id = "CombinedView" + str(Algorithm.int_vid_counter)
                Algorithm.int_vid_counter += 1
                procView[v_combine.view_id]=v_combine
                candidates.add(v_combine)
                count += 1
                benefit_vs = Algorithm.calcBenefitApx(v_source)
                if benefit_vs < benefit_max:
                    del procView[v_source.view_id]
        
        print("Extra view: " + str(count))    
                    
        selected = 0
        gridPlan = currentPlan.GridPlan
        viewMap = currentPlan.ViewMap
        for view in sorted(candidates,key=lambda t:t.benefit,reverse=True):
            if view.isFeed:
                continue
#             counter += 1
#             if counter % 100 == 0:
#                 print("Processed: " + str(counter) + "/" + str(len(candidateViews)))
#             benefit1 = Algorithm.calcBenefitGrid(view, gridviews, gridStatistic, gridPlan)
            benefit2 = Algorithm.calcBenefitGridByPlan(view, gridviews, gridStatistic, gridPlan)
#             if benefit1 <= benefit2:
#                 print("Benefit 1: " + str(benefit1) + "\t Benefit 2: " + str(benefit2))
            view.benefit = benefit2
            
            if view.benefit <= 0:
                #Remove view, change gridPlan
                if view.view_id in viewMap:
                    del viewMap[view.view_id]
                    for grid in view.coverageRegion:
                        if grid in gridviews and view in gridviews[grid]:
                            gridviews[grid].remove(view)
                    for feed in view.feed_set:
                        feedViews[feed.feed_id].remove(view)
                    GreedyAlgorithm.UpdateRemovedPlan(view,gridviews,gridPlan,gridStatistic)
            else:
                if view.view_id not in viewMap:
                    selected += 1
                if view.view_id not in viewMap:
                    viewMap[view.view_id]=view
                    for grid in view.coverageRegion:
#                         if grid in gridStatistic:
                        if grid not in gridviews:
                            gridviews[grid] = set()
                        gridviews[grid].add(view)
                    for feed in view.feed_set:
                        if feed.feed_id in feedViews:
                            feedViews[feed.feed_id].add(view)
                        else:
                            feedViews[feed.feed_id] = set()
                            feedViews[feed.feed_id].add(view)
                GreedyAlgorithm.UpdateQueryPlan(view,gridviews,gridPlan,gridStatistic)
        print("Extra Views Materialized: " + str(selected))
        
        DPlan = collections.namedtuple('Plan',['ViewMap','GridPlan','GridStatistic','GridView','GridViewMap','FeedViewMap'])
        P = DPlan(ViewMap=viewMap,GridPlan=gridPlan,GridStatistic=gridStatistic,GridView = currentPlan.GridView,GridViewMap=gridviews,FeedViewMap=feedViews)
        return P
    
    @classmethod   
    def locViewSelection(cls,gridStatistic,currentPlan):      
        '''
            Calculate new view selection plan under new gridStatistic with given gridView
            Following set defined as grids (loc)
            Return a new view selection plan with the parameter as follows,
                ViewMap, HashMap from view id to materialized view
                GridPlan, HashMap from Location to set of view from ViewMap
                gridStatistic, HashMap from Location to float (query frequency)
                gridView, HashMap from Location to view, all time materialized native view
        '''
#         Algorithm.gridFollowDict.clear()
        feedViews = currentPlan.FeedViewMap
        gridviews = currentPlan.GridViewMap
        candidates = set()
        procView = {}
        
#         print("ViewMap Size: " + str(len(currentPlan.ViewMap.items())))
        
        for vid,view in currentPlan.ViewMap.items():
            qfreq = 0.0
            for grid,vset in currentPlan.GridPlan.items():
                if view in vset:
                    if grid in gridStatistic:
                        qfreq += gridStatistic[grid]
                    else:
                        del currentPlan.GridPlan[grid]
#             if view.query_freq != qfreq:
#                     print("Query Frequency Changed In Raw! Old: " + str(view.query_freq) + " New: " + str(qfreq))
            view.query_freq = qfreq
            procView[view.view_id] = view
            if view.isFeed:
                continue
            else:
                candidates.add(view)
#         print("Total ProcView: " + str(len(procView.items())))    
        
            
        count = 0
        
#         print("Total gridView: " + str(len(currentPlan.gridView.items())))
            
        for grid,feed_view in currentPlan.GridView.items():
            if feed_view.view_id not in procView:
                #print("Continue of not used")
                continue
            else:
                del procView[feed_view.view_id]
            
            benefit_max = Algorithm.calcBenefitApx(feed_view)
            v_combine = None
            v_source = None
            
            if len(feed_view.coverageRegion) == 0:
                print("Coverage Region not set feed view")
                
            for vid,vj in procView.items():
                if vj == feed_view:
                    continue
                v_t = View.combineView(feed_view,vj)
                if len(v_t.coverageRegion) == 0:
                    continue
                else:
                    qfreq = float(0)
                    for cgrid,vset in currentPlan.GridPlan.items():
                        if cgrid in v_t.coverageRegion:
                            if cgrid in gridStatistic:
                                qfreq += gridStatistic[cgrid]
                            else:
                                del currentPlan.GridPlan[cgrid]
                            
#                     if v_t.query_freq != qfreq:
#                         print("Query Frequency Changed! Old: " + str(v_t.query_freq) + " New: " + str(qfreq))
                    v_t.query_freq = qfreq
                    b_t = Algorithm.calcBenefitApx(v_t)
                    if b_t > benefit_max:
                        v_combine = v_t
                        v_source = vj
                        benefit_max = b_t
            if v_combine is not None:
                v_combine.benefit = benefit_max
                v_combine.view_id = "CombinedView" + str(Algorithm.int_vid_counter)
                Algorithm.int_vid_counter += 1
                procView[v_combine.view_id]=v_combine
                candidates.add(v_combine)
                count += 1
                benefit_vs = Algorithm.calcBenefitApx(v_source)
                if benefit_vs < benefit_max:
                    del procView[v_source.view_id]
        
        print("Extra view: " + str(count))    
                    
        selected = 0
        gridPlan = currentPlan.GridPlan
        viewMap = currentPlan.ViewMap
        for view in sorted(candidates,key=lambda t:t.benefit,reverse=True):
            if view.isFeed:
                continue
#             counter += 1
#             if counter % 100 == 0:
#                 print("Processed: " + str(counter) + "/" + str(len(candidateViews)))
#             benefit1 = Algorithm.calcBenefitGrid(view, gridviews, gridStatistic, gridPlan)
            benefit2 = Algorithm.calcBenefitGridByPlan(view, gridviews, gridStatistic, gridPlan)
#             if benefit1 <= benefit2:
#                 print("Benefit 1: " + str(benefit1) + "\t Benefit 2: " + str(benefit2))
            view.benefit = benefit2
            
            if view.benefit <= 0:
                #Remove view, change gridPlan
                if view.view_id in viewMap:
                    del viewMap[view.view_id]
                    for grid in view.coverageRegion:
                        if grid in gridviews and view in gridviews[grid]:
                            gridviews[grid].remove(view)
                    for feed in view.feed_set:
                        feedViews[feed.feed_id].remove(view)
                    GreedyAlgorithm.UpdateRemovedPlan(view,gridviews,gridPlan,gridStatistic)
            else:
                if view.view_id not in viewMap:
                    selected += 1
                if view.view_id not in viewMap:
                    viewMap[view.view_id]=view
                    for grid in view.coverageRegion:
#                         if grid in gridStatistic:
                        if grid not in gridviews:
                            gridviews[grid] = set()
                        gridviews[grid].add(view)
                    for feed in view.feed_set:
                        if feed.feed_id in feedViews:
                            feedViews[feed.feed_id].add(view)
                        else:
                            feedViews[feed.feed_id] = set()
                            feedViews[feed.feed_id].add(view)
                GreedyAlgorithm.UpdateQueryPlan(view,gridviews,gridPlan,gridStatistic)
        print("Extra Views Materialized: " + str(selected))
        
        DPlan = collections.namedtuple('Plan',['ViewMap','GridPlan','GridStatistic','GridView','GridViewMap','FeedViewMap'])
        P = DPlan(ViewMap=viewMap,GridPlan=gridPlan,GridStatistic=gridStatistic,GridView = currentPlan.GridView,GridViewMap=gridviews,FeedViewMap=feedViews)
        return P
     
    def viewSelection(self):
        initialPlan = GridView.search(self.feeds, self.users)
        matViews = initialPlan.ViewMap
        procView = matViews.copy()
        candidateViews = []
        #feedMap = initialPlan.FeedMap
        feedviewMap = initialPlan.FeedViewMap
        userQueryPlan = initialPlan.QueryPlan
        
        #Combine views to generate better 
        counter = 0;
        for vid,vi in matViews.items():
            counter += 1
#             if counter % 100 == 0:
#                 print("Processed: " + str(counter) + "/" + str(len(matViews)))
            if vid in procView:
                del procView[vi.view_id]
            else:
                continue
            benefit_max = Algorithm.calcBenefitApx(vi)
            v_combine = None
            v_source = None
#             vcounter = 0
            for id2,vj in procView.items():
                if vi.feed_set == vj.feed_set:
                    continue
#                 vcounter += 1
                v_t = View.combineView(vi,vj)
                if len(v_t.service_set) == 0:
                    continue
#                 if vcounter % 100 == 0:
#                     print(vcounter)
                b_t = Algorithm.calcBenefitApx(v_t)
                if b_t > benefit_max:
                    v_combine = v_t
                    v_source = vj
                    benefit_max = b_t
            if v_combine is not None:
                v_combine.benefit = benefit_max
                v_combine.view_id = "CombinedView" + str(Algorithm.int_vid_counter)
                Algorithm.int_vid_counter += 1
                procView[v_combine.view_id]=v_combine
                benefit_vs = Algorithm.calcBenefitApx(v_source)
                if benefit_vs < benefit_max:
                    del procView[v_source.view_id]
                candidateViews.append(v_combine)
        #print("Number of Candidate views: " + str(len(candidateViews)))
#         counter = 0
        selected = 0
        for view in sorted(candidateViews,key=lambda t:t.benefit,reverse=True):
#             counter += 1
#             if counter % 100 == 0:
#                 print("Processed: " + str(counter) + "/" + str(len(candidateViews)))
            view.benefit = Algorithm.calcBenefitOpt(view, userQueryPlan)
            if view.benefit <= 0:
                continue
            else:
                selected += 1
                for feed in view.feed_set:
                    feedviewMap[feed.feed_id].add(view)
                matViews[view.view_id]=view
                for user in view.service_set:
#                     calcPlan = Algorithm.queryPlanning(user, self.feeds, feedviewMap)
#                     cPlan = userQueryPlan[user]
#                     amount = len(cPlan) - len(calcPlan)
                    toRemove = set()
                    if view in userQueryPlan[user]:
                        print("Already in Plan")
                    for vi in userQueryPlan[user]:
                        if vi.feed_set.issubset(view.feed_set):
                            toRemove.add(vi)
                    if len(toRemove) > 0:
                        userQueryPlan[user] = userQueryPlan[user] - toRemove
                        userQueryPlan[user].add(view)
#                     if amount != len(toRemove) - 1:
#                         print("Error Planning")
        #print("Extra Views Materialized: " + str(selected))        
        
        Plan = collections.namedtuple('Plan',['ViewMap','FeedMap','UserMap','FeedViewMap','QueryPlan'])
        P = Plan(ViewMap=matViews,FeedMap=self.feeds,UserMap=self.users,FeedViewMap=feedviewMap,QueryPlan=userQueryPlan)
        
        return P
        
    def viewSelection2(self,lastPlan):
        initialPlan = lastPlan
        matViews = initialPlan.ViewMap
        procView = matViews.copy()
        candidateViews = []
        feedMap = initialPlan.FeedMap
        feedviewMap = initialPlan.FeedViewMap
        userQueryPlan = initialPlan.QueryPlan
        
        #Combine views to generate better 
        counter = 0;
        for vid,vi in matViews.items():
            counter += 1
#             if counter % 100 == 0:
#                 print("Processed: " + str(counter) + "/" + str(len(matViews)))
            if vid in procView:
                del procView[vi.view_id]
            else:
                continue
            benefit_max = Algorithm.calcBenefitApx(vi)
            v_combine = None
            v_source = None
#             vcounter = 0
            for id2,vj in procView.items():
                if vi.feed_set == vj.feed_set:
                    continue
#                 vcounter += 1
                v_t = View.combineView(vi,vj)
                if len(v_t.service_set) == 0:
                    continue
#                 if vcounter % 100 == 0:
#                     print(vcounter)
                b_t = Algorithm.calcBenefitApx(v_t)
                if b_t > benefit_max:
                    v_combine = v_t
                    v_source = vj
                    benefit_max = b_t
            if v_combine is not None:
                v_combine.benefit = benefit_max
                v_combine.view_id = "CombinedView" + str(Algorithm.int_vid_counter)
                Algorithm.int_vid_counter += 1
                procView[v_combine.view_id]=v_combine
                benefit_vs = Algorithm.calcBenefitApx(v_source)
                if benefit_vs < benefit_max:
                    del procView[v_source.view_id]
                candidateViews.append(v_combine)
        print("Number of Candidate views: " + str(len(candidateViews)))
#         counter = 0
        selected = 0
        removed = 0
        for view in sorted(candidateViews,key=lambda t:t.benefit,reverse=True):
#             counter += 1
#             if counter % 100 == 0:
#                 print("Processed: " + str(counter) + "/" + str(len(candidateViews)))
            view.benefit = Algorithm.calcBenefitOpt(view, userQueryPlan, feedMap, feedviewMap)
            if view.benefit <= 0:
                if view.isFeed or view.view_id not in matViews:
                    continue
                else:
                    removed += 1
                    for feed in view.feed_set:
                        feedviewMap[feed.feed_id].remove(view)
                    del matViews[view.view_id]
                    for user in view.service_set:
                        if view in userQueryPlan[user]:
                            userQueryPlan[user] = Algorithm.queryPlanning(user, feedMap, feedviewMap)
            else:
                selected += 1
                for feed in view.feed_set:
                    feedviewMap[feed.feed_id].add(view)
                matViews[view.view_id]=view
                for user in view.service_set:
                    calcPlan = Algorithm.queryPlanning(user, self.feeds, feedviewMap)
                    cPlan = userQueryPlan[user]
                    if len(calcPlan) < len(cPlan):
                        userQueryPlan[user] = calcPlan
#                     amount = len(cPlan) - len(calcPlan)
#                     toRemove = set()
#                     if view in userQueryPlan[user]:
#                         continue
#                     for vi in userQueryPlan[user]:
#                         if vi.feed_set.issubset(view.feed_set):
#                             toRemove.add(vi)
#                     if len(toRemove) > 0:
#                         userQueryPlan[user] = userQueryPlan[user] - toRemove
#                         userQueryPlan[user].add(view)
#                     if amount != len(toRemove) - 1:
#                         print("Error Planning")
        print("Extra Views Materialized: " + str(selected) + " Removed" + str(removed))        
        
        Plan = collections.namedtuple('Plan',['ViewMap','FeedMap','UserMap','FeedViewMap','QueryPlan'])
        P = Plan(ViewMap=matViews,FeedMap=self.feeds,UserMap=self.users,FeedViewMap=feedviewMap,QueryPlan=userQueryPlan)
        planCost = Algorithm.calcCost(P)
        print("Greedy Cost: Mat = " + str(planCost[0]) + " Eva: " + str(planCost[1]) + " Total: " + str(planCost[2]))
        print("Total view: " + str(len(matViews)))
        return P
        
        
        
        
        