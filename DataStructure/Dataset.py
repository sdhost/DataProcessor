'''
Created on Jul 20, 2015

@author: SDH
'''
from math import floor, ceil
from _collections import defaultdict
import random
import math
import collections
import sys
#from __builtin__ import str
from math import floor, ceil
from datetime import datetime





class UserInfo(object):
    '''
    UserInfo stands for individual users in location based Feed-following system
    '''
    default_range = 5
    default_tags = ["Tag"]

    def __init__(self, user_id, locrecord, query_range=default_range, query_intv=None, tags=default_tags):
        '''
        Constructor
        '''
        self.user_id = user_id
        self.tags = tags
        self.locRecord = locrecord
        self.query_range = query_range
        self.query_intv = query_intv
    
    def __eq__(self,other):
        return (isinstance(other, self.__class__)
            and self.user_id == other.user_id)
    
    def __ne__(self,other):
        return not self.__eq__(other)
    
    def __hash_(self):
        return hash(self.user_id)
    
    def toStr(self):
        line = self.user_id + "\t" + self.locRecord.toStr() + "\t" + str(self.query_range) + "\t" + str(self.query_intv)
        for tag in self.tags:
            line += "\t" + tag
        return line
    
    @classmethod
    def loadFromLine(cls,line):
        elem=line.strip().split("\t")
        uid=elem[0]
        loc_line=elem[1]+"\t"+elem[2]+"\t"+elem[3] + "\t" + elem[4]
        locr=LocationRecord.loadFromLine(loc_line)
        qrange=UserInfo.default_range
        qint=int(math.ceil(float(elem[6])/LocationRecord.TimeScale))
        tags=[]
        for i in range(7,len(elem)):
            tags.append(elem[i])
        return UserInfo(uid,locr,qrange,qint,tags)
            
        
        
        
        
# class Statistic(object):
#     '''
#     User density Statistic for one grid
#     Set scale parameters before create any instance if needed
#     '''
#     min_x = 0
#     min_y = 0
#     max_x = 0
#     max_y = 0
#     ori_minx = 0
#     ori_miny = 0
#     ori_maxx = 0
#     ori_maxy = 0
#     isScaled = False
#     def __init__(self, loc, density, tag=None):
#         '''
#         Constructor
#         '''
#         if Statistic.isScaled == True:
#             loc.x_dimension = (loc.x_dimension - Statistic.ori_minx) / (Statistic.ori_maxx - Statistic.ori_minx) * (Statistic.max_x - Statistic.min_x) + Statistic.min_x
#             loc.y_dimension = (loc.y_dimension - Statistic.ori_miny) / (Statistic.ori_maxy - Statistic.ori_miny) * (Statistic.max_y - Statistic.min_y) + Statistic.min_y
#         self.loc = loc
#         self.tag = tag
#         self.density = density
#         
#     @classmethod
#     def setScaleRegion(cls,min_x,min_y,max_x,max_y,ori_minx,ori_miny,ori_maxx,ori_maxy):
#         cls.min_x = min_x
#         cls.min_y = min_y
#         cls.max_x = max_x
#         cls.max_y = max_y
#         cls.ori_maxx = ori_maxx
#         cls.ori_maxy = ori_maxy
#         cls.ori_minx = ori_minx
#         cls.ori_miny = ori_miny
#         cls.isScaled = True
       
        
# class UID(object):
#     '''
#     Identifier of each statistic unit
#     '''
# 
#     def __init__(self, id, loc, query_range, tags):
#         '''
#         Constructor
#         '''
#         self.id = id
#         self.loc = loc
#         self.query_range = query_range
#         self.tags = tags
#         
# class LocUID(UID):
#     '''
#     Identifier with only location
#     '''
#     def __init__(self,id,loc):
#         UID.__init__(self, id, loc, None, None)
#         
# class LocRangeUID(UID):
#     '''
#     Identifier with location and range
#     '''
#     def __init__(self,id,loc,query_range):
#         UID.__init__(self, id, loc, query_range, None)
        
class FeedInfo(object):
    '''
    Feed information with same FID
    '''
    default_tag = "Tag"
    def __init__(self,feed_id, loc = None, tag = default_tag, update_intv=0):
        self.feed_id = feed_id
        self.loc = loc
        self.tag = tag
        self.update_intv = update_intv
        
    def __eq__(self,other):
        return (isinstance(other, self.__class__)
            and self.feed_id == other.feed_id)
    
    def __ne__(self,other):
        return not self.__eq__(other)
    
    def __hash_(self):
        return hash(self.feed_id)
    
    def setIntv(self,update_intv):
        self.update_intv = update_intv
        
    def toStr(self):
        line = self.feed_id + "\t" + self.loc.toStr() + "\t" + self.tag + "\t" + str(self.update_intv)
        return line
    
    @classmethod
    def loadFromLine(cls,line):
        elem = line.strip().split("\t")
        fid=elem[0]
        loc_line=elem[1]+"\t"+elem[2]
        loc=Location.loadFromLine(loc_line)
        tag=elem[3]
        upint=int(math.ceil(float(elem[4]) / LocationRecord.TimeScale))
        if upint == 0.0:
            upint = random.randint(10,100)
        return FeedInfo(fid,loc,tag,upint)
        
# class FID(object):
#     '''
#     Identifier of feed
#     '''
#     def __init__(self,id_number,loc,tags):
#         self.id = id_number
#         self.loc = loc
#         self.tags = tags
#         
# class LocFID(FID):
#     '''
#     Identifier of feed without tags
#     '''
#     def __init__(self,id,loc):
#         FID.__init__(self,id,loc,None)
        
class View(object):
    '''
    Materialized view
    '''
    
    def __hash__(self):
        return (hash(tuple(self.feed_set)))
    
    def __eq__(self,other):
        return (isinstance(other, self.__class__)
            and self.feed_set == other.feed_set)
    
    def __ne__(self,other):
        return not self.__eq__(other)
    
    def __init__(self,view_id,feed_set,service_set,update_freq=0,query_freq=0,benefit=0):
        self.view_id = view_id
        self.feed_set = feed_set
        self.service_set = set()
        self.service_set = self.service_set | service_set
        self.update_freq = update_freq
        self.query_freq = query_freq
        self.benefit = benefit
        self.isMaterialized = True
        self.coverageRegion = set()
        self.effectiveRegion = set()
        self.isFeed = False
        self.locations = set()
    
    def setRegion(self,cr,er=None):
        self.coverageRegion = cr
        self.effectiveRegion = er
        
    def removeUsers(self,user_set):
        self.service_set = self.service_set - user_set
        
    def addUsers(self,user_set):
        self.service_set = self.service_set | user_set
        
    def setBenefit(self,benefit):
        self.benefit = benefit
    
    def setFreq(self,update_freq,query_freq):
        self.update_freq = update_freq
        self.query_freq = query_freq
    
    def toStr(self):
        line = self.view_id + "\t" + str(self.update_freq) + "\t" + str(self.query_freq) + "\t" + str(self.benefit) + "\t" + str(len(self.feed_set)) + "\t" + str(len(self.service_set))
        for feed in self.feed_set:
            line += "\t" + feed.feed_id
        for user in self.service_set:
            line += "\t" + user.user_id
        
        return line
    
    def toStrTranslator(self):
        '''
            Produce ViewSelectionRedis compatible view line output
        '''
        line = self.view_id + "\t"#Elem 0
        
        if self.query_freq == 0:
            line += str(9999999) + "\t"
        else:
            query_intv = int(ceil(1.0/self.query_freq))
            if query_intv == 0:
                print("Error View with 0 Query Intv, original frequency is " + str(self.query_freq))
            line += str(query_intv) + "\t"
        #Elem 1
        
        if self.isMaterialized:
            update_intv = int(ceil(1.0/self.update_freq))
            if update_intv == 0:
                print("Error View with 0 Update Intv, original frequency is " + str(self.update_freq))
            line += str(update_intv) + "\t"
        else:
            line += str(9999999) + "\t"
        #Elem 2
        
        if self.isFeed:
            line += str(True) + "\t"
        else:
            line += str(False) + "\t"
        #Elem 3    
        
        if self.isMaterialized:
            line += str(True) + "\t"
        else:
            line += str(False) + "\t"
        #Elem 4
        
        line += str(len(self.feed_set)) + "\t"#Elem 5
        
        if self.isMaterialized:
            line += "0\t"
        else:
            line += str(len(self.service_set)) + "\t"
        #feed_set for feeds and service_set will be changed to view sources for not materialized views
        #Elem 6
        i = 0
        for feed in self.feed_set:
            if i < len(self.feed_set) - 1:
                line += feed.feed_id + " "
            else:
                line += feed.feed_id
            i += 1
        #Elem 7
        
        if self.isMaterialized:
            return line
        else:
            line += "\t"
            i = 0
            for view in self.service_set:
                if i < len(self.service_set) - 1:
                    line += view.view_id + " "
                else:
                    line += view.view_id
                i += 1
            #Elem 8
            return line   
    
    @classmethod
    def loadFromLine(cls,line,feeds,users):
        '''
        Load view from files
        Input:
            line ---  The string generated by a view's toLine() function
            feeds --- HashMap from String(feed_id) to FeedInfo
            users --- HashMap from String(user_id) to UserInfo
        Output:
            A View object with the same properties as the view that generate line using toLine()
        '''
        elem = line.strip().split("\t")
        view_id = elem[0]
        update_freq = float(elem[1])
        query_freq = float(elem[2])
        benefit = float(elem[3])
        num_feed = int(elem[4])
        num_user = int(elem[5])
        feed_set = set()
        service_set = set()
        for i in range(num_feed):
            feed_set.add(feeds[elem[i+6]])
        for i in range(num_user):
            service_set.add(users[elem[i+num_feed+6]])
            
        return View(view_id,feed_set,service_set,update_freq,query_freq,benefit)
    
    @classmethod
    def combineView(cls, vi, vj):
        feedset = vi.feed_set|vj.feed_set
        service_set = vi.service_set&vj.service_set
        update_freq = 0
        for feed in feedset:
            update_freq += float(1.0/feed.update_intv)
        query_freq = 0
        for user in service_set:
            query_freq += float(1.0/user.query_intv)
        vid=str(vi.view_id)+"W"+str(vj.view_id)
        vc = View(vid,feedset,service_set,update_freq,query_freq)
        vc.setRegion(vi.coverageRegion & vj.coverageRegion)
        vc.locations = vi.locations | vj.locations
        return vc
    
    


def map_lat(latitude):
    return int(floor(latitude / Location.Base_lat))
            
def map_long(longitude):
    return int(floor(longitude / Location.Base_long))
            
class Location(object):
    '''
    Grid id for given latitude and longitude 
    ''' 
    Base_lat = 0.5
    Base_long = 0.5
    
    
    def __init__(self,latitude,longitude):
        self.latitude = latitude
        self.longitude = longitude
        self.x_dimension = map_lat(latitude)
        self.y_dimension = map_long(longitude)
    
    def __eq__(self,other):
        return ((isinstance(other, self.__class__)
            or (isinstance(other, LocationRecord.__class__)))
            and self.x_dimension == other.x_dimension
            and self.y_dimension == other.y_dimension)
            
        
    def __ne__(self, other):
        return not self.__eq__(other)
    
    def __hash__(self):
        return (hash(self.x_dimension) ^
                hash(self.y_dimension))
    
    def getRange(self,t_loc):
        return math.sqrt(pow(t_loc.x_dimension - self.x_dimension, 2) + pow(t_loc.y_dimension - self.y_dimension,2))
    
    def toStr(self):
        line = str(self.latitude) + "\t" + str(self.longitude)
        return line
    
    @classmethod
    def loadFromLine(cls,line):
        elem = line.strip().split("\t")
        x=float(elem[0])
        y=float(elem[1])
        return Location(x,y)
    
    @classmethod
    def queryGrids(cls,t_loc,t_range):
        #Set range as integer, the number of grids will be N= 2r^2 + 2r + 1, if we don't consider grid edge
        locList = set()
        for x in range(t_range+1):
            x_change = x * Location.Base_lat
            for y in range(t_range + 1 - x):
                y_change = y * Location.Base_long
                tmp_loc1 = Location(t_loc.latitude - x_change, t_loc.longitude - y_change)
                tmp_loc2 = Location(t_loc.latitude - x_change, t_loc.longitude + y_change)
                tmp_loc3 = Location(t_loc.latitude + x_change, t_loc.longitude - y_change)
                tmp_loc4 = Location(t_loc.latitude + x_change, t_loc.longitude + y_change)
                locList.add(tmp_loc1)
                locList.add(tmp_loc2)
                locList.add(tmp_loc3)
                locList.add(tmp_loc4)
        return locList

class LocationRecord(Location):
    userBegin = {}
    TimeScale = 1.0
    
    def __init__(self,latitude,longitude,loc_timestamp=None, uid=None):
        super(LocationRecord,self).__init__(latitude, longitude)
        if uid is None:
            self.uid = None
            self.int = 0
            if loc_timestamp:
                self.loc_timestamp = loc_timestamp
            else:
                self.loc_timestamp = datetime.now()
        else:
            self.uid = uid
            self.loc_timestamp = loc_timestamp
            if uid in LocationRecord.userBegin:
                self.int = int(abs(math.ceil((loc_timestamp - LocationRecord.userBegin[uid]).total_seconds() / LocationRecord.TimeScale)))
            else:
                self.int = 0
                if loc_timestamp != None:
                    LocationRecord.userBegin[uid] = loc_timestamp
    
    def __eq__(self,other):
        return (isinstance(other, self.__class__)
            and self.x_dimension == other.x_dimension
            and self.y_dimension == other.y_dimension
            and self.uid == other.uid
            and self.int == other.int)
        
    def __ne__(self, other):
        return not self.__eq__(other)
    
    def __hash__(self):
        return (hash(self.x_dimension) ^
                hash(self.y_dimension) ^
                hash(self.uid) ^
                hash(self.int))
    
    def toStr(self):
        line = str(self.latitude) + "\t" + str(self.longitude) + "\t" + str(self.loc_timestamp) + "\t" + str(self.uid)
        return line
    
    @classmethod
    def loadFromLine(cls,line):
        elem = line.strip().split("\t")
        x=float(elem[0])
        y=float(elem[1])
        s = elem[2]
        if s == 'None':
            time = None
        else:
#             time=datetime.strptime(s[:26]+s[26:].replace(':',''), '%Y-%m-%d %H:%M:%S.%f')
            time=datetime.strptime(s[:26]+s[26:].replace(':',''), '%Y-%m-%d %H:%M:%S')
        uid = elem[3]
        return LocationRecord(x,y,time,uid)
    
    
    
        
class DatasetIO(object):
    '''
    Import and export dataset
    '''
    def __init__(self, file_url):
        self.file_url = file_url
        self.file_url_out = file_url + ".out"
        self.dist = None
        self.users = None
        self.feeds = None
        self.movements = defaultdict(list)#LocationRecord
        self.moveint = defaultdict(list)#LocationRecord
        
        
#     def read(self,file_type):
#         dataset_file = open(self.file_url,'r')
#         if file_type is 'SNAP':
#             self.users = {}
#             self.user_movement = defaultdict(list)
#             for line in dataset_file:
#                 try:
#                     elem = line.strip().split("\t")
#                     user_id = elem[0]
#                     tmptime = datetime.strptime(elem[1],"%Y-%m-%dT%H:%M:%SZ")
#                     #record_timestamp = (tmptime - datetime(1970,1,1)).total_seconds()
#                     user_latitude = float(elem[2])
#                     user_longitude = float(elem[3])
#                     #checkin_loc_id = elem(4)
#                     user_loc = Location(user_latitude,user_longitude,tmptime)
#                     #Read dataset line, build a userinfo object from it.
#                     if self.users.has_key(user_id):
#                         self.user_movement.get(user_id).append(user_loc)
#                     else:
#                         new_user = UserInfo(user_id,user_loc)
#                         self.users[user_id] = new_user
#                         self.user_movement[user_id] = []
#                         self.user_movement.get(user_id).append(user_loc)
#                 except IndexError:
#                     print(line)
#                     pass
#         elif file_type is 'TEXT':
#             self.feeds = {}
#             for line in dataset_file:
#                 try:
#                     elem = line.strip().split("\t")
#                     feed_id = elem[0]
#                     feed_intv = float(elem[1])
#                     feed_latitude = float(elem[1])
#                     feed_longitude = float(elem[2])
#                     feed_loc = Location(feed_latitude,feed_longitude)
#                     new_feed = FeedInfo(feed_id,feed_loc,FeedInfo.default_tag,feed_intv)
#                     self.feeds[feed_id] = new_feed
#                 except IndexError:
#                     print(line)
#                     pass
#         else:
#             print("Not Supported, File type: " + file_type)
#             return
#         dataset_file.close()
#     
#     def readDistribution(self,file_url):
#         dist_file = open(file_url,'r')
#         lines = dist_file.readlines()[2:]
#         self.dist = []
#         for line in lines:
#             self.dist.append(int(line))
#         dist_file.close()
#     
#     def assignDistribution(self):
#         #TODO
#         return
    
    def processGeoText(self):
        geoText_file = open(self.file_url,"r")
        max_lat = float(0)
        min_lat = float(9999)
        max_long = float(0)
        min_long = float(9999)
        self.feeds = {}
        avg_intv = float(0)
        nzerorec = 0
        lid = None
        intv = []
        totlat = float(0)
        totlong = float(0)
        rec = 0
        self.debug_minint = 99999
        self.debug_maxint = 0
        self.debug_totrecord = 0
        self.debug_totint = 0
        for line in geoText_file:
            
            elem = line.strip().split("\t")
            fid = elem[0]
            timed = datetime.strptime(elem[1],"%Y-%m-%dT%H:%M:%S")
            latitude = float(elem[3])
            longitude = float(elem[4])
            
            rloc = LocationRecord(latitude,longitude,timed,fid)
            #self.moveint[rloc.int].append(rloc)
            
            
            if fid in self.movements:
                self.movements[fid].append(rloc)
            else:
                move_list = list()
                move_list.append(rloc)
                self.movements[fid] = move_list
                LocationRecord.userBegin[fid] = timed
                
            if fid != lid:
                if lid != None:
                    if(totlat/rec > max_lat):
                        max_lat = totlat/rec
                    if(totlong/rec > max_long):
                        max_long = totlong/rec
                    if(totlat/rec < min_lat):
                        min_lat = totlat/rec
                    if(totlong/rec < min_long):
                        min_long = totlong/rec
                    loc = Location(totlat/rec,totlong/rec)
                    intv.sort()
                    totintv = 0.0
                    for i in range(0,len(intv)-1):
                        totintv += (intv[i+1] - intv[i]).total_seconds() / LocationRecord.TimeScale
                    totintv /= len(intv) - 1
                    if totintv != 0:
                        avg_intv += totintv
                        nzerorec += 1
                    else:
                        totintv = random.randint(100,1000)
                    new_feed = FeedInfo(lid,loc,FeedInfo.default_tag,int(math.ceil(totintv)))
                    self.feeds[lid] = new_feed
                lid = fid
                rec = 1
                totlat = latitude
                totlong = longitude
                intv = []
                intv.append(timed)
                
            else:
                intv.append(timed)
                rec += 1
                totlat += latitude
                totlong += longitude
        
        
        print("Average Intv: " + str(avg_intv/nzerorec) + " Max Lat: " + str(max_lat) + "Max Long: " + str(max_long) + " Min Lat: " + str(min_lat) + " Min Long: " + str(min_long))
        #File read
        
    def processSNAP(self):
        snap_file = open(self.file_url,"r")
        self.feeds = {}
        max_lat = float(0)
        min_lat = float(9999)
        max_long = float(0)
        min_long = float(9999)
        avg_intv = 0.0
        nzerorec = 0
        lid = None
        intv = []
        totlat = 0.0
        totlong = 0.0
        rec = 0
        self.debug_minint = 99999
        self.debug_maxint = 0
        self.debug_totrecord = 0
        self.debug_totint = 0
        for line in snap_file:
            elem = line.strip().split()
            if(len(elem) != 5):
                print(line)
                continue
            elif elem[2] == "0.0" and elem[3] == "0.0":
                continue
            fid = elem[0]
            timed = datetime.strptime(elem[1],"%Y-%m-%dT%H:%M:%SZ")
            latitude = float(elem[2])
            longitude = float(elem[3])
            
            rloc = LocationRecord(latitude,longitude,timed,fid)
            
            if fid in self.movements:
                self.movements[fid].append(rloc)
            else:
                move_list = list()
                move_list.append(rloc)
                self.movements[fid] = move_list
                LocationRecord.userBegin[fid] = timed

            
            if fid != lid:
                if lid != None:
                    if(totlat/rec > max_lat):
                        max_lat = totlat/rec
                    if(totlong/rec > max_long):
                        max_long = totlong/rec
                    if(totlat/rec < min_lat):
                        min_lat = totlat/rec
                    if(totlong/rec < min_long):
                        min_long = totlong/rec

                    loc = Location(totlat/rec,totlong/rec)
                    intv.sort()
                    totintv = 0.0
                    if len(intv) > 1:
                        for i in range(0,len(intv)-1):
                            totintv += abs((intv[i+1] - intv[i]).total_seconds() / LocationRecord.TimeScale)
                        totintv /= len(intv) - 1
                    if totintv != 0:
                        avg_intv += totintv
                        nzerorec += 1
                    else:
                        totintv = random.randint(100,1000)
                    new_feed = FeedInfo(lid,loc,FeedInfo.default_tag,int(math.ceil(totintv)))
                    self.feeds[lid] = new_feed

                lid = fid
                rec = 1
                totlat = latitude
                totlong = longitude
                intv = []
                intv.append(timed)
            else:
                intv.append(timed)
                rec += 1
                totlat += latitude
                totlong += longitude
        print("Average Intv: " + str(avg_intv/nzerorec) + " Max Lat: " + str(max_lat) + "Max Long: " + str(max_long) + " Min Lat: " + str(min_lat) + " Min Long: " + str(min_long))

    
#     def saveData(self):
#         output = open(self.file_url_out,"w")
#         if self.feeds is not None:
#             for fid,feed in self.feeds.items():
#                 line = str(id) + "\t" + str(feed.update_intv) + "\t" + str(feed.loc.x_dimension) + "\t" + str(feed.loc.y_dimension)
#                 output.write(line+"\n")
#         elif self.users is not None:
#             for uid,user in self.users.items():
#                 line = str(id) + "\t" + str(user.query_intv) + "\t" + user.locRecord.toStr()
#                 output.write(line+"\n")
#         output.close()
                
    def sampleUsers(self,user_feed_ratio,is_moving_feed):
        '''
            Sample user from feeds, write for GeoText dataset
            The number of user generated will be the number of feeds multiply user_feed_ratio
            The generated users will be removed from feed set.
            The sampling is using a uniform distribution
        '''
        if self.feeds is not None:
            self.users = {}
            sampled_feed_keys = random.sample(list(self.feeds),int(math.floor(len(self.feeds.items()) * user_feed_ratio)))
#             max_change_time = 0
#             last_interval = 0
#             tot_interval = 0
            for key in sampled_feed_keys:
                feed = self.feeds[key]
                locrlist = self.movements[feed.feed_id]
                nkey = "U"+key
                #feed.loc.uid = nkey
                locr_user = LocationRecord(feed.loc.latitude,feed.loc.longitude,None,nkey)
                
                for locr in locrlist:
#                     if locr.int > max_change_time:
#                         max_change_time = locr.int
#                         tot_interval += locr.int - last_interval
#                         last_interval = locr.int
                    locr.uid = nkey
                    if locr.int not in self.moveint:
                        self.moveint[locr.int] = list()
                    self.moveint[locr.int].append(locr)
                    
                if feed.update_intv > self.debug_maxint:
                    self.debug_maxint = feed.update_intv
            
                if feed.update_intv < self.debug_minint and feed.update_intv != 0:
                    self.debug_minint = feed.update_intv
                
                self.debug_totrecord += len(locrlist)
                self.debug_totint += feed.update_intv
                
#                 print("Max Int: " + str(max_change_time) + "\t Avg Int: " + str(tot_interval / len(locrlist)))
                
                del self.feeds[key]
                new_user = UserInfo(nkey,locr_user,UserInfo.default_range,feed.update_intv,UserInfo.default_tags)
                self.users[nkey] = new_user
                
                if is_moving_feed:
                   del self.movements[feed.feed_id] 
                
            if not is_moving_feed:
                self.movements.clear()
                
    def mapLocationToFeed(self):
        if self.feeds is not None and self.movements is not None:
            gridEventMap = defaultdict(list)
            for fid,locrlist in self.movements.items():
                for locr in locrlist:
                    loc = Location(locr.latitude,locr.longitude)
                    if loc in gridEventMap.keys():
                        gridEventMap[loc].append(locr)
                    else:
                        elist = list()
                        elist.append(locr)
                        gridEventMap[loc] = elist
            self.movements.clear()
            self.feeds.clear()
            for gid,elist in gridEventMap.items():
                intv = list()
                for locr in elist:
                    intv.append(locr.loc_timestamp)
                totintv = 0.0
                if len(intv) > 1:
                    for i in range(0,len(intv)-1):
                        totintv += abs((intv[i+1] - intv[i]).total_seconds() / LocationRecord.TimeScale)
                    totintv /= len(intv) - 1
                lid = "U"+str(gid.x_dimension)+"-"+str(gid.y_dimension)
                new_feed = FeedInfo(lid,gid,FeedInfo.default_tag,int(math.ceil(totintv)))
                self.feeds[lid] = new_feed
                
    
    def storeInputData(self,filename):
        '''
            Store user map, feed map and user interval map
        '''
        output = open(filename,"w")
        line= str(len(self.feeds)) + "\t" + str(len(self.users)) + "\t" + str(len(self.moveint))
        output.write(line + "\n")
        
        for fid,feed in self.feeds.items():
            line = feed.toStr()
            output.write(line+"\n")
        
        for uid,user in self.users.items():
            line = user.toStr()
            output.write(line+"\n")
        
        for interval,locrlist in self.moveint.items():
            line = str(interval) + "\t" + str(len(locrlist))
            output.write(line+"\n")
            for locr in locrlist:
                line = locr.toStr()
                output.write(line+"\n")
    
    @classmethod
    def loadInputData(cls,filename):
        inputfile = open(filename,"r")
        feedMap = {}
        userMap = {}
        moveint = defaultdict(list)
        lcount = 0
        fcount = 0
        ucount = 0
        mcount = 0
        c_int = -1
        mx_int = 0
        for line in inputfile:
            if lcount == 0:
                elem = line.strip().split("\t")
                fcount = int(elem[0])
                ucount = int(elem[1])
                mcount = int(elem[2])
            elif lcount <= fcount:
                f = FeedInfo.loadFromLine(line)
                feedMap[f.feed_id] = f
            elif lcount <= fcount + ucount:
                u = UserInfo.loadFromLine(line)
                userMap[u.user_id] = u
            else:
                elem = line.strip().split("\t")
                if len(elem) == 2:
                    c_int = int(math.ceil(int(elem[0]) / LocationRecord.TimeScale))
                    if c_int > mx_int:
                        mx_int = c_int
                    if c_int not in moveint:
                        moveint[c_int] = list()
                else:
                    lcr = LocationRecord.loadFromLine(line)
                    moveint[c_int].append(lcr)
            lcount += 1
        inputfile.close()
        #print("Finished Loading, feed: " + str(len(feedMap)) + " user: " + str(len(userMap)) + " moveint: " + str(len(moveint)))
        #print("Max Interval: " + str(mx_int) + " Total: " + str(len(moveint)))
        data = DatasetIO(filename)
        data.feeds = feedMap
        data.users = userMap
        data.moveint = moveint
        data.max_int = mx_int
        return data
        
    
    @classmethod
    def storeViewSelectionPlan(cls,plan,filename):
        '''
            Store a view selection plan to a file
            the plan is a nammed tuple with the definition as
                Plan = collections.namedtuple('Plan',['ViewMap','FeedMap','UserMap','FeedViewMap','QueryPlan'])
        '''
        output = open(filename,"w")
        line=str(len(plan.FeedMap))+"\t"+str(len(plan.UserMap))+"\t"+str(len(plan.ViewMap))+"\t"+str(len(plan.FeedViewMap))+"\t"+str(len(plan.QueryPlan))
        output.write(line+"\n")
        #Headlne FeedCount UserCount MaterializedViewCount FeedViewMapCount QueryPlanCount
        for fid,feed in plan.FeedMap.items():
            line = feed.toStr()
            output.write(line+"\n")
        
        for uid,user in plan.UserMap.items():
            line = user.toStr()
            output.write(line+"\n")
            
        for vid,view in plan.ViewMap.items():
            line = view.toStr()
            output.write(line+"\n")
            
        for feedid,viewset in plan.FeedViewMap.items():
            line = feedid
            for view in viewset:
                if view.view_id not in plan.ViewMap:
                    print("Error view with id: " + view.view_id)
                line += "\t" + view.view_id
            output.write(line+"\n")
        
        for user,viewset in plan.QueryPlan.items():
            if user.user_id not in plan.UserMap:
                print("Error user with id: " + user.user_id)
            line = user.user_id
            for view in viewset:
                if view.view_id not in plan.ViewMap:
                    print("Error view with id: " + view.view_id)
                line += "\t" + view.view_id
            output.write(line+"\n")
        output.close()
    
    @classmethod        
    def loadViewSelectionPlan(cls,filename):    
        inputfile = open(filename,"r")
        feedMap = {}
        userMap = {}
        viewMap = {}
        feedviewMap = defaultdict(set)
        queryPlan = defaultdict(set)
        
        lcount = 0
        totFeed = 0
        totUser = 0
        totView = 0
        totFvm = 0
        totQp = 0
        for line in inputfile:
            if lcount == 0:
                elem = line.strip().split("\t")
                totFeed = int(elem[0])
                totUser = int(elem[1])
                totView = int(elem[2])
                totFvm = int(elem[3])
                totQp = int(elem[4])
            elif lcount <= totFeed:
                f = FeedInfo.loadFromLine(line)
                feedMap[f.feed_id] = f
            elif lcount <= totFeed+totUser:
                u = UserInfo.loadFromLine(line)
                userMap[u.user_id] = u
            elif lcount <= totFeed+totUser+totView:
                v = View.loadFromLine(line, feedMap, userMap)
                viewMap[v.view_id] = v
            elif lcount <= totFeed+totUser+totView+totFvm:
                elem = line.strip().split("\t")
                fid = elem[0]
                viewset = set()
                for i in range(1,len(elem)):
                    viewset.add(viewMap[elem[i]])
                feedviewMap[fid]=viewset
            elif lcount <= totFeed+totUser+totView+totFvm+totQp:
                elem = line.strip().split("\t")
                uid = elem[0]
                viewset = set()
                for i in range(1,len(elem)):
                    viewset.add(viewMap[elem[i]])
                queryPlan[userMap[uid]] = viewset
            else:
                print("Error Line: " + line)
            lcount += 1
        
        inputfile.close()
        Plan = collections.namedtuple('Plan',['ViewMap','FeedMap','UserMap','FeedViewMap','QueryPlan'])
        P = Plan(ViewMap=viewMap,FeedMap=feedMap,UserMap=userMap,FeedViewMap=feedviewMap,QueryPlan=queryPlan)
        return P

class Translator(object):
    
    def __init__(self,viewPlan,userFollowDict):
        '''
            Take the view selection plan generated by algorithm as input and
            generate ViewSelectionRedis compatible format
        '''
        self.viewPlan = viewPlan
        self.createViewGraph(userFollowDict)
    
    
    def createViewGraph(self,userFollowDict):
        
        #Translate feed id to integer
        int_fid = 0
        fidMap = {}
        for fid,feed in self.viewPlan.FeedMap.items():
            fidMap[fid] = int_fid
            int_fid += 1
        
        viewGraph = defaultdict(set)
        if self.viewPlan is not None:
            for vid,view in self.viewPlan.ViewMap.items():
                viewGraph[view] = view.feed_set
            
            for uid,user in self.viewPlan.UserMap.items():
                if len(self.viewPlan.QueryPlan[user]) > 1:
                    new_view = View("VirtualV" + uid,userFollowDict[uid],self.viewPlan.QueryPlan[user],0,float(1.0/user.query_intv))
                    new_view.isMaterialized = False
                    viewGraph[new_view] = self.viewPlan.QueryPlan[user]
                    
        self.fidMap = fidMap
        self.viewGraph = viewGraph
    
    def store(self,ftype,planCost,filename):
        '''
            Store the view selection plan into ViewSelectionRedis compatible format
        '''
        if self.viewGraph is None:
            print("Error! Translate before Store!")
        else:
            output = open(filename,"w")
            line = "Algorithm type: " + str(ftype)
            output.write(line+"\n")
            line = "Total Push Cost: " + str(planCost[0])
            output.write(line+"\n")
            line = "Total Pull Cost: " + str(planCost[1])
            output.write(line+"\n")
            line = "Total view: " + str(len(self.viewGraph))
            output.write(line+"\n")
            line = "Mat View: " + str(len(self.viewPlan.ViewMap))
            output.write(line+"\n")
            line = "Shared View: Unknown"
            output.write(line+"\n")
            line = str(len(self.viewGraph))
            output.write(line+"\n")
        
            for view,queryplan in self.viewGraph.items():
                line = view.toStrTranslator()
                output.write(line+"\n")
            output.close()
            print("Stored")
        
# class TagTree(object):
#     '''
#     Tag tree storaging tag relationship
#         Format:
#             Tag->prefix id
#     '''
#     def __init__(self):
#         self.data={}
#         self.size=0
#         
#     def insert(self,node,parent):    
#         if self.data.has_key(parent):
#             self.data[node]=self.data.get(parent)+'\t'+node
#         else:
#             print("Insert root node parent")
#             self.data[parent]=parent
#     
#     def check(self,fromNode,toNode):debug_maxint
#         if self.data.has_key(fromNode) and self.data.has_key(toNode):
#             tagList = self.data.get(toNode).split('\t')
#             if fromNode in tagList:
#                 return True
#             else:
#                 return False
#         else:
#             print("Tag not exist in tree")
#             return False

class Transaction(object):
    
    def __init__(self,timeinterval,viewlist,message=None,isUpdate=False):
        self.interval = timeinterval
        self.viewlist = viewlist
        self.isUpdate = isUpdate
        if isUpdate:
            self.message = str(timeinterval)
    
    def toStr(self):
        line = str(self.interval) + '\t' + str(self.isUpdate) + "\t"
        if self.isUpdate:
            line += self.message + "\t"
        i = 1
        for vname in self.viewlist:
            if i < len(self.viewlist):
                line += vname + " "
            else:
                line += vname
            i += 1
        return line