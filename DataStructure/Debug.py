'''
Created on Oct 6, 2015

@author: SDH
'''
from DataStructure.Dataset import DatasetIO, Location, UserInfo, View,\
    LocationRecord, Translator
from DataStructure.Algorithm import GreedyAlgorithm, GridView, GeoFeedAlgorithm,\
    Algorithm, FeedingFrenzy
from _collections import defaultdict
import sys
from datetime import datetime


def generatePlan(inputname,outname,filename,filetype,generateDataset,calculatePlan,dynamic,algorithm,user_ratio,timescale,granularity,qrange,distance):
    
    Location.Base_lat = 30.0 / granularity
    Location.Base_long = 65.0 / granularity
    
    if generateDataset:
        data = DatasetIO(filename)
        if filetype == "SNAP":
            LocationRecord.TimeScale = 100.0
            data.processSNAP()
        elif filetype == "TEXT":
            data.processGeoText()
        data.sampleUsers(user_ratio,True)
        data.mapLocationToFeed()
        data.storeInputData(filename+".R"+str(user_ratio)+".ds.M")
    elif calculatePlan:
        if filetype == "TEXT":
            LocationRecord.TimeScale = timescale
        elif filetype == "SNAP":
            LocationRecord.TimeScale = timescale / 100.0
        UserInfo.default_range = qrange
#         granularity = 1000.0
#        Location.Base_lat = 30.0 / granularity
#        Location.Base_long = 65.0 / granularity

        Algorithm.CHANGED_DISTANCE = distance
        #outname += algorithm + ".S" + str(timescale) + "R" + str(qrange) + "G" + str(granularity) + ".Ratio" + str(user_ratio) + ".D" + str(distance)
        outname += algorithm + ".S" + str(timescale) + "G" + str(granularity) + ".Ratio" + str(user_ratio)
        data = DatasetIO.loadInputData(inputname)
        #print('Loaded')
        users = data.users
        feeds = data.feeds
        if dynamic:    
            userint = data.moveint
            if algorithm == "Greedy":
                alg = GreedyAlgorithm(feeds,users,userint)
            elif algorithm == "Grid":
                alg = GridView(feeds,users,userint)
            elif algorithm == "FF":
                alg = FeedingFrenzy(feeds,users,userint)
            elif algorithm == "Geo":
                alg = GeoFeedAlgorithm(feeds,users,None,userint)
            else:
                print("Undefined Algorithm" + str(algorithm))
            if filetype == "SNAP":
                if data.max_int > 300:
                    max_int = 300
                else:
                    max_int = data.max_int
            elif filetype == "TEXT":
                if data.max_int > 15000:
                    max_int = 15000
                else:
                    max_int = data.max_int
            alg.viewSelectionDynamic(max_int, outname)
        else:
            begintime = datetime.now()
            if algorithm == "Greedy":
                alg = GreedyAlgorithm(feeds,users)
                plan = alg.viewSelection()
            elif algorithm == "Grid":
                plan = GridView.search(feeds, users, dynamic)
            elif algorithm == "FF":
                alg = FeedingFrenzy(feeds,users)
                plan = alg.viewSelection()
            elif algorithm == "Geo":
                alg = GeoFeedAlgorithm(feeds,users)
                plan = alg.viewSelection()
            else:
                print("Undefined Algorithm" + str(algorithm))
            ctime = datetime.now()
            print("Ratio: " + str(user_ratio) + "," + algorithm + " Running time(ms): " + str((ctime-begintime).total_seconds() * 1000))
            
            comPlan = Translator(plan,Algorithm.userFollowDict)
            planCost = Algorithm.calcCost(plan)
            print( algorithm + " Cost: Mat = " + str(planCost[0]) + " Eva: " + str(planCost[1]) + " Total: " + str(planCost[2]))
            comPlan.store(1,planCost,outname)
            
    else:
        print("Undefined Operation")
        
    LocationRecord.userBegin.clear()
    Algorithm.userFollowDict.clear()
    Algorithm.gridFollowDict.clear()
    Algorithm.planDict.clear()
    
    #print("Finished Generating: " + outname)

if __name__ == '__main__':
    
    begintime = datetime.now()
    
    filename_s = "/home/kaijichen/Desktop/LocationViewSelectionBackup/loc-brightkite_totalCheckins.txt"
    outname = "/home/kaijichen/Desktop/LocationViewSelectionBackup/"
    filename_g = "/home/kaijichen/Desktop/LocationViewSelectionBackup/full_text.txt"
    
#     filename_s = "/Users/SDH/Dropbox/DataProcessor/loc-brightkite_totalCheckins.txt"
#     outname = "/Users/SDH/Dropbox/DataProcessor/"
#     filename_g = "/Users/SDH/Dropbox/DataProcessor/full_text.txt"
    
    user_ratios = [0.75,0.8,0.9,0.95]
    
    generateDataset = False
    filetype = "TEXT"
    calculatePlan = True
    dynamic = False
    algorithms = ["Greedy","Grid"]
    altalgorithms = ["Geo"]
    altalgorithms += algorithms
    Onealgorithm = ['Geo']
    
    print(altalgorithms)
    
    timescales = [1000.0,7000.0,10000.0,13000.0] #Default TimeScale set to 100 for SNAP file
    qranges = [3,7,9]
    granularitys = [125.0,250.0,500.0,750.0,1000.0]
    #changed_distance_geo = [12500,50000,sys.maxint]
    #changed_distance_snap = [sys.maxint,600000,150000]
    
    
#     for user_ratio in user_ratios:
#         generatePlan("", "", filename_g, filetype, generateDataset, False, False, "", user_ratio, 0, 250, 0, 50000)

    
#     for user_ratio in user_ratios:
#         inputname = filename_g+".R"+str(user_ratio)+".ds"
#         timescale = 40.0
#         qrange = 5
#         granularity = 250.0
#         for algorithm in altalgorithms:
#             generatePlan(inputname, outname+"ExpPlans/GeoText/Static/UserRatio/", filename_g, filetype, generateDataset, calculatePlan, dynamic, algorithm, user_ratio, timescale, granularity, qrange, 25000)
#       
    user_ratio = 0.9
#     granularity = 250.0
#     qrange = 5
#     timescale = 40.0
#     inputname = filename_g+".R"+str(user_ratio)+".ds"
#     for algorithm in altalgorithms:
#         generatePlan(inputname, outname+"ExpPlans/GeoText/Dynamic/GRP/", filename_g, filetype, generateDataset, calculatePlan, dynamic, algorithm, user_ratio, timescale, granularity, qrange, 25000)
#     timescale = 40.0
    qrmap = dict()
    qrmap[125.0]=1
    qrmap[250.0]=2
    qrmap[500.0]=4
    qrmap[750.0]=6
    qrmap[1000.0]=8
#     granularity = 250.0
#     for granularity in granularitys:        
#         timescale = 40.0
#         qrange = qrmap[granularity]
#         #granularity = 250.0
#         for algorithm in altalgorithms:
#             generatePlan(inputname, outname+"ExpPlans/GeoText/Dynamic/GRP/", filename_g, filetype, generateDataset, calculatePlan, dynamic, algorithm, user_ratio, timescale, granularity, qrange, 25000)
#             ctime = datetime.now()
#             print("S3:Running time: " + str(ctime-begintime))       
    filetype='SNAP'
    timescale = 4000.0
    
    inputname = filename_s+".R"+str(user_ratio)+".ds"
#     for algorithm in altalgorithms:
#         generatePlan(inputname, outname+"ExpPlans/SNAP/MF/", filename_s, filetype, generateDataset, calculatePlan, dynamic, algorithm, user_ratio, timescale, granularity, qrange, 250000)
        #ctime = datetime.now()
        #print("S2:Running time: " + str(ctime-begintime) + "  Alg: " + algorithm)
    for granularity in granularitys:        
        timescale = 4000.0
        qrange = qrmap[granularity]
         #granularity = 250.0
        for algorithm in altalgorithms:
            generatePlan(inputname, outname+"ExpPlans/SNAP/Static/GRP/", filename_s, filetype, generateDataset, calculatePlan, dynamic, algorithm, user_ratio, timescale, granularity, qrange, 250000)
            ctime = datetime.now()
            print("S6:Running time: " + str(ctime-begintime))
    #generatePlan("", "", filename_g, filetype, generateDataset, False, False, "", user_ratio, 0, 250, 0, 50000)
#     filetype='SNAP'
#     generatePlan("", "", filename_s, filetype, generateDataset, False, False, "", user_ratio, 0, 250, 0, 50000)
#         
#     inputname = filename_g+".R"+str(user_ratio)+".ds"
#     for timescale in timescales:        
#         #timescale = 13000.0
#         qrange = 5
#         granularity = 250.0
#         for algorithm in altalgorithms:
#             generatePlan(inputname, outname+"ExpPlans/GeoText/Dynamic/LoadLevel/", filename_g, filetype, generateDataset, calculatePlan, dynamic, algorithm, user_ratio, timescale, granularity, qrange, 25000)
#             ctime = datetime.now()
#             print("S1:Running time: " + str(ctime-begintime))
#      
#     for qrange in qranges:        
#         timescale = 40.0
#         #qrange = 5
#         granularity = 250.0
#         for algorithm in altalgorithms:
#             generatePlan(inputname, outname+"ExpPlans/GeoText/Dynamic/QueryRange/", filename_g, filetype, generateDataset, calculatePlan, dynamic, algorithm, user_ratio, timescale, granularity, qrange, 25000)
#             ctime = datetime.now()
#             print("S2:Running time: " + str(ctime-begintime))
#              
#     for granularity in granularitys:        
#         timescale = 40.0
#         qrange = 5
#         if granularity == 100.0:
#             continue
#         #granularity = 250.0
#         for algorithm in altalgorithms:
#             generatePlan(inputname, outname+"ExpPlans/GeoText/Dynamic/Granularity/", filename_g, filetype, generateDataset, calculatePlan, dynamic, algorithm, user_ratio, timescale, granularity, qrange, 25000)
#             ctime = datetime.now()
#             print("S3:Running time: " + str(ctime-begintime))
#               
#     print("Processing for Dynamic GeoText finished!")
#      
#     filetype = "SNAP"
#     
#     dynamic = True
#     
#     for user_ratio in user_ratios:
#         inputname = filename_s+".R"+str(user_ratio)+".ds"
#         timescale = 4000.0
#         qrange = 5
#         granularity = 250.0
#         for algorithm in altalgorithms:
#         #for algorithm in FFalgorithm:
#             generatePlan(inputname, outname+"ExpPlans/SNAP/Static/UserRatio/", filename_s, filetype, generateDataset, calculatePlan, dynamic, algorithm, user_ratio, timescale, granularity, qrange, 25000)
#        
#     user_ratio = 0.9
# #         
#     inputname = filename_s+".R"+str(user_ratio)+".ds"
#     timescales = [1000.0,7000.0,10000.0,13000.0]
#     for timescale in timescales:        
#          #timescale = 13000.0
#         qrange = 5
#         granularity = 250.0
#         #for algorithm in altalgorithms:
#         for algorithm in FFalgorithm:
#             generatePlan(inputname, outname+"ExpPlans/SNAP/Static/LoadLevel/", filename_s, filetype, generateDataset, calculatePlan, dynamic, algorithm, user_ratio, timescale, granularity, qrange, 250000)
#             ctime = datetime.now()
#             print("S4:Running time: " + str(ctime-begintime))
#              
#     for qrange in qranges:        
#         timescale = 4000.0
#         #qrange = 5
#         granularity = 250.0
#         #for algorithm in altalgorithms:
#         for algorithm in FFalgorithm:
#             generatePlan(inputname, outname+"ExpPlans/SNAP/Static/QueryRange/", filename_s, filetype, generateDataset, calculatePlan, dynamic, algorithm, user_ratio, timescale, granularity, qrange, 250000)
#             ctime = datetime.now()
#             print("S5:Running time: " + str(ctime-begintime))
#             
#     for granularity in granularitys:        
#         timescale = 4000.0
#         qrange = 5
#          #granularity = 250.0
#         #for algorithm in altalgorithms:
#         for algorithm in FFalgorithm:
# #             if granularity == 100.0 and algorithm == 'FF':
# #                 continue
# #             elif granularity == 250:
# #                 continue
#             generatePlan(inputname, outname+"ExpPlans/SNAP/Static/Granularity/", filename_s, filetype, generateDataset, calculatePlan, dynamic, algorithm, user_ratio, timescale, granularity, qrange, 250000)
#             ctime = datetime.now()
#             print("S6:Running time: " + str(ctime-begintime))
#             
#     print("Processing for Static SNAP finished!")
    
    
            


