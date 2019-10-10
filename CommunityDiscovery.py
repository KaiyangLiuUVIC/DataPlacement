#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Dec 16 08:53:55 2018

@author: liukaiyang
"""
from settings import *
import numpy as np 
import time
import sys
import threading 
import copy

ReqPat = np.load('RequestPattern.npy', allow_pickle=True)
P = len(ReqPat)                                                                #number of request patterns P
ReqPatData = np.load('ReqPatData.npy', allow_pickle=True)                      #Data are included in request patterns
ReqFrePat = np.load('ReqFrePat.npy', allow_pickle=True)
#W_Data = np.load('W_Data.npy', allow_pickle=True)
W_Data = np.round(epsion*np.load('W_Data.npy', allow_pickle=True))
R_Data = np.load('R_Data.npy', allow_pickle=True)
MasterLoc = np.load('MasterLoc.npy', allow_pickle=True)
Original_data_loc = np.load('Original_data_loc.npy', allow_pickle=True)

Place  = np.zeros((node,data), dtype=np.int)     

for i in range(node):   
    x=np.int64(R_Data[i]>=W_Data)
    y=np.int64(R_Data[i]>0)
    x=np.multiply(x,y)
    Place[i][np.where(x==1)]=1
    Place[i][np.asarray(Original_data_loc[i])]=1
    Place[i][np.where(W_Data==0)]=1
      
def ReqestRouting(Place,Data_in_Pat,j):
    Route = np.zeros((len(Data_in_Pat)), dtype=np.int) 
    Route[np.where(Place[j-1,Data_in_Pat-1]==1)]=j
    while min(Route)==0:
        Tem_node = np.argmax(np.sum(Place[:,Data_in_Pat[np.where(Route==0)]-1],axis=1))+1
        Route[np.where(Place[Tem_node-1,Data_in_Pat-1]==1)]=Tem_node
    Route[np.where(Place[j-1,Data_in_Pat-1]==1)]=j
    return Route
           
def CommunDiscovery(MasterLoc,ReqFrePat,ReqPat,ReqPatData,R_Data,W_Data,node,data,P,w1,w2,j):
    sys.stdout.write('Start node %d\n' % (j+1))
    sys.stdout.flush()
    Route={}
    for i in range(P):
        Data_in_Pat = np.asarray(ReqPat[i])
        Route[i]=ReqestRouting(Place,Data_in_Pat,j+1)
        
    for i in range(data):
        if Place[j][i]==0 and R_Data[j][i]>0:
            Cost_py = 0
            Place[j][i]=1           
            for j1 in range(len(ReqPatData[i])):
                Theta = 0
                j3=ReqPatData[i][j1]-1
                Data_in_Pat = ReqPat[j3]
                Involved_nodes = list({}.fromkeys(Route[j3]).keys())
                Accessible_data=copy.deepcopy(Place[:,np.asarray(Data_in_Pat)-1])
                Accessible_data=Accessible_data[np.asarray(Involved_nodes)-1]
                sum_Node=sum(Accessible_data)
                for j2 in range(len(Involved_nodes)):
                    if Involved_nodes[j2]!=j+1:
                        Theta += min(sum_Node-Accessible_data[j2])
                Cost_py+=w1*(min(1,Theta))*ReqFrePat[j][j3]
               
            if Cost_py+w2*R_Data[j][i]<w2*W_Data[i]:
                Place[j][i]=0
                  
    sys.stdout.write('Node %d done\n' % (j+1))
    sys.stdout.flush()     
    return Place[j]
                
def CostFun(Place,j):
    Route={}
    Cost1 = 0
    for i in range(P):
        Data_in_Pat = np.asarray(ReqPat[i])
        Route[i]=ReqestRouting(Place,Data_in_Pat,j+1)
        if i in Data_in_Pat:
            node_num= len(list({}.fromkeys(Route[i]).keys()))-1
        else:
            node_num= len(list({}.fromkeys(Route[i]).keys()))     
        Cost1 += w1*ReqFrePat[j][i]*node_num
    Cost2 = w2*sum(np.multiply(1-Place[j],R_Data[j]))
    Temp_loc=copy.deepcopy(Place[j])
    Temp_loc[Original_data_loc[j]]=0
    Cost3 = w2*sum(np.multiply(Temp_loc,W_Data))
    #Cost3 = w2*sum(np.multiply(sum(Place)-1,W_Data))
    return Route,Cost1,Cost2,Cost3

class MyThread(threading.Thread):
    def __init__(self, func, args, name=''):
        threading.Thread.__init__(self)
        self.name = name
        self.func = func
        self.args = args
 
    def run(self):
        self.func(*self.args)

###### Running time: max(t_time)
#t_time=[]    
#def mainLoop(node):
#    threads = []
#    nloops = list(range(node))
#    
#    for i in nloops:
#        tt = time.time() 
#        t = MyThread(CommunDiscovery,(MasterLoc,ReqFrePat,ReqPat,ReqPatData,R_Data,W_Data,node,data,P,w1,w2,i),CommunDiscovery.__name__)
#        threads.append(t)
#        threads[i].start()
#        threads[i].join()
#        t_time.append(time.time()-tt)
#        print(time.time()-tt)
        
def mainLoop(node):
    threads = []
    nloops = list(range(node))
    tt = time.time() 
    for i in nloops:
        t = MyThread(CommunDiscovery,(MasterLoc,ReqFrePat,ReqPat,ReqPatData,R_Data,W_Data,node,data,P,w1,w2,i),CommunDiscovery.__name__)
        threads.append(t)
        
    for i in nloops: 
        threads[i].start()
 
    for i in nloops: 
        threads[i].join()
    print(time.time()-tt)

Route  = []
if __name__ == '__main__':
    print('loop:')
    #mainLoop(1)
    mainLoop(node)
    C1=0
    C2=0
    C3=0
    for i in range(node):
    #for i in range(1):
        print('Route node', i)
        [Route_T,Cost1,Cost2,Cost3]=CostFun(Place,i)
        C1+=Cost1
        C2+=Cost2
        C3+=Cost3
        Route.append(Route_T)
        
    np.save('Route.npy',Route)
    np.save('Place.npy',Place)
  
outgoing_traffic=np.zeros((node), dtype=np.int)  
for i in range(node):
    for j in range(P):
        for k in range(len(Route[i][j])):
            if Route[i][j][k]-1!=i:
                outgoing_traffic[Route[i][j][k]-1]+=ReqFrePat[i][j]

for i in range(data):
    outgoing_traffic[MasterLoc[i]-1]+=(sum(Place)[i]-1)*W_Data[i]
    
Placed_data_number=np.sum(Place,axis=1)