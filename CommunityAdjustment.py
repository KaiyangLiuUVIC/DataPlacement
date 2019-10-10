# -*- coding: utf-8 -*-
"""
Created on Mon Dec 23 21:47:00 2018

@author: inuyy
"""
from settings import *
import numpy as np 
import time
import sys
import threading
from queue import Queue
import copy

#node=36
#data = 4509
ReqPat = np.load('RequestPattern.npy', allow_pickle=True)
P = len(ReqPat)                                                                #number of request patterns P
ReqPatData = np.load('ReqPatData.npy', allow_pickle=True)                      #Data are included in request patterns
ReqFrePat_Online = np.load('ReqFrePat_Online.npy', allow_pickle=True)
W_Data_Online = np.round(epsion*np.load('W_Data_Online.npy', allow_pickle=True))
R_Data_Online = np.load('R_Data_Online.npy', allow_pickle=True)
MasterLoc = np.load('MasterLoc.npy', allow_pickle=True)
Original_data_loc = np.load('Original_data_loc.npy', allow_pickle=True)
#w1=0.05                                                                        #Multi-get
#w2=0.01
T = 60
#Theta=2
Place  = np.load('Place.npy', allow_pickle=True)
Route  = np.load('Route.npy', allow_pickle=True)
C1=np.zeros((node))
C2=np.zeros((node))
C3=np.zeros((node))
q=[]
for i in range(node):
    q.append(Queue())

outgoing_traffic=np.zeros((node), dtype=np.int) 
Placed_data_number=np.zeros((node,T-1), dtype=np.int)
Node_changed=np.zeros((node,data), dtype=np.int)

def ReqestRouting(Place,Data_in_Pat,j):
    Route1 = np.zeros((len(Data_in_Pat)), dtype=np.int) 
    Route1[np.where(Place[j-1,Data_in_Pat-1]==1)]=j
    #xxx=0
    while min(Route1)==0:
        #xxx+=1
        Tem_node = np.argmax(np.sum(Place[:,Data_in_Pat[np.where(Route1==0)]-1],axis=1))+1
        #if xxx>len(Data_in_Pat):
            #print('xxy',Place[:,Data_in_Pat[np.where(Route1==0)]-1],Place[:,43259-1])
        Route1[np.where(Place[Tem_node-1,Data_in_Pat-1]==1)]=Tem_node
        #if xxx>len(Data_in_Pat):
            #print('xxx',np.where(Place[Tem_node-1,Data_in_Pat-1]==1),Route1,Tem_node)
            #break
    Route1[np.where(Place[j-1,Data_in_Pat-1]==1)]=j
    return Route1

def CostFun(Place,j,t):
    Route2={}
    Cost1 = 0
    for i in range(P):
        #print('route',i)
        Data_in_Pat = np.asarray(ReqPat[i])
        Route2[i]=ReqestRouting(Place,Data_in_Pat,j+1)
        if i in Data_in_Pat:
            node_num= len(list({}.fromkeys(Route2[i]).keys()))-1
        else:
            node_num= len(list({}.fromkeys(Route2[i]).keys()))     
        Cost1 += w1*ReqFrePat_Online[j][t][i]*node_num
    Cost2 = w2*sum(np.multiply(1-Place[j],R_Data_Online[j][t]))
    Temp_loc=copy.deepcopy(Place[j])
    Temp_loc[Original_data_loc[j]]=0
    Cost3 = w2*sum(np.multiply(Temp_loc,W_Data_Online[t]))
    #Cost3 = w2*sum(np.multiply(sum(Place)-1,W_Data_Online[t]))
    return Route2,Cost1,Cost2,Cost3

def CommunAdjustment(j):
    sys.stdout.write('Start node %d\n' % (j+1))
    sys.stdout.flush()
    C1_n=0
    C2_n=0
    C3_n=0 
    for t in range(T-1):
        #print('t=',t)
        for i in range(data):
            #print('data',i)
            for j5 in range(q[j].qsize()):
                xx=q[j].get()
                for j1 in range(len(ReqPatData[xx[1]])):
                    j3=ReqPatData[xx[1]][j1]-1
                    Data_in_Pat = np.array(ReqPat[j3])
                    j6=np.where(Data_in_Pat==xx[1]+1)
                    if Route[j][j3][j6]==xx[0]+1:
                        Route[j][j3]=ReqestRouting(Place,Data_in_Pat,j+1)
                      
            if abs(R_Data_Online[j][t+1][i]-R_Data_Online[j][t][i])+abs(W_Data_Online[t+1][i]-W_Data_Online[t][i])>Theta:
                
                Temp_place=copy.deepcopy(Place[j][i])
                
                if Place[j][i] and MasterLoc[i]!=j+1 and R_Data_Online[j][t+1][i]<W_Data_Online[t+1][i]:
                    Place[j][i]=0 
                    #print('Remove',j,i)
                    for k in range(len(ReqPatData[i])):
                        ll=np.where(np.array(ReqPat[ReqPatData[i][k]-1])==i+1)[0]
                        if Route[j][ReqPatData[i][k]-1][ll]==j+1:
                            x=list({}.fromkeys(Route[j][ReqPatData[i][k]-1]).keys())
                            x.remove(j+1)
                            y=set(np.where(Place[:,i]==1)[0]+1)
                            z=set(x).intersection(y)     
                            if len(z):
                                Route[j][ReqPatData[i][k]-1][ll]=list(z)[0]
                            else:
                                Route[j][ReqPatData[i][k]-1][ll]=MasterLoc[i]
                    
                if Place[j][i]==0 and R_Data_Online[j][t+1][i]>0:
                    Cost_py = 0
                    Place[j][i]=1
                    #print('Add',j,i)
                    for j1 in range(len(ReqPatData[i])):
                        Theta_l = 0
                        j3=ReqPatData[i][j1]-1
                        Data_in_Pat = ReqPat[j3]
                        Involved_nodes = list({}.fromkeys(Route[j][j3]).keys())
                        Accessible_data=copy.deepcopy(Place[:,np.asarray(Data_in_Pat)-1])
                        Accessible_data=Accessible_data[np.asarray(Involved_nodes)-1]
                        sum_Node=sum(Accessible_data)
                        for j2 in range(len(Involved_nodes)):
                             if Involved_nodes[j2]!=j+1:
                                 Theta_l += min(sum_Node-Accessible_data[j2])
                        Cost_py+=w1*(min(1,Theta_l))*ReqFrePat_Online[j][t+1][j3]
                        
                    if Cost_py+w2*R_Data_Online[j][t+1][i]<w2*W_Data_Online[t+1][i]:
                        Place[j][i]=0
                    
                    if Place[j][i]==1:
                        for j1 in range(len(ReqPatData[i])):
                            j3=ReqPatData[i][j1]-1
                            Data_in_Pat = np.asarray(ReqPat[j3])
                            Route[j][j3]=ReqestRouting(Place,Data_in_Pat,j+1)
               
                #print('done',i)
                Index_Place=Place[j][i]-Temp_place                             #1 if data is placed, 0 if data is deleted
                if Index_Place!=0:
                    Node_changed[j][i]=1
                    Change_Place=(j,i,Index_Place)
                    for nj in range(node):   
                        if nj!=j and Place[nj][i]==0 and Index_Place==-1:
                            #print('nj',nj)
                            q[nj].put(Change_Place)

        [Route[j],Cost1,Cost2,Cost3]=CostFun(Place,j,t+1)
        Placed_data_number[j][t]=sum(Place[j])
        for x1 in range(P):
            for x2 in range(len(Route[j][x1])):
                if Route[j][x1][x2]-1!=j:
                    outgoing_traffic[Route[j][x1][x2]-1]+=ReqFrePat_Online[j][t+1][x1]
        print(t,j,'Cost',Cost1,Cost2,Cost3)
        #sys.stdout.write(t,j,'Cost',Cost1+Cost2+Cost3)
        #sys.stdout.flush()
        C1_n+=Cost1
        C2_n+=Cost2
        C3_n+=Cost3
        
    sys.stdout.write('Node %d done\n' % (j+1))
    sys.stdout.flush() 
    return C1_n,C2_n,C3_n
                        
                
class MyThread(threading.Thread):
    def __init__(self, func, args, name=''):
        threading.Thread.__init__(self)
        self.name = name
        self.func = func
        self.args = args
 
    def run(self):
        self.result=self.func(*self.args)
        
    def get_result(self):
        try:
            return self.result
        except Exception:
            return None
       
def mainLoop(node):
    threads = []
    nloops = list(range(node))
    tt = time.time() 
        
    for i in nloops:
        t = MyThread(CommunAdjustment,(i,),CommunAdjustment.__name__)
        threads.append(t)
        
    for i in nloops: 
        threads[i].start()
 
    for i in nloops: 
        threads[i].join()
        C=threads[i].get_result()
        print('C',C)
        C1[i]=C[0]
        C2[i]=C[1]
        C3[i]=C[2]
    print(time.time()-tt)

if __name__ == '__main__':
    print('loop:')
    #mainLoop(1)
    mainLoop(node)
                    

       
        