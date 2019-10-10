#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Dec 16 23:30:46 2018

@author: liuky
"""
from settings import *
import numpy as np 
import random

RequestPattern = [[1]]
ReqPatData =[[] for i in range(data)]
DataItems=[]


with open('out.facebook-wosn-links') as reader:
    for index, line in enumerate(reader):
        array = line.split()
        x = int(array[0])
        y = int(array[1])
        if x<=data and y<=data:
            DataItems.append(x)
            DataItems.append(y)
            if RequestPattern[-1][0]==x:
                RequestPattern[-1].append(y)
                ReqPatData[x-1].append(len(RequestPattern))
                ReqPatData[x-1]=list(set(ReqPatData[x-1])) 
                ReqPatData[y-1].append(len(RequestPattern))
                ReqPatData[y-1]=list(set(ReqPatData[y-1])) 
            else:
                RequestPattern.append([x])
                RequestPattern[-1].append(y)
                ReqPatData[x-1].append(len(RequestPattern))
                ReqPatData[x-1]=list(set(ReqPatData[x-1])) 
                ReqPatData[y-1].append(len(RequestPattern))
                ReqPatData[y-1]=list(set(ReqPatData[y-1])) 

DataItem = list(set(DataItems)) 
np.save('RequestPattern.npy',RequestPattern)
np.save('ReqPatData.npy',ReqPatData)


## Master node location for each data item
MasterLoc = []
Original_data_loc = [[] for i in range(node)]

for i in range(data):
    loc = random.randint(1,node)
    MasterLoc.append(loc)
    Original_data_loc[loc-1].append(i)
    
np.save('MasterLoc.npy',MasterLoc)   
np.save('Original_data_loc.npy',Original_data_loc) 


#x=0 
#for i in range(len(RequestPattern)):
#    x+=len(RequestPattern[i])
#print(x/len(RequestPattern))
    

       
