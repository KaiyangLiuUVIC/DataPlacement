# -*- coding: utf-8 -*-
"""
Created on Sun Dec 22 23:10:32 2018

@author: inuyy
"""
from settings import *
import csv
import numpy as np
import random
import numpy
from scipy import stats

CONST_Time = 3600
s=60
t_slot=CONST_Time /s
random.seed(CONST_SEED)
np.random.seed(CONST_SEED)
para = 1.25
ReqPat = np.load('RequestPattern.npy',allow_pickle=True)
ReqFrePat_Online = np.load('ReqFrePat_Online.npy',allow_pickle=True)
num = len(ReqPat)
ReqFrePat_Online = np.zeros(((node,s,num)), dtype=np.int)
ReqFrePat = np.zeros(((node,num)), dtype=np.int)
R_Data_Online = np.zeros(((node,s,data)), dtype=np.int)
R_Data = np.zeros(((node,data)), dtype=np.int)
W_Data_Online = np.zeros(((s,data)), dtype=np.int)
W_Data = np.zeros(((data)), dtype=np.int)

def genRates(para, num):
    seq = [i for i in range(0,num)]
    ans = [0.0 for i in range(0,num)]
    random.shuffle(seq)
    for i in range(len(seq)):
        ans[seq[i]] = float(stats.zipf.pmf(i+1, para))
    return ans

def OnlineRates(nid):   
    Read=np.zeros(((s)), dtype=np.int) 
    Write=np.zeros(((s)), dtype=np.int)
    with open('D:\MSR-Cambridge/%d.csv'% (nid+1),'rt') as csvfile:
        reader = csv.reader(csvfile)
        for Trace_i,rows in enumerate(reader):
            #print(Trace_i,rows)
            if Trace_i == 0:
                row = rows
                Init_time=float(row[0])/10000000     
            row = rows
            if Trace_i >= 0 and ((float(row[0])/10000000-Init_time)<=CONST_Time and (float(row[0])/10000000-Init_time)>=0):
                i=int((float(row[0])/10000000-Init_time)/t_slot)
                if row[3] == 'Write':
                    Write[i]+=1
                if row[3] == 'Read':
                    Read[i]+=1
            else:
                break
    print(nid,'done')
    return Read,Write

RF=np.zeros(((node,s)), dtype=np.int) 
WF=np.zeros(((node,s)), dtype=np.int)

for i in range(node):
    [RF[i],WF[i]]=OnlineRates(i)
    print(RF[i])
    print(WF[i])

###############################################################################

#Request rates in patterns
Total_R=0  
for i in range(node):
    print('Read patterns',i)
    for k in range(s):
        ans = genRates(para, num)
        Rates = [round(RF[i,k]*ans[j]) for j in range(len(ans))]
        Rates[random.randint(0,num-1)]+=RF[i,k]-sum(Rates)
        Total_R+=sum(Rates)
        ReqFrePat_Online[i,k]=Rates   
np.save('ReqFrePat_Online.npy',ReqFrePat_Online)
ReqFrePat=[sum(ReqFrePat_Online[i]) for i in range(node)]
np.save('ReqFrePat.npy',ReqFrePat)

#data read rates
for i in range(node):
    print('Read data',i)
    for k1 in range(s):
        R_Data_node_Online = [0 for j in range(data)]
        for j in range(len(ReqPat)):
            for k in range(len(ReqPat[j])):
                R_Data_node_Online[ReqPat[j][k]-1]+=ReqFrePat_Online[i][k1][j]
        R_Data_Online[i,k1]=R_Data_node_Online    
np.save('R_Data_Online.npy',R_Data_Online)
R_Data=[sum(R_Data_Online[i]) for i in range(node)]
np.save('R_Data.npy',R_Data)

#Write rates in data
Original_data_loc = np.load('Original_data_loc.npy',allow_pickle=True)
for i in range(node):
    print(i)
    data_in_node = len(Original_data_loc[i])
    for k1 in range(s):
        ans = genRates(para, data_in_node)
        WRates = [round(WF[i][k1]*ans[j]) for j in range(len(ans))]
        WRates[random.randint(1,data_in_node)-1]+=WF[i][k1]-sum(WRates)
        for k in range(data_in_node):
            W_Data_Online[k1][Original_data_loc[i][k]]=WRates[k]
    
np.save('W_Data_Online.npy',W_Data_Online)
W_Data=sum(W_Data_Online)
np.save('W_Data.npy',W_Data)