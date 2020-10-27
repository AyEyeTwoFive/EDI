# -*- coding: utf-8 -*-
"""
Created on Fri Sep 25 15:10:05 2020
Convert x12-formatted EDI data to csv
@author: asef islam
"""
import numpy as np
import pandas as pd
import sys

# Get input file name from command line and read it 
infile = sys.argv[1]
f = open(infile, "r")
data = f.read()

# Find and separate ISA, GS, ST, DTM, DTM
isa_start = data.find("ISA*")
gs_start = data.find("~GS*", isa_start+4,len(data)-1)
st_start = data.find("~ST*", gs_start+4,len(data)-1)
bpt_start = data.find("~BPT*", st_start+4,len(data)-1)
dtm_start1 = data.find("~DTM*", bpt_start+5,len(data)-1)
dtm_start2 = data.find("~DTM*", dtm_start1+5,len(data)-1)
N1_start1 = data.find("~N1*", dtm_start2+5,len(data)-1)
N1_start2 = data.find("~N1*", N1_start1+4,len(data)-1)
detail_start = data.find("~PTD*", N1_start2+4,len(data)-1)

ISA = data[isa_start+4: data.find("~", isa_start+4,len(data)-1)]
ISA = ISA.split("*")
GS = data[gs_start+4:data.find("~", gs_start+4,len(data)-1)]
GS = GS.split("*")
ST = data[st_start+4:data.find("~", st_start+4,len(data)-1)]
ST = ST.split("*")
BPT = data[bpt_start+5:data.find("~", bpt_start+5,len(data)-1)]
BPT = BPT.split("*")
if dtm_start1 > -1:
    DTM1 = data[dtm_start1+5:data.find("~", dtm_start1+5,len(data)-1)]
    DTM1 = DTM1.split("*")
else:
    DTM1 = []
if dtm_start2 > -1:
    DTM2 = data[dtm_start2+5:data.find("~", dtm_start2+5,len(data)-1)]
    DTM2 = DTM2.split("*")
else:
    DTM2 = []
if N1_start1 > -1:
    N1_1 = data[N1_start1+4:data.find("~", N1_start1+4,len(data)-1)]
    N1_1 = N1_1.split("*")
else:
    N1_1 = []
if N1_start2 > -1:
    N1_2 = data[N1_start2+4:data.find("~", N1_start2+4,len(data)-1)]
    N1_2 = N1_2.split("*")
else:
    N1_2 = []
    
# create data frame
df = pd.DataFrame()
df['ISA'] = pd.Series(ISA)
df['GS'] = pd.Series(GS)
df['ST'] = pd.Series(ST)
df['BPT'] = pd.Series(BPT)
df['DTM1'] = pd.Series(DTM1)
df['DTM2'] = pd.Series(DTM2)
df['N1_1'] = pd.Series(N1_1)
df['N1_2'] = pd.Series(N1_2)

# Parse detail loop 
detail = data[detail_start:len(data)-1]
num = detail.count("~PTD*") # how many items

for n in range(num):
   ptd_start =  detail.find("~PTD*")
   dtm_start = detail.find("~DTM*", ptd_start+5, len(detail)-1)
   N1_start = detail.find("~N1*", dtm_start+5,len(detail)-1)
   N2_start = detail.find("~N2*", N1_start+4,len(detail)-1)
   N3_start = detail.find("~N3*", N2_start+4,len(detail)-1)
   N4_start = detail.find("~N4*", N3_start+4,len(detail)-1)
   qty_start = detail.find("~QTY*", N4_start+4,len(detail)-1)
   lin_start = detail.find("~LIN*", qty_start+5,len(detail)-1)
   uit_start = detail.find("~UIT*", lin_start+5,len(detail)-1)
   amt_start = detail.find("~AMT*", uit_start+5,len(detail)-1)
   pid_start = detail.find("~PID*", amt_start+5,len(detail)-1)
   next_ptd = detail.find("~PTD*", pid_start+5,len(detail)-1)
   this_prod = detail[0:next_ptd]
   
   prod = pd.DataFrame()
   PTD = this_prod[ptd_start+5:this_prod.find("~", ptd_start+5,len(this_prod)-1)]
   PTD = PTD.split("*")
   if dtm_start > -1:
       DTM = this_prod[dtm_start+5:this_prod.find("~", dtm_start+5,len(this_prod)-1)]
       DTM = DTM.split("*")
   else:
       DTM = []
   if N1_start > -1:
       N1 = this_prod[N1_start+4:this_prod.find("~", N1_start+4,len(this_prod)-1)]
       N1 = N1.split("*")
   else:
       N1 = []
   if N2_start > -1:
       N2 = this_prod[N2_start+4:this_prod.find("~", N2_start+4,len(this_prod)-1)]
       N2 = N2.split("*")
   else:
       N2 = []
   if N3_start > -1:
       N3 = this_prod[N3_start+4:this_prod.find("~", N3_start+4,len(this_prod)-1)]
       N3 = N3.split("*")
   else:
       N3 = []
   if N4_start > -1:
       N4 = this_prod[N4_start+4:this_prod.find("~", N4_start+4,len(this_prod)-1)]
       N4 = N4.split("*")
   else:
       N4 = []
   if qty_start > -1:
       QTY = this_prod[qty_start+5:this_prod.find("~", qty_start+5,len(this_prod)-1)]
       QTY = QTY.split("*")
   else:
       QTY = []
   if lin_start > -1:
       LIN = this_prod[lin_start+5:this_prod.find("~", lin_start+5,len(this_prod)-1)]
       LIN = LIN.split("*")
   else:
       LIN = []
   if uit_start > -1:
       UIT = this_prod[uit_start+5:this_prod.find("~", uit_start+5,len(this_prod)-1)]
       UIT = UIT.split("*")
   else:
       UIT = []
   if amt_start > -1:
       AMT = this_prod[amt_start+5:this_prod.find("~", amt_start+5,len(this_prod)-1)]
       AMT = AMT.split("*")
   else: 
       AMT = []
   if pid_start > -1:
       PID = this_prod[pid_start+5:this_prod.find("~", pid_start+5,len(this_prod)-1)]
       PID = PID.split("*")
   else:
       PID = []
   
   prod['PTD'] = pd.Series(PTD)
   prod['DTM'] = pd.Series(DTM)
   prod['N1'] = pd.Series(N1)
   prod['N2'] = pd.Series(N2)
   prod['N3'] = pd.Series(N3)
   prod['N4'] = pd.Series(N4)
   prod['QTY'] = pd.Series(QTY)
   prod['LIN'] = pd.Series(LIN)
   prod['UIT'] = pd.Series(UIT)
   prod['AMT'] = pd.Series(AMT)
   prod['PID'] = pd.Series(PID)
   
   df = pd.concat([df, prod], axis=1)
    
   detail = detail[next_ptd:len(detail)-1]
   
# Get closing data
ctt_start = data.find("~CTT*", 0, len(data)-1)
se_start = data.find("~SE*", ctt_start+5, len(data)-1)
ge_start = data.find("~GE*", se_start + 4, len(data)-1)
iea_start = data.find("~IEA*", ge_start + 4, len(data)-1)
if ctt_start > -1:
    CTT = data[ctt_start+5:data.find("~", ctt_start+5,len(data)-1)]
    CTT = CTT.split("*")
else:
    CTT = []
if se_start > -1:
    SE = data[se_start+4:data.find("~", se_start+4,len(data)-1)]
    SE = SE.split("*")
else:
    SE = []
if ge_start > -1:
    GE = data[ge_start+4:data.find("~", ge_start+4,len(data)-1)]
    GE = GE.split("*")
else:
    GE = []
if iea_start > -1:
    IEA = data[iea_start+5:data.find("~", iea_start+5,len(data)-1)]
    IEA = IEA.split("*")
else:
    IEA = []

df['CTT'] = pd.Series(CTT)
df['SE'] = pd.Series(SE)
df['GE'] = pd.Series(GE)
df['IEA'] = pd.Series(IEA)
  
# Output full parsed data
df.to_csv(infile.replace('.x12', '_full.csv'))


# Get formatted and labeled csv output
out = pd.DataFrame([ST], columns = ['Transaction Set ID', 'Transaction Set Control Number'])
out = pd.concat((out, pd.DataFrame([BPT], columns = ['Transaction Set Purpose Code', 'Reference ID',\
                    'Report Creation Date', 'Report Type Code (SS = Seller Sales Report)'])), axis=1)
out = pd.concat((out, pd.DataFrame([DTM1], columns = ['Date/Time Qualifier (090 = Report Start\
                                                      091 = Report End)', 'Report Start Date'])), axis=1)
out = pd.concat((out, pd.DataFrame([DTM2], columns = ['Date/Time Qualifier (090 = Report Start\
                                                      091 = Report End)', 'Report End Date'])), axis=1)
out = pd.concat((out, pd.DataFrame([N1_1], columns = ['Entry Identifier Code (DB = Distributor Branch\
            SU = Supplier/Manufacturer)', 'Name', 'Identification Code Qualifier', 'Identification Code'])), axis=1)
out = pd.concat((out, pd.DataFrame([N1_2], columns = ['Entry Identifier Code ', 'Name'])), axis=1)

PTD = np.transpose(df['PTD'].head(5))
PTD.columns = ['Product Transfer Type Code','','','Reference ID Qualifier', 'Reference ID']
PTD.reset_index(drop=True, inplace=True)
out = pd.concat((out, PTD), axis=1)  

DTM = np.transpose(df['DTM'].head(2))
DTM.columns = ['Date/Time Qualifier','Date']
DTM.reset_index(drop=True, inplace=True)
out = pd.concat((out, DTM), axis=1)  

N1 = np.transpose(df['N1'].head(4))
N1.columns = ['Entity Identifier Code','Name', 'Identification Code Qualifier', 'Identification Code']
N1.reset_index(drop=True, inplace=True)
out = pd.concat((out, N1), axis=1) 

N3 = np.transpose(df['N3'].head(2))
N3.columns = ['Address Information','Address Information']
N3.reset_index(drop=True, inplace=True)
out = pd.concat((out, N3), axis=1) 

N4 = np.transpose(df['N4'].head(4))
N4.columns = ['City Name','State or Province Code','Postal Code', 'Country Code']
N4.reset_index(drop=True, inplace=True)
out = pd.concat((out, N4), axis=1) 

QTY = np.transpose(df['QTY'].head(3))
QTY.columns = ['Quantity Qualifier','Quantity','UOM']
QTY.reset_index(drop=True, inplace=True)
out = pd.concat((out, QTY), axis=1) 

LIN = np.transpose(df['LIN'].head(3))
LIN.columns = ['Assigned Identification','Product/Service ID Qualifier','Product/Service ID']
LIN.reset_index(drop=True, inplace=True)
out = pd.concat((out, LIN), axis=1)

UIT = np.transpose(df['UIT'].head(3))
UIT.columns = ['UOM','Unit Price','Basis of Unit Price Code']
UIT.reset_index(drop=True, inplace=True)
out = pd.concat((out, UIT), axis=1)

AMT = np.transpose(df['AMT'].head(2))
AMT.columns = ['Amount Qualifier Code','Monetary Amount']
AMT.reset_index(drop=True, inplace=True)
out = pd.concat((out, AMT), axis=1)

PID = np.transpose(df['PID'].head(5))
PID.columns = ['Item Description Type','','','','Description']
PID.reset_index(drop=True, inplace=True)
out = pd.concat((out, PID), axis=1)

out = pd.concat((out, pd.DataFrame([CTT], columns = ['Number of Line Items'])), axis=1)
out = pd.concat((out, pd.DataFrame([SE], columns = ['Number of Included Segments', \
                                                    'Transaction Set Control Number'])), axis=1)
out = pd.concat((out, pd.DataFrame([GE], columns = ['GE',''])), axis=1)
out = pd.concat((out, pd.DataFrame([IEA], columns = ['IEA',''])), axis=1)
                                     
# Output labelled csv
out.to_csv(infile.replace('.x12', '.csv'))



