# -*- coding: utf-8 -*-
"""
Created on Tue Sep 29 22:29:49 2020
Convert x12-formatted EDI data to csv
2.0: added QC checks
@author: asef islam, aislam@qralgroup.com
"""

import numpy as np
import pandas as pd
import sys
import time
import csv
import datetime

t0 = time.time()

# Get input file name from command line and read it 
infile = sys.argv[1]
f = open(infile, "r")
data = f.read()
if "~" in data:
    data = data.replace("~","\n")
if "|" in data:
    data = data.replace("|","*")    
lines = data.splitlines()  # split data by lines 
full = pd.DataFrame() # prepare outfile 
out = pd.DataFrame() # labelled csv output
out1 = pd.DataFrame() # nested labelled csv output
file852 = False

for l in range(len(lines)): # split every line by *
    print("Processing item " + str(l+1) + " Of " + str(len(lines)))
    line = lines[l].split("*")
    full = pd.concat([full, pd.DataFrame(line)], axis=1)
    
    if line[0] == 'ST':
        out1 = pd.concat((out1, out), axis=0)
        out = pd.DataFrame()
        out = pd.concat((out, pd.DataFrame([line[1:3]], columns = \
                                           ['Transaction Set ID', 'Transaction Set Control Number'])), axis=1)
    elif line[0] == 'BPT':
        out = pd.concat((out, pd.DataFrame([line[1:5]], columns = ['Transaction Set Purpose Code', 'Reference ID',\
                        'Report Creation Date', 'Report Type Code'])), axis=1)
    elif line[0] == 'DTM':
        if line[1] == '090':
            out = pd.concat((out, pd.DataFrame([line[2]], columns = ['Report Start Date'])), axis=1)
        elif line[1] == '091':
            out = pd.concat((out, pd.DataFrame([line[2]], columns = ['Report End Date'])), axis=1)
        elif line[1] == '003':
            out = pd.concat((out, pd.DataFrame([line[2]], columns = ['Invoice Date'])), axis=1)
    elif line[0] == 'N1':
        cols=pd.Series(out.columns)
        for dup in cols[cols.duplicated()].unique(): 
            cols[cols[cols == dup].index.values.tolist()] = [dup + '.' + str(i) if i != 0 else dup for i in range(sum(cols == dup))]
        out.columns=cols
        out1 = pd.concat((out1, out), axis=0)
        out = pd.DataFrame()
        if len(line) == 5:
            out = pd.concat((out, pd.DataFrame([line[1:5]], columns = ['Entity Identifier Code','Name'\
                        , 'Identification Code Qualifier', 'Identification Code'])), axis=1)
        elif len(line) == 3:
            out = pd.concat((out, pd.DataFrame([line[1:5]], columns = ['Entity Identifier Code','Name'])), axis=1) 
    elif line[0] == 'PTD':
        cols=pd.Series(out.columns)
        for dup in cols[cols.duplicated()].unique(): 
            cols[cols[cols == dup].index.values.tolist()] = [dup + '.' + str(i) if i != 0 else dup for i in range(sum(cols == dup))]
        out.columns=cols
        out1 = pd.concat((out1, out), axis=0)
        out = pd.DataFrame()
        if len(line) == 6:
            out = pd.concat((out, pd.DataFrame([line[1:6]], columns =  ['Product Transfer Type Code','','',\
                                                         'Reference ID Qualifier', 'Reference ID'])), axis=1)
        elif len(line) == 2:
            out = pd.concat((out, pd.DataFrame([line[1:6]], columns =  ['Product Transfer Type Code'])), axis=1)
    elif line[0] == 'REF':
        out = pd.concat((out, pd.DataFrame([line[1:3]], columns = ['Reference ID Qualifier', 'Reference ID'])), axis=1)
    elif line[0] == 'SII':
        out = pd.concat((out, pd.DataFrame([line[1:8]], columns =['Product/Service ID Qualifier',\
            'Item NDC or UPC Number','Quantity', 'UOM',\
            'Basis of Unit Price Code', 'Unit Price','Monetary Amount'])), axis=1) 
    elif line[0] == 'N3':
        if len(line) == 3:
            out = pd.concat((out, pd.DataFrame([line[1:3]], columns = \
                                           ['Address Information','Address Information'])), axis=1)
        elif len(line) == 2:
            out = pd.concat((out, pd.DataFrame([line[1:3]], columns = ['Address Information'])), axis=1)
    elif line[0] == 'N4':
        if len(line) == 5:
            out = pd.concat((out, pd.DataFrame([line[1:5]], columns = \
                        ['City Name','State or Province Code','Postal Code', 'Country Code'])), axis=1)  
        elif len(line) == 4:
            out = pd.concat((out, pd.DataFrame([line[1:5]], columns = \
                        ['City Name','State or Province Code','Postal Code'])), axis=1)
    elif line[0] == 'QTY':
        cols=pd.Series(out.columns)
        for dup in cols[cols.duplicated()].unique(): 
            cols[cols[cols == dup].index.values.tolist()] = [dup + '.' + str(i) if i != 0 else dup for i in range(sum(cols == dup))]
        out.columns=cols
        out1 = pd.concat((out1, out), axis=0)
        out = pd.DataFrame()
        out = pd.concat((out, pd.DataFrame([line[1:4]], columns = ['Quantity Qualifier','Quantity','UOM'])), axis=1)
    elif line[0] == 'LIN':
        if len(line) == 8:
            out = pd.concat((out, pd.DataFrame([line[1:8]], columns =['Assigned Identification',\
                    'Product/Service ID Qualifier1','Product/Service ID1', 'Product/Service ID Qualifier2',\
            'Product/Service ID2', 'Product/Service ID Qualifier3','Product/Service ID3'])), axis=1)    
        elif len(line) == 6:
            out = pd.concat((out, pd.DataFrame([line[1:8]], columns =['Assigned Identification',\
                    'Product/Service ID Qualifier1','Product/Service ID1', 'Product/Service ID Qualifier2',\
                        'Product/Service ID2'])), axis=1)    
    elif line[0] == 'UIT':
        out = pd.concat((out, pd.DataFrame([line[1:4]], columns = ['UOM','Unit Price','Basis of Unit Price Code']))\
                        , axis=1)
    elif line[0] == 'AMT':
        out = pd.concat((out, pd.DataFrame([line[1:3]], columns =['Amount Qualifier Code','Monetary Amount']\
                                           )), axis=1)
    elif line[0] == 'PID':
        out = pd.concat((out, pd.DataFrame([line[1:6]], columns = ['Item Description Type','','','','Description'] \
                                           )), axis=1)
    elif line[0] == 'CTT':
        out = pd.concat((out, pd.DataFrame([line[1]], columns = ['Number of Line Items'])), axis=1)
    elif line[0] == 'SE':
        out = pd.concat((out, pd.DataFrame([line[1:3]], columns = ['Number of Included Segments', \
                                                    'Transaction Set Control Number'])), axis=1)
        cols=pd.Series(out.columns)
        for dup in cols[cols.duplicated()].unique(): 
            cols[cols[cols == dup].index.values.tolist()] = [dup + '.' + str(i) if i != 0 else dup for i in range(sum(cols == dup))]
        out.columns=cols
        out1 = pd.concat((out1, out), axis=0)
        out = pd.DataFrame()
    elif line[0] == 'XQ':
        if len(line) == 4:
            out = pd.concat((out, pd.DataFrame([line[1:4]], columns = ['Transaction Handling Code','Report Start Date',\
                                                                   'Report End Date'])), axis=1)
        elif len(line) == 3:
            out = pd.concat((out, pd.DataFrame([line[1:3]], columns = ['Transaction Handling Code','Report Start Date'])), axis=1)
        file852 = True # must be an 852 file
    elif line[0] == 'ZA':
        out = pd.concat((out, pd.DataFrame([line[1:4]], columns = ['Activity Code','Quantity','UOM'])), axis=1)
        cols=pd.Series(out.columns)
        for dup in cols[cols.duplicated()].unique(): 
            cols[cols[cols == dup].index.values.tolist()] = [dup + '.' + str(i) if i != 0 else dup for i in range(sum(cols == dup))]
        out.columns=cols
        out1 = pd.concat((out1, out), axis=0)
        out = pd.DataFrame()
    elif line[0] == 'XPO':
        out = pd.concat((out, pd.DataFrame([line[1]], columns = ['Purchase Order'])), axis=1)
    elif line[0] == 'N9':
        if line[1] == 'AEM':
            out = pd.concat((out, pd.DataFrame([line[2]], columns = ['Distribution Center Number'])), axis=1)
        elif line[1] == 'ACC':
            out = pd.concat((out, pd.DataFrame([line[2]], columns = ['Status'])), axis=1)
        elif line[1] == 'DI':
            out = pd.concat((out, pd.DataFrame([line[2:5]], columns = ['Invoice Number','','Invoice Date'])), axis=1)

outfile = infile[0:infile.rfind(".")]

# Output full parsed data
full.to_csv(outfile + '_full.csv', header=False, float_format='%f')

# Output labelled csv
# out1 = out1.ffill()
if file852:
    keep = out1['Activity Code'].notna() # only keep rows with an activity code
else:
    keep = out1['Quantity'].notna() # only keep rows with a quantity
cols = out1.columns.intersection(['Number of Line Items', 'Number of Included Segments', 'Transaction Set Control Number'])
out1.loc[:,cols] = out1.loc[:,cols].bfill() # backfill closing data
out1.loc[:,~out1.columns.isin(cols)] = out1.loc[:,~out1.columns.isin(cols)].ffill() #forward fill other data
out1 = out1[keep]
if 'Postal Code' in out1:
    out1['Postal Code'] = out1['Postal Code'].astype(str).str[:5]
    
out1.to_csv(outfile + '.csv', index=False, float_format='%f')
t1 = time.time()
print("Finished processing. Elapsed time " + str(t1-t0) + " seconds."  )

def QC_checks(out1, file852):
    tests = []
    results = []
    
    if file852:
        tests.append("Transaction Set ID Should be 852")
        if all(out1['Transaction Set ID'] == '852'):
            results.append("Pass")
        else:
            results.append("Fail")
            
    tests.append("Report Start Date should be formatted as 'yyyymmdd'")
    temp = True
    for d in out1['Report Start Date']:
        try:
            datetime.datetime.strptime(d, '%Y%m%d')
        except ValueError:
            temp = False
    if temp:
        results.append("Pass")
    else:
        results.append("Fail")
        
    if file852:
        tests.append("Entity Identifier Code should always be 'ST'")
        if all(out1['Entity Identifier Code']  == 'ST'):
            results.append("Pass")
        else:
            results.append("Fail")
            
    if file852:        
        tests.append("Product/Service ID2 should be one of 73380470001, 73380471509, 99999999999")
        if all(c in ['73380470001','73380471509','99999999999'] for c in out1['Entity Identifier Code']):
                results.append("Pass")
        else:
                results.append("Fail")
        
    if file852:
        tests.append("Activity Code Recognized")
        if all(c in ['QX','QS','QO','QA','QP','QR','QD','WQ','LS','QW','QZ'
                'Q2','Q3','QH','QC','RE','QK','OP','QI','OF','PA','QN','PO','BS','MS','QE','QF','OQ'
                'QM','QL','Q1','QT','TS'] for c in out1['Activity Code']):
                results.append("Pass")
        else:
                results.append("Fail")

    tests.append("Quantities should be non-negative")
    if all(pd.to_numeric(out1['Quantity']) >= 0):
            results.append("Pass")
    else:
            results.append("Fail")
            
    return pd.DataFrame({'tests':tests, 'results':results})
    
print("Running QC checks on file")
logs = QC_checks(out1, file852)
if all(logs.results == 'Pass'):
    print("All QC checks passed")
else:
    print("The following QC checks failed:")
    
    

    
    
    
    
