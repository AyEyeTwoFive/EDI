# -*- coding: utf-8 -*-
"""
Created on Tue Sep 29 22:29:49 2020
Convert x12-formatted EDI data to csv
@author: asef islam, aislam@qralgroup.com
"""

import numpy as np
import pandas as pd
import sys
import time

t0 = time.time()

# Get input file name from command line and read it 
infile = sys.argv[1]
f = open(infile, "r")
data = f.read()
data = data.replace("~","")
lines = data.splitlines()  # split data by lines 
full = pd.DataFrame() # prepare outfile 
out = pd.DataFrame() # labelled csv output

for l in range(len(lines)): # split every line by *
    print("Processing item " + str(l+1) + " Of " + str(len(lines)))
    line = lines[l].split("*")
    full = pd.concat([full, pd.DataFrame(line)], axis=1)
    
    if line[0] == 'ST':
        out = pd.concat((out, pd.DataFrame([line[1:3]], columns = \
                                           ['Transaction Set ID', 'Transaction Set Control Number'])), axis=0)
    elif line[0] == 'BPT':
        out = pd.concat((out, pd.DataFrame([line[1:5]], columns = ['Transaction Set Purpose Code', 'Reference ID',\
                        'Report Creation Date', 'Report Type Code'])), axis=0)
    elif line[0] == 'DTM':
        out = pd.concat((out, pd.DataFrame([line[1:3]], columns = ['Date/Time Qualifier',\
                                                                   'Date'])), axis=0)
    elif line[0] == 'N1':
        if len(line) == 5:
            out = pd.concat((out, pd.DataFrame([line[1:5]], columns = ['Entry Identifier Code', 'Name'\
                        , 'Identification Code Qualifier', 'Identification Code'])), axis=0)
        elif len(line) == 3:
            out = pd.concat((out, pd.DataFrame([line[1:5]], columns = ['Entry Identifier Code', 'Name'])), axis=0)
    elif line[0] == 'PTD':
        if len(line) == 6:
            out = pd.concat((out, pd.DataFrame([line[1:6]], columns =  ['Product Transfer Type Code',\
                     'PTD02','PTD03','Reference ID Qualifier', 'Reference ID'])), axis=0)
        elif len(line) == 2:
            out = pd.concat((out, pd.DataFrame([line[1:6]], columns =  ['Product Transfer Type Code'])), axis=0)
    elif line[0] == 'REF':
        out = pd.concat((out, pd.DataFrame([line[1:3]], columns = ['Reference ID Qualifier','Reference ID'])), axis=0)
    elif line[0] == 'N3':
        if len(line) == 3:
            out = pd.concat((out, pd.DataFrame([line[1:3]], columns = \
                                           ['Address Information1','Address Information2'])), axis=0)
        elif len(line) == 2:
            out = pd.concat((out, pd.DataFrame([line[1:3]], columns = ['Address Information'])), axis=0)
    elif line[0] == 'N4':
        if len(line) == 5:
            out = pd.concat((out, pd.DataFrame([line[1:5]], columns = \
                        ['City Name','State or Province Code','Postal Code', 'Country Code'])), axis=0)  
        elif len(line) == 4:
            out = pd.concat((out, pd.DataFrame([line[1:5]], columns = \
                        ['City Name','State or Province Code','Postal Code'])), axis=0)
    elif line[0] == 'QTY':
        out = pd.concat((out, pd.DataFrame([line[1:4]], columns = ['Quantity Qualifier','Quantity','UOM'])), axis=0)
    elif line[0] == 'LIN':
        out = pd.concat((out, pd.DataFrame([line[1:8]], columns =['Assigned Identification',\
                    'Product/Service ID Qualifier1','Product/Service ID1', 'Product/Service ID Qualifier2',\
            'Product/Service ID2', 'Product/Service ID Qualifier3','Product/Service ID3'])), axis=0)    
    elif line[0] == 'UIT':
        out = pd.concat((out, pd.DataFrame([line[1:4]], columns = ['UOM','Unit Price','Basis of Unit Price Code']))\
                        , axis=0)
    elif line[0] == 'AMT':
        out = pd.concat((out, pd.DataFrame([line[1:3]], columns =['Amount Qualifier Code','Monetary Amount']\
                                           )), axis=0)
    elif line[0] == 'PID':
        out = pd.concat((out, pd.DataFrame([line[1:6]], columns = ['Item Description Type','PID02','PID03',\
                        'PID04','Description'] )), axis=0)
    elif line[0] == 'CTT':
        out = pd.concat((out, pd.DataFrame([line[1]], columns = ['Number of Line Items'])), axis=0)
    elif line[0] == 'SE':
        out = pd.concat((out, pd.DataFrame([line[1:3]], columns = ['Number of Included Segments', \
                                                    'Transaction Set Control Number'])), axis=0)

outfile = infile[0:len(infile)-4]

# Output full parsed data
full.to_csv(outfile + '_full.csv', header=False)

# Output labelled csv
out.to_csv(outfile + '.csv', index=False)

t1 = time.time()

print("Finished processing. Elapsed time " + str(t1-t0) + " seconds."  )



