# -*- coding: utf-8 -*-
"""
Created on Tue Sep 29 22:29:49 2020
Convert x12-formatted EDI data to csv
@author: asef islam, aislam@qralgroup.com
"""

import pandas as pd
import sys
import time
import datetime

def process_edi(infile):
    t0 = time.time()
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
                                               ['Transaction_Set_ID', 'Transaction_Set_Control_Number'])), axis=1)
        elif line[0] == 'BPT':
            out = pd.concat((out, pd.DataFrame([line[1:5]], columns = ['Transaction_Set_Purpose_Code', 'Reference_ID',\
                            'Report_Creation_Date', 'Report_Type_Code'])), axis=1)
        elif line[0] == 'DTM':
            if line[1] == '090':
                out = pd.concat((out, pd.DataFrame([line[2]], columns = ['Report_Start_Date'])), axis=1)
            elif line[1] == '091':
                out = pd.concat((out, pd.DataFrame([line[2]], columns = ['Report_End_Date'])), axis=1)
            elif line[1] == '003':
                out = pd.concat((out, pd.DataFrame([line[2]], columns = ['Invoice_Date'])), axis=1)
        elif line[0] == 'N1':
            cols=pd.Series(out.columns)
            for dup in cols[cols.duplicated()].unique(): 
                cols[cols[cols == dup].index.values.tolist()] = [dup + '.' + str(i) if i != 0 else dup for i in range(sum(cols == dup))]
            out.columns=cols
            out1 = pd.concat((out1, out), axis=0)
            out = pd.DataFrame()
            if line[1] == 'DB':
                if len(line) == 5:
                    out = pd.concat((out, pd.DataFrame([line[1:5]], columns = ['Entity_Identifier_Code1','Name1'\
                            , 'Identification_Code_Qualifier1', 'Identification_Code1'])), axis=1)
                elif len(line) == 3:
                    out = pd.concat((out, pd.DataFrame([line[1:5]], columns = ['Entity_Identifier_Code1','Name1'])), axis=1) 
            if line[1] == 'MF':
                if len(line) == 5:
                    out = pd.concat((out, pd.DataFrame([line[1:5]], columns = ['Entity_Identifier_Code2','Name2'\
                            , 'Identification_Code_Qualifier2', 'Identification_Code2'])), axis=1)
                elif len(line) == 3:
                    out = pd.concat((out, pd.DataFrame([line[1:5]], columns = ['Entity_Identifier_Code2','Name2'])), axis=1)
            if line[1] == 'ST':
                if len(line) == 5:
                    out = pd.concat((out, pd.DataFrame([line[1:5]], columns = ['Entity_Identifier_Code3','Name2'\
                            , 'Identification_Code_Qualifier3', 'Identification_Code3'])), axis=1)
                elif len(line) == 3:
                    out = pd.concat((out, pd.DataFrame([line[1:5]], columns = ['Entity_Identifier_Code3','Name3'])), axis=1) 
        elif line[0] == 'PTD':
            cols=pd.Series(out.columns)
            for dup in cols[cols.duplicated()].unique(): 
                cols[cols[cols == dup].index.values.tolist()] = [dup + '.' + str(i) if i != 0 else dup for i in range(sum(cols == dup))]
            out.columns=cols
            out1 = pd.concat((out1, out), axis=0)
            out = pd.DataFrame()
            if len(line) == 6:
                out = pd.concat((out, pd.DataFrame([line[1:6]], columns =  ['Product_Transfer_Type_Code','','',\
                                                             'Reference_ID_Qualifier', 'Reference_ID'])), axis=1)
            elif len(line) == 2:
                out = pd.concat((out, pd.DataFrame([line[1:6]], columns =  ['Product_Transfer_Type_Code'])), axis=1)
        elif line[0] == 'REF':
            if line[1] == 'AEM':
                out = pd.concat((out, pd.DataFrame([line[1:3]], columns = ['Reference_ID_Qualifier1', 'Reference_ID1'])), axis=1)
            if line[1] == 'IN':
                out = pd.concat((out, pd.DataFrame([line[1:3]], columns = ['Reference_ID_Qualifier2', 'Reference_ID2'])), axis=1)
            if line[1] == 'QK':
                out = pd.concat((out, pd.DataFrame([line[1:3]], columns = ['Reference_ID_Qualifier3', 'Reference_ID3'])), axis=1)
        elif line[0] == 'SII':
            out = pd.concat((out, pd.DataFrame([line[1:8]], columns =['Product_Service_ID_Qualifier',\
                'Item_NDC_UPC_Number','Quantity', 'UOM',\
                'Basis_of_Unit_Price_Code', 'Unit_Price','Monetary_Amount'])), axis=1) 
        elif line[0] == 'N3':
            if len(line) == 3:
                out = pd.concat((out, pd.DataFrame([line[1:3]], columns = \
                                               ['Address_Information','Address_Information'])), axis=1)
            elif len(line) == 2:
                out = pd.concat((out, pd.DataFrame([line[1:3]], columns = ['Address_Information'])), axis=1)
        elif line[0] == 'N4':
            if len(line) == 5:
                out = pd.concat((out, pd.DataFrame([line[1:5]], columns = \
                            ['City_Name','State_Province_Code','Postal_Code', 'Country_Code'])), axis=1)  
            elif len(line) == 4:
                out = pd.concat((out, pd.DataFrame([line[1:5]], columns = \
                            ['City_Name','State_Province_Code','Postal_Code'])), axis=1)
        elif line[0] == 'QTY':
            cols=pd.Series(out.columns)
            for dup in cols[cols.duplicated()].unique(): 
                cols[cols[cols == dup].index.values.tolist()] = [dup + '.' + str(i) if i != 0 else dup for i in range(sum(cols == dup))]
            out.columns=cols
            out1 = pd.concat((out1, out), axis=0)
            out = pd.DataFrame()
            out = pd.concat((out, pd.DataFrame([line[1:4]], columns = ['Quantity_Qualifier','Quantity','UOM'])), axis=1)
        elif line[0] == 'LIN':
            if len(line) == 8:
                out = pd.concat((out, pd.DataFrame([line[1:8]], columns =['Assigned_Identification',\
                        'Product_Service_ID_Qualifier1','Product_Service_ID1', 'Product_Service_ID_Qualifier2',\
                'Product_Service_ID2', 'Product_Service_ID_Qualifier3','Product_Service_ID3'])), axis=1)    
            elif len(line) == 6:
                out = pd.concat((out, pd.DataFrame([line[1:8]], columns =['Assigned_Identification',\
                        'Product_Service_ID_Qualifier1','Product_Service_ID1', 'Product_Service_ID_Qualifier2',\
                            'Product_Service_ID2'])), axis=1)    
        elif line[0] == 'UIT':
            out = pd.concat((out, pd.DataFrame([line[1:4]], columns = ['UOM','Unit_Price','Basis_of_Unit_Price_Code']))\
                            , axis=1)
        elif line[0] == 'AMT':
            out = pd.concat((out, pd.DataFrame([line[1:3]], columns =['Amount_Qualifier_Code','Monetary_Amount']\
                                               )), axis=1)
        elif line[0] == 'PID':
            out = pd.concat((out, pd.DataFrame([line[1:6]], columns = ['Item_Description_Type','','','','Description'] \
                                               )), axis=1)
        elif line[0] == 'CTT':
            out = pd.concat((out, pd.DataFrame([line[1]], columns = ['Number_of_Line_Items'])), axis=1)
        elif line[0] == 'SE':
            out = pd.concat((out, pd.DataFrame([line[1:3]], columns = ['Number_of_Included_Segments', \
                                                        'Transaction_Set_Control_Number'])), axis=1)
            cols=pd.Series(out.columns)
            for dup in cols[cols.duplicated()].unique(): 
                cols[cols[cols == dup].index.values.tolist()] = [dup + '.' + str(i) if i != 0 else dup for i in range(sum(cols == dup))]
            out.columns=cols
            out1 = pd.concat((out1, out), axis=0)
            out = pd.DataFrame()
        elif line[0] == 'XQ':
            if len(line) == 4:
                out = pd.concat((out, pd.DataFrame([line[1:4]], columns = ['Transaction_Handling_Code','Report_Start_Date',\
                                                                       'Report_End_Date'])), axis=1)
            elif len(line) == 3:
                out = pd.concat((out, pd.DataFrame([line[1:3]], columns = ['Transaction_Handling_Code','Report_Start_Date'])), axis=1)
            file852 = True # must be an 852 file
        elif line[0] == 'ZA':
            out = pd.concat((out, pd.DataFrame([line[1:4]], columns = ['Activity_Code','Quantity','UOM'])), axis=1)
            cols=pd.Series(out.columns)
            for dup in cols[cols.duplicated()].unique(): 
                cols[cols[cols == dup].index.values.tolist()] = [dup + '.' + str(i) if i != 0 else dup for i in range(sum(cols == dup))]
            out.columns=cols
            out1 = pd.concat((out1, out), axis=0)
            out = pd.DataFrame()
        elif line[0] == 'XPO':
            out = pd.concat((out, pd.DataFrame([line[1]], columns = ['Purchase_Order'])), axis=1)
        elif line[0] == 'N9':
            if line[1] == 'AEM':
                out = pd.concat((out, pd.DataFrame([line[2]], columns = ['Distribution_Center_Number'])), axis=1)
            elif line[1] == 'ACC':
                out = pd.concat((out, pd.DataFrame([line[2]], columns = ['Status'])), axis=1)
            elif line[1] == 'DI':
                out = pd.concat((out, pd.DataFrame([line[2:5]], columns = ['Invoice_Number','','Invoice_Date'])), axis=1)
    
    outfile = infile[0:infile.rfind("/")+1]
    
    if 'McKesson Plasma & Biologics LLC' in out1.values or 'MCKESSON PLASMA BIOLOGICS' in out1.values:
        outfile += 'McK-BIOLOGICS'
        
    if file852:
        outfile += '_852_'
    else:
        outfile += '_867_'
        
    outfile += lines[1].split('*')[4]
    
    # Output full parsed data
    full.to_csv(outfile + '_full.csv', header=False, float_format='%f')
    
    # Output labelled csv
    # out1 = out1.ffill()
    if file852:
        keep = out1['Activity_Code'].notna() # only keep rows with an activity code
    else:
        keep = out1['Quantity'].notna() # only keep rows with a quantity
    cols = out1.columns.intersection(['Number_of_Line_Items', 'Number_of_Included_Segments', 'Transaction_Set_Control_Number'])
    out1.loc[:,cols] = out1.loc[:,cols].bfill() # backfill closing data
    out1.loc[:,~out1.columns.isin(cols)] = out1.loc[:,~out1.columns.isin(cols)].ffill() #forward fill other data
    out1 = out1[keep]
    if 'Postal_Code' in out1:
        out1['Postal_Code'] = out1['Postal_Code'].astype(str).str[:5]
        
    out1.to_csv(outfile + '.csv', index=False, float_format='%f')
    t1 = time.time()
    print("Finished processing. Elapsed time " + str(t1-t0) + " seconds."  )
    
    print("Running QC checks on file")
    logs = QC_checks(out1, file852)
    if all(logs.results == 'Pass'):
        print("All QC checks passed")
    else:
        print("The following QC checks failed:")
        for l in range(len(logs)):
            if logs.results[l] == 'Fail':
                print(logs.tests[l])

def QC_checks(out1, file852):
    tests = []
    results = []
    
    if file852:
        tests.append("Transaction Set ID Should be 852")
        if all(out1['Transaction_Set_ID'] == '852'):
            results.append("Pass")
        else:
            results.append("Fail")
    else:
        tests.append("Transaction Set ID Should be 867")
        if all(out1['Transaction_Set_ID'] == '867'):
            results.append("Pass")
        else:
            results.append("Fail")
    
    if not file852:
        tests.append("Transaction Set Purpose Code Should be 00")
        if all(out1['Transaction_Set_Purpose_Code'] == '00'):
            results.append("Pass")
        else:
            results.append("Fail")
            
            
    tests.append("Report Start Date should be formatted as 'yyyymmdd'")
    temp = True
    for d in out1['Report_Start_Date']:
        try:
            datetime.datetime.strptime(d, '%Y%m%d')
        except ValueError:
            temp = False
    if temp:
        results.append("Pass")
    else:
        results.append("Fail")
        
    if not file852:
        tests.append("Report Type Code Should be SS")
        if all(out1['Report_Type_Code'] == 'SS'):
            results.append("Pass")
        else:
            results.append("Fail")        
        
        
    if file852:
        tests.append("Entity Identifier Code should always be 'ST'")
        if all(out1['Entity_Identifier_Code3']  == 'ST'):
            results.append("Pass")
        else:
            results.append("Fail")
    else:
        tests.append("Entity Identifier Code should be DB or MF")
        if all(c in ['DB','MF'] for c in out1['Entity_Identifier_Code1']) and all(c in ['DB','MF'] for c 
                                                                                  in out1['Entity_Identifier_Code2']):
            results.append("Pass")
        else:
            results.append("Fail") 
            
    if not file852:
        idquals = out1.columns.intersection(['Identification_Code_Qualifier1', 'Identification_Code_Qualifier2',
                                          'Identification_Code_Qualifier3'])
        tests.append("Identification Code Qualifier should be one of 11, 21, UL, 1, 92")
        for q in idquals:
            if any(c not in ['11','21','UL','1','92'] for c in out1[q]):
                results.append("Fail")
        else:
                results.append("Pass")        
    
    if 'State_Province_Code' in out1:
        tests.append("State recognized as a valid US state")
        states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA", 
              "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", 
              "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", 
              "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", 
              "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]
        if all(c in states for c in out1['State_Province_Code']):
            results.append("Pass")
        else:
            results.append("Fail")

    
    if file852:        
        tests.append("Product/Service ID2 should be one of 73380470001, 73380471509, 99999999999")
        if all(c in ['73380470001','73380471509','99999999999'] for c in out1['Product_Service_ID2']):
                results.append("Pass")
        else:
                results.append("Fail")
        
    if file852:
        tests.append("Activity Code Recognized")
        activity_codes =  ['QX','QS','QO','QA','QP','QR','QD','WQ','LS','QW','QZ','Q2','Q3','QH','QC','RE','QK','OP','QI','OF',
                           'PA','QN','PO','BS','MS','QE','QF','OQ', 'QM','QL','Q1','QT','TS']
        if all(c in activity_codes for c in out1['Activity_Code']):
                results.append("Pass")
        else:
                results.append("Fail")

    if not file852:
        tests.append("Quantity Qualifier should be 32 or 76")
        if all(c in ['32','76'] for c in out1['Quantity_Qualifier']):
            results.append("Pass")
        else:
            results.append("Fail")
                
               
    tests.append("Quantities should be non-negative")
    if all(pd.to_numeric(out1['Quantity']) >= 0):
            results.append("Pass")
    else:
            results.append("Fail")
            
    if not file852:
        tests.append("Unit Price should be numeric")
        if pd.to_numeric(out1['Unit_Price'], errors='coerce').notnull().all():
            results.append("Pass")
        else:
            results.append("Fail")
            
    if not file852:
        tests.append("Invoice Date should be formatted as 'yyyymmdd'")
        temp = True
        for d in out1['Invoice_Date']:
            try:
                datetime.datetime.strptime(d, '%Y%m%d')
            except ValueError:
                temp = False
        if temp:
            results.append("Pass")
        else:
            results.append("Fail")       
                
            
    return pd.DataFrame({'tests':tests, 'results':results})

def sendlogs(logs):
    print(logs)
    

# Get input file name from command line and read it 
if __name__ == '__main__':
    # executed as script
    infile = sys.argv[1]
    process_edi(infile=infile)
    

    
    
    
    
