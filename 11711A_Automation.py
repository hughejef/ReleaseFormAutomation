# -*- coding: utf-8 -*-
"""
Created on Thu Sep 14 11:03:32 2023

@author: jeff.hughes
"""

from pypdf import PdfReader, PdfWriter, PdfMerger
import pyodbc as db
import os


pooled_date = 'convert(date,getdate())'  # temp variable for calling procedure



# conn = db.connect(DRIVER = '{SQL Server}', SERVER = 'CMASQL', DATABASE = 'market', Trusted_Connections='yes', autocommit=True)
# data = pd.read_sql(query, conn)
# cursor = conn.cursor()

pool_list = []
file_list = []
warehouse_pool_dict = {"BankOfAmerica":[],  # dictionary of warehouse lenders and their respective pools on date
                       "WellsFargo":[], 
                       "RoyalBankOfCanada": [],
                       "JpMorgan": [],
                       "Ubs": [],
                       }

#get pools pooled on a date
def get_pools(pool_date):
    """
    takes a date as a parameter and appends the pool_list list with poolnums pooled to GNMA for that date
    """
    conn = db.connect(DRIVER = '{SQL Server}', SERVER = 'CMASQL', DATABASE = 'market', Trusted_Connections='yes', autocommit=True)
    cursor = conn.cursor()
    
    cursor.execute("select PoolNumTxt from EzPool Where PoolDt = convert(date,getdate()) and ProdCd in('G2F40S','G2F30S','G1F30S','G2F15S','G2F30HS','G2A05S') and Crawlsbit=0")
    rows = [item[0] for item in cursor.fetchall()]

    for row in rows:
        pool_list.append(row)
    
    
    conn.close()

 







#for loan in pool list if loan not cash, then add pool to warehouseline keyvalue in dict.. 
def get_warehouse_pools(pools):
    """
    take pool list and assigns poolnums to warehouses in the warehouse dictionary
    
    """
    conn = db.connect(DRIVER = '{SQL Server}', SERVER = 'CMASQL', DATABASE = 'market', Trusted_Connections='yes', autocommit=True)
    cursor = conn.cursor()
    
    
    for pool in pools:
        cursor.execute("select DISTINCT lw.WarehouseLineTypeName from Magnus.dbo.EzLoanWarehouse lw LEFT JOIN Magnus.dbo.EzLoanPool lp on lw.loanid = lp.loanid Where lp.PoolNumTxt = ? and lw.WarehouseLineTypeName not in('Cash','Unknown','Na') ",(pool,))
        warehouse_lines = [item[0] for item in cursor.fetchall()]
        for warehouse in warehouse_lines:
            warehouse_pool_dict[warehouse].append(pool)
    conn.close()


def generate_11711As():
    """
    take dictionary of pools on a warehouse line and generate 11711A pdfs for each lender
    """
    
    #create single files for each 11711A
    for warehouse in warehouse_pool_dict:
        i = 0
        file_list = []
        while len(warehouse_pool_dict[warehouse]) > i :    
            
            reader = PdfReader(f'11711A {warehouse} {i}.pdf')
            writer = PdfWriter()
            
            page = reader.pages[0]
            fields = reader.get_fields()
            
            writer.append(reader)
            
            writer.update_page_form_field_values(
                writer.pages[0], {f'PoolNum {i}': warehouse_pool_dict[warehouse][i]}
            )
            
            with open(f'11711A_{warehouse} {i}.pdf', "wb") as output_stream:        
                writer.write(output_stream)
                file_list.append(f'11711A_{warehouse} {i}.pdf')
            writer.close() 
            i += 1
            
        merger = PdfMerger()
        
        #merge the single files
        for file in file_list:
            merger.append(file)
            
        merger.write(f'11711A_{warehouse}_All.pdf')
        merger.close()
        
        #delete the single files
        for file in file_list:
            os.remove(file)
        print(f'11711A for {warehouse} completed...')
    print('All 11711As completed successfully!')

get_pools(pooled_date)

get_warehouse_pools(pool_list)

generate_11711As()


