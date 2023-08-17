from snowflake.snowpark.session import Session
import snowflake.snowpark.functions as F
import snowflake.snowpark.types as T
from snowflake.snowpark.window import Window
import streamlit as st
import time

import sys
import getpass
import pandas as pd
import numpy as np

with st.form("my_form"):
    tgt_ddl=st.text_area('Target DDL')
    st.text_input("SRC_DUPLICATE", key="src_duplicate")
    st.text_input("PRIMARY_KEYS", key="primary_keys")
    st.text_input("SURROGATE_KEY", key="surrogate_key")
    st.text_input("CDC START DATE", key="cdc_start_date")
    st.text_input("CDC END DATE", key="cdc_end_date")
    st.text_input("LOAD_DATE", key="load_date")
    st.text_input("UPDATE_DATE", key="update_date")
    st.text_input("STAGE_DB", key="stg_db")
    #st.text_input("TARGET_DB", key="tgt_db")
    st.text_input("SCD_TYPE", key="scd_type")
    st.text_input("SNOWFLAKE_ACCOUNT", key="sf_account")
    st.text_input("SNOWFLAKE_USER", key="sf_user")
    st.text_input("SNOWFLAKE_PASSWORD", key="sf_password")
    #st.text_input("SNOWFLAKE_WAREHOUSE", key="sf_warehouse")
    st.text_input("SNOWFLAKE_DB", key="sf_db")
    st.text_input("SNOWFLAKE_SCHEMA", key="sf_schema")
    #st.text_input("USER_ROLE", key="sf_role")
    
    
    submitted = st.form_submit_button("Submit")
    
    
    if submitted:
       #st.write(txt.strip())
       latest_iteration = st.empty()
       bar = st.progress(0)

       for i in range(100):
          # Update the progress bar with each iteration.
         #latest_iteration.text(f'Iteration {i+1}')
         bar.progress(i + 1)
         time.sleep(0.01)

       tgt_ddl=tgt_ddl.upper()
       src_duplicate=st.session_state.src_duplicate
       surrogate_key=st.session_state.surrogate_key.upper()
       primary_keys=st.session_state.primary_keys.upper()
       cdc_start_date=st.session_state.cdc_start_date.upper()
       cdc_end_date=st.session_state.cdc_end_date.upper()
       load_date=st.session_state.load_date.upper()
       update_date=st.session_state.update_date.upper()
       stg_db=st.session_state.stg_db.upper()
       #tgt_db=st.session_state.tgt_db.upper()
       scd_type=st.session_state.scd_type
       sf_account=st.session_state.sf_account
       sf_user=st.session_state.sf_user
       sf_password=st.session_state.sf_password
       sf_db=st.session_state.sf_db.upper()
       sf_schema=st.session_state.sf_schema.upper()
       #sf_role=st.session_state.sf_role
       #sf_warehouse=st.session_state.sf_warehouse
       #choose SCD TYPE - 1 or 2

       connection_parameters = {
           "account": sf_account,#'lb56092.central-india.azure',
           "user": sf_user,#'nivedha',
           "password": sf_password,#'Snowflake123',
           #"role": "ACCOUNTADMIN",
           "database": sf_db,#"ANALYTICS",
           "schema": sf_schema,#"PUBLIC",
           #"warehouse": "COMPUTE_WH"
       }

       session = Session.builder.configs(connection_parameters).create()
              
      
       
       import re

       stg_cols=''
       tgt_cols=''
       tgt_db=sf_db+"."+sf_schema
       #st.write(tgt_cols)
       cols=tgt_ddl.splitlines()
       #st.code(cols)
       for c in cols:
        c1=c
        if(c.startswith('CREATE OR REPLACE TABLE') or c.startswith('CREATE TABLE') or c.startswith('(') or c.startswith(')') or c.strip().startswith(surrogate_key)) and surrogate_key not in ('','null'):
            c=''
        c=c+"\n"
        stg_cols+=c
        if(c1.startswith('CREATE OR REPLACE TABLE') or c1.startswith('CREATE TABLE') or c1.startswith('(') or c1.startswith(')')):
            c1=''
        c1=c1+"\n"
        tgt_cols+=c1
       if surrogate_key in ('','null'):
        stg_cols=tgt_cols
       stg_cols=stg_cols.strip()
       
       tgt_cols=tgt_cols.strip()
       
       if tgt_ddl.splitlines()[0].endswith("("):
          tgt_tbl=tgt_ddl.splitlines()[0].replace("(","").strip()
       else:
          tgt_tbl=tgt_ddl.splitlines()[0].strip()
       tgt_tbl=tgt_tbl.replace("CREATE OR REPLACE TABLE ","").replace("CREATE TABLE ","")

       tgt_tbl_ws=tgt_db+"."+tgt_tbl
       stg_ddl="CREATE OR REPLACE TABLE "+stg_db+"."+tgt_tbl+"_STG\n("+stg_cols+");\n"

       stg_tbl_ws= stg_db+"."+tgt_tbl+"_STG"
       cdc_ddl="CREATE OR REPLACE TABLE "+stg_db+"."+tgt_tbl+"_CDC\n("+tgt_cols+");\n"
       cdc_tbl_ws= stg_db+"."+tgt_tbl+"_CDC"
       sk_ddl="CREATE OR REPLACE TABLE "+stg_db+"."+tgt_tbl+"_SK\n("+tgt_cols+");\n"
       sk_tbl_ws= stg_db+"."+tgt_tbl+"_SK"


       tgt_ddl= "CREATE OR REPLACE TABLE "+tgt_db+"."+tgt_tbl+"\n("+tgt_cols+");"

      
      
       session.sql(tgt_ddl).collect()


       ###################### FOR THE FIRST RUN COMMENT THE BELOW SCRIPTS #############################
       

       df=session.read.table(tgt_db+"."+tgt_tbl)
       if scd_type=='2':
           df3=session.sql("select "+cdc_end_date+" from "+tgt_db+"."+tgt_tbl)
           lef='9999-12-31'
           date_var="DAY"
           if  re.search("Timestamp",str(df3.schema)):
               lef='9999-12-31 23:59:59.000'
               date_var="SECOND"
       col_list=df.columns
       audit_cols=cdc_start_date+","+cdc_end_date+","+load_date+","+update_date
       ac_list=audit_cols.split(',')
       #ac_list
       pk_list=primary_keys.split(',')
       sk_list=surrogate_key.split(',')
       ac_pk_list=ac_list+pk_list+sk_list
       ac_pk_list=[i for i in ac_pk_list if i]
       other_col_list = [i for i in col_list if i not in ac_pk_list]
       col_list_final=[i for i in col_list if i not in sk_list]
       type1_col_list=[i for i in col_list if i not in pk_list+sk_list]
       
       on_cndn= ["TRIM(STG."+i+") = TRIM(TGT."+i+")" for i in pk_list]
       on_cndn_sk= ["TRIM(SK."+i+") = TRIM(TGT."+i+")" for i in pk_list]

       print (on_cndn)
       if src_duplicate=='1':
          stg_qry="""\nFROM (SELECT * FROM """+stg_tbl_ws+""" QUALIFY ROW_NUMBER() OVER(PARTITION BY """+",".join(pk_list)+" ORDER BY """+cdc_start_date+" DESC) STG"
       else:
          stg_qry="""\nFROM """+stg_tbl_ws+""" STG"""\
 
       get_query = """select column_name,data_type
       from information_schema.columns
       where table_catalog = '"""+tgt_db.split(".")[0]+"""'
       and table_schema = '"""+tgt_db.split(".")[1]+"""'
       and table_name = '"""+tgt_tbl+"""' order by ordinal_position"""
 
       df4=session.sql(get_query)
       df5 = df4.collect()

       where_cndn = ''
       for one_file in df5:
           colname = one_file.as_dict()['COLUMN_NAME']
           coltype = one_file.as_dict()['DATA_TYPE']
           for j in other_col_list:
               if str(j).strip()==str(colname).strip() and coltype.strip()=='TEXT':
                   where_cndn+= "OR COALESCE(STG."+j+",'') <> COALESCE(TGT."+j+",'')\n"
               elif str(j).strip()==str(colname).strip() and coltype.strip() in ('TIMESTAMP_NTZ'):
                   where_cndn+= "OR COALESCE(STG."+j+",'1900-01-01 00:00:00.000') <> COALESCE(TGT."+j+",'1900-01-01 00:00:00.000')\n"
               elif str(j).strip()==str(colname).strip() and coltype.strip() in ('DATE'):
                   where_cndn+= "OR COALESCE(STG."+j+",'1900-01-01') <> COALESCE(TGT."+j+",'1900-01-01')\n"
               elif str(j).strip()==str(colname).strip() and coltype.strip() not in ('TIMESTAMP_NTZ','DATE','TEXT'):
                   where_cndn+= "OR COALESCE(STG."+j+",'0') <> COALESCE(TGT."+j+",'0')\n"
       where_cndn=where_cndn[3:]

       type1_update = ''
       type1_update_sk = ''
       for j in type1_col_list:
          if str(j).strip()==load_date:
             continue
          else:
             type1_update+= "\tTGT."+j+" = STG."+j+",\n"
             type1_update_sk+= "\tTGT."+j+" = SK."+j+",\n"
       type1_update=type1_update[:-2]
       type1_update_sk=type1_update_sk[:-2]

       if scd_type=='1':
          if surrogate_key in ('','null'):
             cdc=''
             sk=''
             merge="MERGE INTO "+tgt_tbl_ws+" TGT USING \n"+sk_tbl_ws+ " SK \nON "+" \nAND ".join(on_cndn_sk)+"\nWHEN MATCHED THEN UPDATE SET \n"+type1_update_sk+"\nWHEN NOT MATCHED THEN\nINSERT("+",\n\t".join(col_list)+")\nVALUES(\n\tSK."+",\n\tSK.".join(col_list)+");"
          else:
             cdc="INSERT OVERWRITE INTO "+cdc_tbl_ws+"(\nSELECT \nCOALESCE(TGT."+surrogate_key+",0) AS "+surrogate_key+',\nSTG.'+',\nSTG.'.join(col_list_final)+stg_qry+"\nLEFT JOIN "+tgt_tbl_ws+ " TGT \nON "+ " \nAND ".join(on_cndn)+"\nWHERE """+"".join(where_cndn)+");"

             sk="INSERT OVERWRITE INTO "+sk_tbl_ws+"\nSELECT CASE WHEN "+surrogate_key+" = 0 \n\tTHEN ROW_NUMBER() OVER (ORDER BY CDC."+','.join(pk_list)+" + COALESCE(MAX_"+surrogate_key+",0))\n\tELSE CDC."+surrogate_key+" END AS "+surrogate_key+",\nCDC."+',\nCDC.'.join(col_list_final)+ "\nFROM "+ cdc_tbl_ws + " CDC \nCROSS JOIN (SELECT MAX("""+surrogate_key +") AS MAX_"+surrogate_key+" FROM "+tgt_tbl_ws+") TGT_MAX;"

             merge="MERGE INTO "+tgt_tbl_ws+" TGT USING \n"+sk_tbl_ws+" SK \nON TGT."+surrogate_key+" = SK."+surrogate_key+"\nWHEN MATCHED THEN\nUPDATE SET \n"+type1_update_sk+"\nWHEN NOT MATCHED THEN \nINSERT("+ ",\n\t".join(col_list)+")\nVALUES(\n\tSK."+",\n\tSK.".join(col_list)+");"

       elif scd_type=='2':
          if surrogate_key in ('','null'):
             cdc=''
             sk=''
             tgt_update="UPDATE "+tgt_tbl_ws+" TGT SET TGT."+cdc_end_date+" = DATEADD("+date_var+", - 1, STG."+cdc_start_date+"),\n       TGT."+update_date+" = STG."+update_date+"\nFROM (SELECT STG.* FROM "+stg_tbl_ws+" STG INNER JOIN "+tgt_tbl_ws+ " TGT ON "+" \nAND ".join(on_cndn)+ " WHERE TGT."+cdc_end_date+"='"+lef+"') STG\nWHERE "+" \nAND ".join(on_cndn)+ " AND TGT."+cdc_end_date+"='"+lef+"';"
             tgt_insert="INSERT INTO "+tgt_tbl_ws+"( \nSELECT DISTINCT STG."+',\nSTG.'.join(col_list_final)+" FROM "+stg_tbl_ws+" STG);"
             merge=tgt_update +"\n\n"+tgt_insert

          else:
             cdc="INSERT OVERWRITE INTO "+cdc_tbl_ws+"(\nSELECT \nCOALESCE(TGT."+surrogate_key+",0) AS "+surrogate_key+',\nSTG.'+',\nSTG.'.join(col_list_final)+stg_qry+"\nLEFT JOIN (SELECT * FROM "+tgt_tbl_ws+ " WHERE "+cdc_end_date+"='"+lef+"') TGT \nON "+ " \nAND ".join(on_cndn)+"\nWHERE "+"".join(where_cndn)+");"
             #print(cdc) 
             sk="INSERT OVERWRITE INTO "+sk_tbl_ws+"\nSELECT CASE WHEN "+surrogate_key+" = 0 \n\tTHEN ROW_NUMBER() OVER (ORDER BY CDC."+','.join(pk_list)+" + COALESCE(MAX_"+surrogate_key+",0)) \n\tELSE CDC."+surrogate_key+" END AS "+surrogate_key+",\nCDC."+',\nCDC.'.join(col_list_final)+ "\nFROM "+ cdc_tbl_ws + " CDC \nCROSS JOIN (SELECT MAX("+surrogate_key +") AS MAX_"+surrogate_key+" FROM "+tgt_tbl_ws+") TGT_MAX;"
             #print(sk)
             merge=merge="MERGE INTO "+tgt_tbl_ws+" TGT USING \n(\nSELECT SK1."+surrogate_key+" AS MERGE_KEY, SK1.* FROM "+sk_tbl_ws+" SK1 \nUNION \nSELECT NULL AS MERGE_KEY, SK2.* FROM """+sk_tbl_ws+" SK2 )SK ON TGT."+surrogate_key+" = SK.MERGE_KEY \nWHEN MATCHED AND TGT."+cdc_end_date+" = '"+lef+"' THEN \nUPDATE SET \n"+cdc_end_date+" = DATEADD("+date_var+", - 1, SK."+cdc_start_date+"),\nTGT."+update_date+" = SK."+update_date+"\nWHEN NOT MATCHED AND MERGE_KEY IS NULL THEN \nINSERT("+ ",\n\t".join(col_list)+")\nVALUES(\n\tSK."+",\n\tSK.".join(col_list)+");"
             #print(merge)


       if surrogate_key in ('','null'):
        DDL_SCRIPT = tgt_ddl +" \n\n\n"+stg_ddl
       else:
        DDL_SCRIPT = tgt_ddl +" \n\n\n"+stg_ddl +" \n\n\n"+cdc_ddl+" \n\n\n"+sk_ddl+" \n\n\n"
       INCR_SCRIPT=cdc+"\n\n"+sk+"\n\n"+merge
       st.code(DDL_SCRIPT)
       st.code(INCR_SCRIPT)

    