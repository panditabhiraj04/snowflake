Create or replace procedure "PROC_PHCDW_EMAIL_NOTIFICATION"(FILE_GROUP_ID VARCHAR,EP_NAME varchar,JOB_NAME varchar,NOTIFICATION_TYPE VARCHAR,
ATTACHMENT_FLAG varchar,BATCH_ID VARCHAR,EMAIL_SUBJECT VARCHAR,EMAIL_BODY VARCHAR,FILENAME ARRAY)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
   /********************************************************************************************                                                    
** Name   : PROC_PHCDW_EMAIL_NOTIFICATION()
** Desc   :This is a snowflake stored procedure which is responsible for loading the data from •	PHCDW_SYS_NOTIF_CONFIG  to PHCDW_SYS_NOTIF_LOG table. 
** Auth   : CTS 
** Date   :12/07/2021 (MM/DD/YYYYY)
** Input  : BATCH_ID - it's the batch id of the AWS Glue job
**          FILE_ID - It's the file Id 
**          FILE_GROUP_ID - It's a file group id
** Output : A JSON response in before format -
**                  [
**                  {
**                   "DESC": "SUCCESS",
**                   "STATUS": 0,
**                   "INSERTCOUNT": 100092,
**					 "UPDATECOUNT": 99	
**					 "DELETECOUNT": 0 -- TBD		
**                  }
**                  ]
***********************************************************************************************       
***********************************************************************************************       
** Convention :
**       “PROC_”[{SOURCESYSTEMCODE}]”_"{PRODUCTCODE}”_"{TYPEOFDATA}”_”{WKLY/DLY}_”{TARGETTABLE}”
**      -  The name should be starting with “PROC” 
**      -  followed by [{SOURCESYSTEMCODE}] like “PRM” , but it's optional, 
**      -  then {PRODUCTCODE} e.g, TKI
** 		-  {TYPEOFDATA} -  like SLS for sales
**      -  followed by frequency “WKLY”/” DLY”
**      –  {TARGETTABLE}- target table name 
**      -  Proc should be created in UPPER CASE
**      -  All variables inside stored procedures should be in snakecase (lower case & separated by “_”).
**      -  All local variables should start with lv_ and if global variables, they should start with gv_.
**
**
**********************************************************************************************
** Change History
**********************************************************************************************
** PR   Date               Author              Description 
** --   --------          -------   ------------------------------------
** 1    12/17/2021           CTS      In case if the sql resultset having zero record its not creating the file so made few Changes to take the List 
                                       of files that got actually created inside the stage and add the if condition to take the list of s3 folder.
**
** 2    03/10/2022           CTS      CHANGES MADE TO IMPLEMENT JOB LEVEL SUCCESS/FAILURE MAIL, CHANGED FEW ARGUMENTS LIST(FILE_GROUP_ID VARCHAR,EP_NAME
**                                     varchar,JOB_NAME varchar,NOTIFICATION_TYPE VARCHAR, ATTACHMENT_FLG varchar,BATCH_ID VARCHAR,EMAIL_SUBJECT 
**                                     VARCHAR,EMAIL_BODY VARCHAR,FILENAME ARRAY)
***********************************************************************************************/
 



/*------------------------------------GETTING REQUIRED PARAMETERS FROM NOTIFICAATION CONFIG TABLE STARTS----------------------*/
//
//var lv_ep_nm = EP_NAME;
//var lv_job_nm = JOB_NAME;

//email parameters Values taken from NOTIFICATION_CONFIG TABLE
var message = '';
var subject = '';
var bcc = '';
var cc = '';
var notification_config_id = 0;
var notification_type = '';


//file format name Value taken from NOTIFICATION_CONFIG TABLE
var file_format  = '';

 //external storage area name Value taken from NOTIFICATION_CONFIG TABLE
var stage_name = '';

//file name of the attachment Values taken from NOTIFICATION_CONFIG TABLE

var file_name = '';
var storageIntegration = '';   // if procedure has context value it will passed in procedure signatue
var query_data = '';
var url = '';

//result storage
var mail_status = '';

//schema name 
var layer = '';

//common_folder
var common_folder = '';

var data = FILENAME;
var file_name_list = '';

//STORED PROCEDURE NAME
var lv_sp_name = 'PROC_PHCDW_EMAIL_NOTIFICATION';

//converting a file name array to string 
var count =0;

if(data != null )
{
  for(i=0 ; i<data.length-1; i++)
  {
   file_name_list= file_name_list+"'"+data[i]+"',";
  
  }
  file_name_list = file_name_list +"'"+ data[data.length-1]+"'";
}
else
{
  file_name_list = null;
}


//fetching data from the notification_config
var lv_mail_stmt = `select * from PHCDW_SYS_COMMON.PHCDW_SYS_NOTIFICATION_CONFIG where ep_name = '${EP_NAME}' and Job_name = '${JOB_NAME}' 
                     and attachment_flag = '${ATTACHMENT_FLAG}' and file_name in (${file_name_list}) and notification_type = '${NOTIFICATION_TYPE}';`;

var lv_mail_rs = snowflake.execute({sqlText: lv_mail_stmt})
/*------------------------------------GETTING REQUIRED PARAMETERS FROM NOTIFICAATION CONFIG TABLE ENDS----------------------*/

/*----------------------------------------------------MAIN PROCEDURE STARTS HERE--------------------------------------------*/
 /*-----------------------------------------Email notification Starts ---------------------------------------------------*/
      
      var s3_path ='';   
      if(ATTACHMENT_FLAG == 'Y')
      {   
      
      /*------------------------------iterating to fetch each record starts-------------------------------------------------*/
         while(lv_mail_rs.next())
            {
                         count = count+1;
                         
			             notification_config_id = lv_mail_rs.getColumnValue(1);
						 notification_type  = lv_mail_rs.getColumnValue(5);
                         message      = lv_mail_rs.getColumnValue(6);
                         subject      = lv_mail_rs.getColumnValue(7);
						 email_from   = lv_mail_rs.getColumnValue(8);
						 email_to     = lv_mail_rs.getColumnValue(9);
                         bcc          = lv_mail_rs.getColumnValue(10);
                         cc           = lv_mail_rs.getColumnValue(11);
                         file_format  = lv_mail_rs.getColumnValue(13);
                         stage_name   = lv_mail_rs.getColumnValue(14);
                         file_name    = lv_mail_rs.getColumnValue(15);
                         storageIntegration = lv_mail_rs.getColumnValue(16);
                         query_data   = lv_mail_rs.getColumnValue(17);
                         url          = lv_mail_rs.getColumnValue(18);
                         
                                            
                      if(!( EMAIL_BODY == 'NULL' || EMAIL_BODY == '' || EMAIL_BODY == null))
                      {
                        message = EMAIL_BODY;
                      }
					  
					   if(!( EMAIL_SUBJECT == 'NULL' || EMAIL_SUBJECT == '' || EMAIL_SUBJECT == null))
                      {
                        subject = EMAIL_SUBJECT;
                      }      
     
                      
                    /*-------------------------------------------- creating file_format starts-----------------------------------------------*/

                     var lv_file_frmt_stmt =`CREATE  FILE FORMAT IF NOT EXISTS PHCDW_SYS_COMMON.${file_format}
                     TYPE = 'CSV'
                     SKIP_HEADER = 0
                     COMPRESSION = NONE
                     FILE_EXTENSION  = '.csv'
                     FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
                     NULL_IF = ('\\N', '');`;

                     var lv_file_frmt_stmt_rs = snowflake.execute({sqlText: lv_file_frmt_stmt});
                    
                    /*-------------------------------------------- creating file_format ends-----------------------------------------------*/

                    /*-------------------------------------------- creating stage_area starts-----------------------------------------------*/

                     
                     var full_path = `${url}/${EP_NAME}/${JOB_NAME}`;

                     //common_folder = full_path
                     var curr_stage = stage_name;

                     var lv_crt_stg_stmt = `CREATE STAGE if not exists PHCDW_SYS_COMMON.${curr_stage}
                     URL = ${full_path}
                     FILE_FORMAT = ${file_format}
                     STORAGE_INTEGRATION = ${storageIntegration};`;

                     var lv_crt_stg_rs = snowflake.execute({sqlText: lv_crt_stg_stmt});
                     
                    /*-------------------------------------------- creating stage_area ends-----------------------------------------------*/

                  /*---------------------------------storing result set in csv file----------------------------------------------*/

                     var lv_copy_stmt=`COPY INTO @${curr_stage}/${file_name} FROM (${query_data})
                     FILE_FORMAT = (FORMAT_NAME = ${file_format})
                     max_file_size=5368709120
                     OVERWRITE = TRUE
                     SINGLE = TRUE;`;

                     var lv_copy_stmt_rs = snowflake.execute({sqlText: lv_copy_stmt,binds :[stage_name,file_name]});
                     
                
                  /*---------------------------------storing result set in csv file----------------------------------------------*/
                  
                  /*------------------------------------FETCHING THE CREATE S3 PATH BASED ON THE FILE NAME FROM STAGE -------------------------*/
                      var dummy ='';
                      var lv_s3_path_rs = snowflake.execute({sqlText: `List @PHCDW_SYS_COMMON.${curr_stage};`});
                           while(lv_s3_path_rs.next())
                           {
                              var stage_path = lv_s3_path_rs.getColumnValue(1);
                              var file_path  = full_path+"/"+file_name;
                              dummy = dummy + stage_path +"-"+file_path +'^^;';
                              if(stage_path == file_path)
                              { 
                                 s3_path =s3_path+'^^'+file_path;
                              }
                           } 
                           
                   /*------------------------------------FETCHING THE CREATE S3 PATH BASED ON THE FILE NAME FROM ENDS -------------------------*/
                  
                           
               /*------------------------------iterating to fetch each record ends-------------------------------------------------*/
             
               }  
         if(count>0){
                       mail_status = 'successful';
                       }
         
       }
      else if(ATTACHMENT_FLAG == 'N')
      {
       s3_path ='';
       while(lv_mail_rs.next())
            {
               count = count+1;
               
               notification_config_id = lv_mail_rs.getColumnValue(1);
               notification_type  = lv_mail_rs.getColumnValue(5);
               message      = lv_mail_rs.getColumnValue(6);
               subject      = lv_mail_rs.getColumnValue(7);
               email_from   = lv_mail_rs.getColumnValue(8);
               email_to     = lv_mail_rs.getColumnValue(9);
               bcc          = lv_mail_rs.getColumnValue(10);
               cc           = lv_mail_rs.getColumnValue(11);
               file_format  = lv_mail_rs.getColumnValue(13);
               stage_name   = lv_mail_rs.getColumnValue(14);
               file_name    = lv_mail_rs.getColumnValue(15);
               storageIntegration = lv_mail_rs.getColumnValue(16);
               query_data   = lv_mail_rs.getColumnValue(17);
               url          = lv_mail_rs.getColumnValue(18);

               }
               if(!( EMAIL_BODY == 'NULL' || EMAIL_BODY == '' || EMAIL_BODY == null))
                      {
                        message = EMAIL_BODY;
                      }
                if(!( EMAIL_SUBJECT == 'NULL' || EMAIL_SUBJECT == '' || EMAIL_SUBJECT == null))
                      {
                        subject = EMAIL_SUBJECT;
                      }      
         
           if(count>0){
                       mail_status = 'successful';
                       }
        }
              
          //populating the email info in PROC_NOTIFICATION table
        if(count>0)  
       { 
         var lv_mailCond_insert = `INSERT INTO PHCDW_SYS_COMMON.PHCDW_SYS_NOTIFICATION_LOG(NOTIFICATION_CONFIG_ID,
									BATCH_ID,
									EMAIL_SUBJECT,
									EMAIL_BODY,
									S3_PATH,
									EMAIL_STATUS,
									CRDT_DT,
									MDFD_DT)values (:1,:2,:3,:4,TRIM(:5,'^^'),'Y',current_timestamp,current_timestamp) ;`;
         
         var lv_mail_rs = snowflake.execute({sqlText: lv_mailCond_insert, binds: [notification_config_id,BATCH_ID,subject,message,s3_path]});
         lv_mail_rs.next();
         var lv_result=lv_mail_rs.getNumRowsAffected(); 
        }
        
    /*-----------------------------------------Email notification ENDS ---------------------------------------------------*/




/*----------------------------------------------------MAIN PROCEDURE ENDS--------------------------------------------*/


return mail_status;
$$;