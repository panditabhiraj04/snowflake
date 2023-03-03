CREATE OR REPLACE PROCEDURE "PROC_S3_FILE_UPLOAD"("EP_NAME" VARCHAR, "JOB_NAME" VARCHAR,"FILE_TYPE" VARCHAR,"FILE_EXTENSION" VARCHAR,"FILE_NAME" VARCHAR,"QUERY_DATA" VARCHAR)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS 
$$

   /*------------------------------------------GETTING THE OTHER PARAM VARIABLES------------------------------------------------------------*/
      var stmt = snowflake.createStatement({sqlText: 'CALL PHCDW_SYS_COMMON.PROC_PHCDW_SYS_VALUES(:1, :2)',binds: [EP_NAME, JOB_NAME]});
      var rs = stmt.execute();
      while(rs.next()){
      var lv_json_othr_param_val= rs.getColumnValue(1);
      }
      var file_format_name = lv_json_othr_param_val["file_format_name"]; 
      var url = lv_json_othr_param_val["url"]; 
      var stage_name = lv_json_othr_param_val["stage_name"]; 
      var storage_integration = lv_json_othr_param_val["storage_integration"];
       var mdmfullurl=   lv_json_othr_param_val["mdmfullurl"];                                   
      /*------------------------------------------GETTING THE OTHER PARAM VARIABLES ENDS-------------------------------------------------------*/

      var lv_file_frmt_stmt = `CREATE  FILE FORMAT IF NOT EXISTS phcdw_sys_common.${file_format_name}
                                TYPE = '${FILE_TYPE}'
                                COMPRESSION = NONE
                                FILE_EXTENSION='${FILE_EXTENSION}'
                                FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
                                NULL_IF = ('\\N', '');`;
     
      var lv_file_frmt_stmt_rs = snowflake.execute({sqlText: lv_file_frmt_stmt});
      
      var lv_crt_stg_stmt = `create stage if not exists phcdw_sys_common.${stage_name}
                             URL = '${url}/${EP_NAME}/${JOB_NAME}/'
                             STORAGE_INTEGRATION = ${storage_integration};`;
                             
      var lv_crt_stg_rs = snowflake.execute({sqlText: lv_crt_stg_stmt});
      
      //remove old_files from stage with file name
      
      var lv_pattern = `\.*${FILE_NAME}\(_[0-9]*\)*${FILE_EXTENSION}$`;
      var removed_path = '';
      var lv_rm_file = `remove @phcdw_sys_common.${stage_name} PATTERN = '${lv_pattern}';`;
      var lv_crt_stg_rs = snowflake.execute({sqlText: lv_rm_file});
      while(lv_crt_stg_rs.next())
      {
        removed_path = removed_path+ lv_crt_stg_rs.getColumnValue(1);
      }
    
      
      var lv_copy_stmt=`COPY INTO @phcdw_sys_common.${stage_name}/${FILE_NAME} FROM (${QUERY_DATA})
                          FILE_FORMAT = (FORMAT_NAME = phcdw_sys_common.${file_format_name})
                          OVERWRITE = TRUE
                          //MAX_FILE_SIZE = 5368709120
                          SINGLE = FALSE
                          HEADER=true;`;
                          
      var lv_copy_stmt_rs = snowflake.execute({sqlText: lv_copy_stmt});
      
  /*--------------------------------------------FETCHING THE UPLOADED LINK FROM EXTERNAL STAGE-------------------------------------------*/      
      
      var stage_path =  '';
      
      var full_path = `${url}/${EP_NAME}/${JOB_NAME}/${FILE_NAME}`;
      
      var lv_s3_path_rs = snowflake.execute({sqlText: `List @PHCDW_SYS_COMMON.${stage_name};`});
      
           while(lv_s3_path_rs.next())
           { 
              var dummy = lv_s3_path_rs.getColumnValue(1) ;
               if( dummy == full_path)
                { 
                   stage_path = dummy;
                }

           } 
     /*--------------------------------------------FETCHING THE UPLOADED LINK FROM EXTERNAL STAGE-------------------------------------------*/      
        
 
     
  return stage_path;                        
$$;    