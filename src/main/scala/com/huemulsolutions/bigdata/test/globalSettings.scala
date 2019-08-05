package com.huemulsolutions.bigdata.test


import com.huemulsolutions.bigdata.common._

object globalSettings {
   val Global: huemul_GlobalPath  = new huemul_GlobalPath()
   Global.GlobalEnvironments = "production, experimental"
   
   //Global.CONTROL_Driver = "com.postgres.jdbc.Driver"
   Global.CONTROL_Driver = "oracle.jdbc.OracleDriver"
   //Global.CONTROL_Driver = "com.mysql.jdbc.Driver"
   //Global.CONTROL_Driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
   
   //Global.CONTROL_Setting.append(new huemul_KeyValuePath("production","jdbc:postgresql://bd-control-14.postgres.database.azure.com:5432/postgres?user=huemul@bd-control-14&password=dev.CODE123456&sslmode=require&currentSchema=public"))
   Global.CONTROL_Setting.append(new huemul_KeyValuePath("production","jdbc:oracle:thin:sys as sysdba/OraPasswd1@bd-oracle-20.cloudapp.net:1521:cdb1"))
   //Global.CONTROL_Setting.append(new huemul_KeyValuePath("production","jdbc:sqlserver://bd-server-sql-20.database.windows.net:1433;database=bd-control-sqlserver;user=control@bd-server-sql-20;password=dev.CODE123456;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"))
   //Global.CONTROL_Setting.append(new huemul_KeyValuePath("production","jdbc:mysql://mysql-control.mysql.database.azure.com:3306/control?useSSL=true&requireSSL=false&user=devcode@mysql-control&password=dev.CODE123456"))
 
   Global.ImpalaEnabled = false
   Global.IMPALA_Setting.append(new huemul_KeyValuePath("production","jdbc:postgresql://control-postgre.postgres.database.azure.com:5432/postgres?user=control@control-postgre&password=developer.CODE6471&currentSchema=public"))
   Global.IMPALA_Setting.append(new huemul_KeyValuePath("experimental","jdbc:postgresql://control-postgre.postgres.database.azure.com:5432/postgres?user=control@control-postgre&password=developer.CODE6471&currentSchema=public"))
   
   //TEMPORAL SETTING
   Global.TEMPORAL_Path.append(new huemul_KeyValuePath("production","hdfs:///user/data/production/temp/"))
   Global.TEMPORAL_Path.append(new huemul_KeyValuePath("experimental","hdfs:///user/data/experimental/temp/"))
     
   //RAW SETTING
   Global.RAW_SmallFiles_Path.append(new huemul_KeyValuePath("production","hdfs:///user/data/production/raw/"))
   Global.RAW_SmallFiles_Path.append(new huemul_KeyValuePath("experimental","hdfs:///user/data/experimental/raw/"))
   
   Global.RAW_BigFiles_Path.append(new huemul_KeyValuePath("production","hdfs:///user/data/production/raw/"))
   Global.RAW_BigFiles_Path.append(new huemul_KeyValuePath("experimental","hdfs:///user/data/experimental/raw/"))
   
   //BACKUP
   Global.MDM_Backup_Path.append(new huemul_KeyValuePath("production","hdfs:///user/data/production/backup/"))
   Global.MDM_Backup_Path.append(new huemul_KeyValuePath("experimental","hdfs:///user/data/experimental/backup/"))
   
   
   
   
   //MASTER SETTING
   Global.MASTER_DataBase.append(new huemul_KeyValuePath("production","production_master"))   
   Global.MASTER_DataBase.append(new huemul_KeyValuePath("experimental","experimental_master"))

   Global.MASTER_SmallFiles_Path.append(new huemul_KeyValuePath("production","hdfs:///user/data/production/master/"))
   Global.MASTER_SmallFiles_Path.append(new huemul_KeyValuePath("experimental","hdfs:///user/data/experimental/master/"))
   
   Global.MASTER_BigFiles_Path.append(new huemul_KeyValuePath("production","hdfs:///user/data/production/master/"))
   Global.MASTER_BigFiles_Path.append(new huemul_KeyValuePath("experimental","hdfs:///user/data/experimental/master/"))

   //DIM SETTING
   Global.DIM_DataBase.append(new huemul_KeyValuePath("production","production_dim"))   
   Global.DIM_DataBase.append(new huemul_KeyValuePath("experimental","experimental_dim"))

   Global.DIM_SmallFiles_Path.append(new huemul_KeyValuePath("production","hdfs:///user/data/production/dim/"))
   Global.DIM_SmallFiles_Path.append(new huemul_KeyValuePath("experimental","hdfs:///user/data/experimental/dim/"))
   
   Global.DIM_BigFiles_Path.append(new huemul_KeyValuePath("production","hdfs:///user/data/production/dim/"))
   Global.DIM_BigFiles_Path.append(new huemul_KeyValuePath("experimental","hdfs:///user/data/experimental/dim/"))

   //ANALYTICS SETTING
   Global.ANALYTICS_DataBase.append(new huemul_KeyValuePath("production","production_analytics"))   
   Global.ANALYTICS_DataBase.append(new huemul_KeyValuePath("experimental","experimental_analytics"))
   
   Global.ANALYTICS_SmallFiles_Path.append(new huemul_KeyValuePath("production","hdfs:///user/data/production/analytics/"))
   Global.ANALYTICS_SmallFiles_Path.append(new huemul_KeyValuePath("experimental","hdfs:///user/data/experimental/analytics/"))
   
   Global.ANALYTICS_BigFiles_Path.append(new huemul_KeyValuePath("production","hdfs:///user/data/production/analytics/"))
   Global.ANALYTICS_BigFiles_Path.append(new huemul_KeyValuePath("experimental","hdfs:///user/data/experimental/analytics/"))

   //REPORTING SETTING
   Global.REPORTING_DataBase.append(new huemul_KeyValuePath("production","production_reporting"))
   Global.REPORTING_DataBase.append(new huemul_KeyValuePath("experimental","experimental_reporting"))

   Global.REPORTING_SmallFiles_Path.append(new huemul_KeyValuePath("production","hdfs:///user/data/production/reporting/"))
   Global.REPORTING_SmallFiles_Path.append(new huemul_KeyValuePath("experimental","hdfs:///user/data/experimental/reporting/"))
   
   Global.REPORTING_BigFiles_Path.append(new huemul_KeyValuePath("production","hdfs:///user/data/production/reporting/"))
   Global.REPORTING_BigFiles_Path.append(new huemul_KeyValuePath("experimental","hdfs:///user/data/experimental/reporting/"))

   //SANDBOX SETTING
   Global.SANDBOX_DataBase.append(new huemul_KeyValuePath("production","production_sandbox"))
   Global.SANDBOX_DataBase.append(new huemul_KeyValuePath("experimental","experimental_sandbox"))
   
   Global.SANDBOX_SmallFiles_Path.append(new huemul_KeyValuePath("production","hdfs:///user/data/production/sandbox/"))
   Global.SANDBOX_SmallFiles_Path.append(new huemul_KeyValuePath("experimental","hdfs:///user/data/experimental/sandbox/"))
   
   Global.SANDBOX_BigFiles_Path.append(new huemul_KeyValuePath("production","hdfs:///user/data/production/sandbox/"))
   Global.SANDBOX_BigFiles_Path.append(new huemul_KeyValuePath("experimental","hdfs:///user/data/experimental/sandbox/"))
   
   
   //DQ_ERROR SETTING
   Global.DQ_SaveErrorDetails = true
   Global.DQError_DataBase.append(new huemul_KeyValuePath("production","production_DQError"))
   Global.DQError_DataBase.append(new huemul_KeyValuePath("experimental","experimental_DQError"))
   
   Global.DQError_Path.append(new huemul_KeyValuePath("production","hdfs:///user/data/production/dqerror/"))
   Global.DQError_Path.append(new huemul_KeyValuePath("experimental","hdfs:///user/data/experimental/dqerror/"))
   
   //MDM Old value full trace
   Global.MDM_SaveOldValueTrace = true
   Global.MDM_OldValueTrace_DataBase.append(new huemul_KeyValuePath("production","production_mdm_oldvalue"))
   Global.MDM_OldValueTrace_DataBase.append(new huemul_KeyValuePath("experimental","experimental_mdm_oldvalue"))

   Global.MDM_OldValueTrace_Path.append(new huemul_KeyValuePath("production","hdfs:///user/data/production/mdm_oldvalue/"))
   Global.MDM_OldValueTrace_Path.append(new huemul_KeyValuePath("experimental","hdfs:///user/data/experimental/mdm_oldvalue/"))
   
   

}


