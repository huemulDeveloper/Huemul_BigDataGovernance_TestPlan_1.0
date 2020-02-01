package com.huemulsolutions.bigdata.tables.master

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.dataquality._
import com.huemulsolutions.bigdata.tables._
import com.huemulsolutions.bigdata.tables.huemulType_Tables._
import com.huemulsolutions.bigdata.tables.huemulType_StorageType._
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.DecimalType



class tbl_OldValueTrace(HuemulLib: huemul_BigDataGovernance, Control: huemul_Control, TipoTabla: huemulType_StorageType) extends huemul_Table(HuemulLib,Control) with Serializable {
  this.setTableType(huemulType_Tables.Master)
  this.setDataBase(HuemulLib.GlobalSettings.MASTER_DataBase)
  this.setDescription("Plan pruebas: verifica el correcto registro de los cambios en tabla oldvalue")
  this.setGlobalPaths(HuemulLib.GlobalSettings.MASTER_BigFiles_Path)
  this.setLocalPath("planPruebas/")
 
  //this.setStorageType(huemulType_StorageType.PARQUET)
  this.setStorageType(TipoTabla)
  this.setDQ_MaxNewRecords_Num(4)
  this.setFrequency(huemulType_Frequency.ANY_MOMENT)
  
  this.WhoCanRun_executeFull_addAccess("com.huemulsolutions.bigdata.test", "Proc_PlanPruebas_OldValueTrace")
  
  //Agrega version 1.3
  this.setNumPartitions(1)

  
  val codigo = new huemul_Columns(IntegerType,true,"Codigo")
  codigo.setIsPK ( true)
  
  
  val Descripcion = new huemul_Columns(StringType,true,"descripción de la tabla")
  Descripcion.setNullable ( true)
  Descripcion.setMDM_EnableOldValue_FullTrace( true) 
  
  val Fecha = new huemul_Columns(TimestampType,true,"datos TimeStamp")
  Fecha.setNullable ( true)
  Fecha.setMDM_EnableOldValue_FullTrace( true)
  Fecha.setMDM_EnableDTLog(true)
  Fecha.setMDM_EnableProcessLog(true)
  Fecha.setMDM_EnableOldValue(true)
  
  val Monto = new huemul_Columns(IntegerType,true,"datos Monto")
  Monto.setNullable ( true)
  Monto.setMDM_EnableOldValue_FullTrace( true)
  Monto.setMDM_EnableDTLog(true)
  Monto.setMDM_EnableProcessLog(true)
  Monto.setMDM_EnableOldValue(true)
   
  
  this.ApplyTableDefinition()
  
}