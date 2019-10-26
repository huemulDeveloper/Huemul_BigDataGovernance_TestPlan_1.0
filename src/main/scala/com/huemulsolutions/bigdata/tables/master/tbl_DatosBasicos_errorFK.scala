package com.huemulsolutions.bigdata.tables.master

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.dataquality._
import com.huemulsolutions.bigdata.tables._
import com.huemulsolutions.bigdata.tables.huemulType_Tables._
import com.huemulsolutions.bigdata.tables.huemulType_StorageType._
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.DecimalType



class tbl_DatosBasicos_errorFK(HuemulLib: huemul_BigDataGovernance, Control: huemul_Control) extends huemul_Table(HuemulLib,Control) with Serializable {
  this.setTableType(huemulType_Tables.Master)
  this.setDataBase(HuemulLib.GlobalSettings.MASTER_DataBase)
  this.setDescription("Plan pruebas: verificar que error en FK funcione con registro en DQ")
  this.setGlobalPaths(HuemulLib.GlobalSettings.MASTER_BigFiles_Path)
  this.setLocalPath("planPruebas/")
  this.setStorageType(huemulType_StorageType.PARQUET)
  this.setDQ_MaxNewRecords_Num(4)
  this.setFrequency(huemulType_Frequency.ANY_MOMENT)
  
  //Agrega version 1.3
  this.setNumPartitions(2)

  val Codigo = new huemul_Columns(IntegerType,true,"Codigo del registro PK")
  Codigo.setIsPK ( )
  
  
  val TipoValor = new huemul_Columns(StringType,true,"Nombre del tipo de valor (FK)")
      .setDQ_MinLen ( 2,null)
      .setDQ_MaxLen ( 50,null)
      
  
  val CampoAdicional = new huemul_Columns(StringType,false,"valor Adicional")
  CampoAdicional.setDefaultValues("'no asignado'")
  
  
  //**********Ejemplo para aplicar DataQuality de Integridad Referencial
  val itbl_DatosBasicos = new tbl_DatosBasicos(HuemulLib,Control)
  val fk_tbl_DatosBasicos = new huemul_Table_Relationship(itbl_DatosBasicos, false).setExternalCode("USER_FK_CODE")
  fk_tbl_DatosBasicos.AddRelationship(itbl_DatosBasicos.TipoValor , TipoValor)
  
  
  
  this.ApplyTableDefinition()
  
}