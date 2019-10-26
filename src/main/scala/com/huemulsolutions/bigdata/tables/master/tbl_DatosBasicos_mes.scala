package com.huemulsolutions.bigdata.tables.master

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.dataquality._
import com.huemulsolutions.bigdata.tables._
import com.huemulsolutions.bigdata.tables.huemulType_Tables._
import com.huemulsolutions.bigdata.tables.huemulType_StorageType._
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.DecimalType



class tbl_DatosBasicos_mes(HuemulLib: huemul_BigDataGovernance, Control: huemul_Control) extends huemul_Table(HuemulLib,Control) with Serializable {
  this.setTableType(huemulType_Tables.Transaction)
  this.setDataBase(HuemulLib.GlobalSettings.MASTER_DataBase)
  this.setDescription("Plan pruebas: verificar que todos los tipos de datos sean interpretados de forma correcta")
  this.setGlobalPaths(HuemulLib.GlobalSettings.MASTER_BigFiles_Path)
  this.setLocalPath("planPruebas/")
  this.setStorageType(huemulType_StorageType.PARQUET)
  this.setDQ_MaxNewRecords_Num(4)
  this.setPartitionField("periodo_mes")
  this.setFrequency(huemulType_Frequency.ANY_MOMENT)
  
  //Agrega version 1.3
  this.setNumPartitions(2)
  
  val periodo_mes = new huemul_Columns(StringType,true,"periodo")
  periodo_mes.setIsPK ( )

  
  val TipoValor = new huemul_Columns(StringType,true,"Nombre del tipo de valor")
    .setIsPK ()
    .setDQ_MinLen ( 2,null)
    .setDQ_MaxLen ( 50,null)
  
  
  val IntValue = new huemul_Columns(IntegerType,true,"datos integer")
      .setNullable ( )
  
  
  
  val BigIntValue = new huemul_Columns(LongType,true,"datos BigInt")
  BigIntValue.setNullable ( )
  
  val SmallIntValue = new huemul_Columns(ShortType,true,"datos SmallInt")
  SmallIntValue.setNullable ()
  
  val TinyIntValue = new huemul_Columns(ShortType,true,"datos TinyInt")
  TinyIntValue.setNullable ()
  
  val DecimalValue = new huemul_Columns(DecimalType(10,4),true,"datos Decimal(10,4)")
  DecimalValue.setNullable ()
  
  val RealValue = new huemul_Columns(DoubleType,true,"datos Real")
  RealValue.setNullable ()
  
  val FloatValue = new huemul_Columns(FloatType,true,"datos Float")
  FloatValue.setNullable ()
  
  val StringValue = new huemul_Columns(StringType,true,"datos String")
  StringValue.setNullable ()
  
  val charValue = new huemul_Columns(StringType,true,"datos Char")
  charValue.setNullable ()
  
  val timeStampValue = new huemul_Columns(TimestampType,true,"datos TimeStamp")
  timeStampValue.setNullable ()
  
  
  
  
  //**********Ejemplo para aplicar DataQuality de Integridad Referencial
  val itbl_DatosBasicos = new tbl_DatosBasicos(HuemulLib,Control)
  val fk_tbl_DatosBasicos = new huemul_Table_Relationship(itbl_DatosBasicos, false)
  fk_tbl_DatosBasicos.AddRelationship(itbl_DatosBasicos.TipoValor , TipoValor)
  
  
  this.ApplyTableDefinition()
  
}