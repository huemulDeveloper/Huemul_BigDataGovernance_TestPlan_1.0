package com.huemulsolutions.bigdata.tables.master

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.dataquality._
import com.huemulsolutions.bigdata.tables._
import com.huemulsolutions.bigdata.tables.huemulType_Tables._
import com.huemulsolutions.bigdata.tables.huemulType_StorageType._
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.DecimalType



class tbl_DatosBasicos_mes_exclude(HuemulLib: huemul_BigDataGovernance, Control: huemul_Control) extends huemul_Table(HuemulLib,Control) with Serializable {
  this.setTableType(huemulType_Tables.Transaction)
  this.setDataBase(HuemulLib.GlobalSettings.MASTER_DataBase)
  this.setDescription("Plan pruebas: verificar que todos los tipos de datos sean interpretados de forma correcta")
  this.setGlobalPaths(HuemulLib.GlobalSettings.MASTER_BigFiles_Path)
  this.setLocalPath("planPruebas/")
  this.setStorageType(huemulType_StorageType.ORC)
  //this.setStorageType(huemulType_StorageType.PARQUET)
  this.setDQ_MaxNewRecords_Num(4)
  this.setPartitionField("periodo_mes")
  this.setFrequency(huemulType_Frequency.ANY_MOMENT)
  
  //Agrega version 1.3
  this.setNumPartitions(2)
  
  val periodo_mes = new huemul_Columns(StringType,true,"periodo")
  periodo_mes.setIsPK ( true)

  
  val TipoValor = new huemul_Columns(StringType,true,"Nombre del tipo de valor")
  TipoValor.setIsPK ( true)
  TipoValor.setDQ_MinLen ( 2)
  TipoValor.setDQ_MaxLen ( 50)
  
  
  val IntValue = new huemul_Columns(IntegerType,true,"datos integer")
  IntValue.setNullable ( true)
  
  
  
  val BigIntValue = new huemul_Columns(LongType,true,"datos BigInt")
  BigIntValue.setNullable ( true)
  
  val SmallIntValue = new huemul_Columns(ShortType,true,"datos SmallInt")
  SmallIntValue.setNullable ( true)
  
  val TinyIntValue = new huemul_Columns(ShortType,true,"datos TinyInt")
  TinyIntValue.setNullable ( true)
  
  val DecimalValue = new huemul_Columns(DecimalType(10,4),true,"datos Decimal(10,4)")
  DecimalValue.setNullable ( true)
  
  val RealValue = new huemul_Columns(DoubleType,true,"datos Real")
  RealValue.setNullable ( true)
  
  val FloatValue = new huemul_Columns(FloatType,true,"datos Float")
  FloatValue.setNullable ( true)
  
  val StringValue = new huemul_Columns(StringType,true,"datos String")
  StringValue.setNullable ( true)
  
  val charValue = new huemul_Columns(StringType,true,"datos Char")
  charValue.setNullable ( true)
  
  val timeStampValue = new huemul_Columns(TimestampType,true,"datos TimeStamp")
  timeStampValue.setNullable ( true)
  
  //Regla para probar exclusión de registro al fallar un warning
  val DQ_warning_exclude: huemul_DataQuality = new huemul_DataQuality(TipoValor ,"Exclusión de valor Cero-Vacio", "tipoValor not in ('Cero-Vacio')",1).setNotification(huemulType_DQNotification.WARNING_EXCLUDE).setQueryLevel(huemulType_DQQueryLevel.Row)
  val DQ_warning_solo: huemul_DataQuality = new huemul_DataQuality(TipoValor ,"Solo warning cuando aparezca registro Cero-Vacio", "tipoValor <> 'Negativo_Maximo'",2).setNotification(huemulType_DQNotification.WARNING).setQueryLevel(huemulType_DQQueryLevel.Row)
  
  
  //**********Ejemplo para aplicar DataQuality de Integridad Referencial
  val itbl_DatosBasicos = new tbl_DatosBasicos(HuemulLib,Control)
  val fk_tbl_DatosBasicos = new huemul_Table_Relationship(itbl_DatosBasicos, false).setNotification(huemulType_DQNotification.WARNING_EXCLUDE).broadcastJoin(true)
  fk_tbl_DatosBasicos.AddRelationship(itbl_DatosBasicos.TipoValor , TipoValor)
  
  
  this.ApplyTableDefinition()
  
}