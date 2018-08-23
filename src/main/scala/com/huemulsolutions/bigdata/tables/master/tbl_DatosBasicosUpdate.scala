package com.huemulsolutions.bigdata.tables.master

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.dataquality._
import com.huemulsolutions.bigdata.tables._
import com.huemulsolutions.bigdata.tables.huemulType_Tables._
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.DecimalType



class tbl_DatosBasicosUpdate(HuemulLib: huemul_Library, Control: huemul_Control) extends huemul_Table(HuemulLib,Control) with Serializable {
  this.TableType = huemulType_Tables.Master
  this.DataBase = HuemulLib.GlobalSettings.MASTER_DataBase
  this.Description = "Plan pruebas: verificar que todos los tipos de datos sean interpretados de forma correcta (carga 1 vez, luego solo actualiza datos)"
  this.GlobalPaths = HuemulLib.GlobalSettings.MASTER_BigFiles_Path
  this.LocalPath = "planPruebas/"
  this.StorageType = huemulType_StorageType.PARQUET
  
  val TipoValor = new huemul_Columns(StringType,true,"Nombre del tipo de valor")
  TipoValor.IsPK = true
  TipoValor.DQ_MinLen = 2
  TipoValor.DQ_MaxLen = 50
  TipoValor.Nullable = true
  
  
  val IntValue = new huemul_Columns(IntegerType,true,"datos integer")
  IntValue.Nullable = true
  IntValue.MDM_EnableDTLog = true
  IntValue.MDM_EnableOldValue = true
  IntValue.MDM_EnableProcessLog = true
  
  val BigIntValue = new huemul_Columns(LongType,true,"datos BigInt")
  BigIntValue.Nullable = true
  BigIntValue.MDM_EnableDTLog = true
  BigIntValue.MDM_EnableOldValue = true
  BigIntValue.MDM_EnableProcessLog = true
  
  val SmallIntValue = new huemul_Columns(ShortType,true,"datos SmallInt")
  SmallIntValue.Nullable = true
  SmallIntValue.MDM_EnableDTLog = true
  SmallIntValue.MDM_EnableOldValue = true
  SmallIntValue.MDM_EnableProcessLog = true
  
  val TinyIntValue = new huemul_Columns(ShortType,true,"datos TinyInt")
  TinyIntValue.Nullable = true
  TinyIntValue.MDM_EnableDTLog = true
  TinyIntValue.MDM_EnableOldValue = true
  TinyIntValue.MDM_EnableProcessLog = true
  
  val DecimalValue = new huemul_Columns(DecimalType(10,4),true,"datos Decimal(10,4)")
  DecimalValue.Nullable = true
  DecimalValue.MDM_EnableDTLog = true
  DecimalValue.MDM_EnableOldValue = true
  DecimalValue.MDM_EnableProcessLog = true
  
  val RealValue = new huemul_Columns(DoubleType,true,"datos Real")
  RealValue.Nullable = true
  RealValue.MDM_EnableDTLog = true
  RealValue.MDM_EnableOldValue = true
  RealValue.MDM_EnableProcessLog = true
  
  val FloatValue = new huemul_Columns(FloatType,true,"datos Float")
  FloatValue.Nullable = true
  FloatValue.MDM_EnableDTLog = true
  FloatValue.MDM_EnableOldValue = true
  FloatValue.MDM_EnableProcessLog = true
  
  val StringValue = new huemul_Columns(StringType,true,"datos String")
  StringValue.Nullable = true
  StringValue.MDM_EnableDTLog = true
  StringValue.MDM_EnableOldValue = true
  StringValue.MDM_EnableProcessLog = true
  
  val charValue = new huemul_Columns(StringType,true,"datos Char")
  charValue.Nullable = true
  charValue.MDM_EnableDTLog = true
  charValue.MDM_EnableOldValue = true
  charValue.MDM_EnableProcessLog = true
  
  val timeStampValue = new huemul_Columns(TimestampType,true,"datos TimeStamp")
  timeStampValue.Nullable = true
  timeStampValue.MDM_EnableDTLog = true
  timeStampValue.MDM_EnableOldValue = true
  timeStampValue.MDM_EnableProcessLog = true
  
  this.ApplyTableDefinition()
  
}