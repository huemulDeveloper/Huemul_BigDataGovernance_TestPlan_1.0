package com.huemulsolutions.bigdata.tables.master

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.dataquality._
import com.huemulsolutions.bigdata.tables._
import com.huemulsolutions.bigdata.tables.huemulType_Tables._
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.DecimalType



class tbl_DatosBasicosUpdate(HuemulLib: huemul_Library, Control: huemul_Control) extends huemul_Table(HuemulLib,Control) with Serializable {
  this.setTableType(huemulType_Tables.Master)
  this.setDataBase(HuemulLib.GlobalSettings.MASTER_DataBase)
  this.setDescription("Plan pruebas: verificar que todos los tipos de datos sean interpretados de forma correcta (carga 1 vez, luego solo actualiza datos)")
  this.setGlobalPaths(HuemulLib.GlobalSettings.MASTER_BigFiles_Path)
  this.setLocalPath("planPruebas/")
  this.setStorageType(huemulType_StorageType.PARQUET)
  
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
  
  val StringNoModificarValue = new huemul_Columns(StringType,true,"datos String solo inserta, no modifica")
  StringNoModificarValue.Nullable = true

  
  
  val IntDefaultValue = new huemul_Columns(IntegerType,false,"datos default integer")
  IntDefaultValue.DefaultValue = "10000"
  
  val BigIntDefaultValue = new huemul_Columns(LongType,false,"datos default BigInt")
  BigIntDefaultValue.DefaultValue = "10000"
  
  val SmallIntDefaultValue = new huemul_Columns(ShortType,false,"datos default SmallInt")
  SmallIntDefaultValue.DefaultValue = "10000"
  
  val TinyIntDefaultValue = new huemul_Columns(ShortType,false,"datos default TinyInt")
  TinyIntDefaultValue.DefaultValue = "10000"
  
  val DecimalDefaultValue = new huemul_Columns(DecimalType(10,4),false,"datos default Decimal(10,4)")
  DecimalDefaultValue.DefaultValue = "10000.345"
  
  val RealDefaultValue = new huemul_Columns(DoubleType,false,"datos default Real")
  RealDefaultValue.DefaultValue = "10000.456"
  
  val FloatDefaultValue = new huemul_Columns(FloatType,false,"datos default Float")
  FloatDefaultValue.DefaultValue = "10000.567"
  
  val StringDefaultValue = new huemul_Columns(StringType,false,"datos default String")
  StringDefaultValue.DefaultValue = "'valor en string'"
  
  val charDefaultValue = new huemul_Columns(StringType,false,"datos default Char")
  charDefaultValue.DefaultValue = "cast('hola' as string)"
  
  val timeStampDefaultValue = new huemul_Columns(TimestampType,false,"datos default TimeStamp")
  timeStampDefaultValue.DefaultValue = "'2019-01-01'"
  
  
  this.ApplyTableDefinition()
  
}