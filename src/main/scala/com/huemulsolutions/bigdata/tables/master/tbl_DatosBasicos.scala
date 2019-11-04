package com.huemulsolutions.bigdata.tables.master

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.dataquality._
import com.huemulsolutions.bigdata.tables._
import com.huemulsolutions.bigdata.tables.huemulType_Tables._
import com.huemulsolutions.bigdata.tables.huemulType_StorageType._
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.Decimal


class tbl_DatosBasicos(HuemulLib: huemul_BigDataGovernance, Control: huemul_Control) extends huemul_Table(HuemulLib,Control) with Serializable {
  this.setTableType(huemulType_Tables.Master)
  this.setDataBase(HuemulLib.GlobalSettings.MASTER_DataBase)
  this.setDescription("Plan pruebas: verificar que todos los tipos de datos sean interpretados de forma correcta")
  this.setGlobalPaths(HuemulLib.GlobalSettings.MASTER_BigFiles_Path)
  this.setLocalPath("planPruebas/")
  this.setStorageType(huemulType_StorageType.ORC)
  //this.setStorageType(huemulType_StorageType.PARQUET)
  this.setDQ_MaxNewRecords_Num(4)
  this.setFrequency(huemulType_Frequency.ANY_MOMENT)
  
  //Agrega version 1.3
  this.setNumPartitions(2)
  
  //Agrega versión 2.0
  this.setSaveBackup(true)

  this.setPK_externalCode("USER_COD_PK")
  
  val TipoValor = new huemul_Columns(StringType,true,"Nombre del tipo de valor")
                            .setIsPK().setDQ_MinLen(2, "USER_COD_MINLEN").setDQ_MaxLen(50, "USER_COD_MAXLEN")
  //TipoValor.setIsPK ( true)
  //TipoValor.setDQ_MinLen ( 2)
  //TipoValor.setDQ_MaxLen ( 50)
  //TipoValor.setBusinessGlossary_Id("BG_001")
  
  val IntValue = new huemul_Columns(IntegerType,true,"datos integer")
                            .setMDM_EnableOldValue_FullTrace().setBusinessGlossary("BG_002")
  IntValue.setNullable ( true)
  //IntValue.setMDM_EnableOldValue_FullTrace( true)
  //IntValue.setBusinessGlossary_Id("BG_002")
  //IntValue.setDQ_MaxDecimalValue(Decimal.apply(10))
  
  val BigIntValue = new huemul_Columns(LongType,true,"datos BigInt").setMDM_EnableOldValue_FullTrace()
  BigIntValue.setNullable ()
  //BigIntValue.setMDM_EnableOldValue_FullTrace( true)
  
  val SmallIntValue = new huemul_Columns(ShortType,true,"datos SmallInt").setMDM_EnableOldValue_FullTrace()
                    .setNullable ()
  //SmallIntValue.setMDM_EnableOldValue_FullTrace( true)
  
  val TinyIntValue = new huemul_Columns(ShortType,true,"datos TinyInt")
            .setNullable ()
  
  val DecimalValue = new huemul_Columns(DecimalType(10,4),true,"datos Decimal(10,4)")
            .setNullable ()
  
  val RealValue = new huemul_Columns(DoubleType,true,"datos Real")
            .setNullable ()
  
  val FloatValue = new huemul_Columns(FloatType,true,"datos Float")
            .setNullable ()
  
  val StringValue = new huemul_Columns(StringType,true,"datos String")
            .setNullable ()
  
  val charValue = new huemul_Columns(StringType,true,"datos Char")
            .setNullable ()
  
  val timeStampValue = new huemul_Columns(TimestampType,true,"datos TimeStamp")
            .setNullable ()
  
  
  
  
   val IntDefaultValue = new huemul_Columns(IntegerType,false,"datos default integer")
  IntDefaultValue.setDefaultValues ( "10000")
  
  val BigIntDefaultValue = new huemul_Columns(LongType,false,"datos default BigInt")
  BigIntDefaultValue.setDefaultValues ( "10000")
  
  val SmallIntDefaultValue = new huemul_Columns(ShortType,false,"datos default SmallInt")
  SmallIntDefaultValue.setDefaultValues ( "10000")
  
  val TinyIntDefaultValue = new huemul_Columns(ShortType,false,"datos default TinyInt")
  TinyIntDefaultValue.setDefaultValues ( "10000")
  
  val DecimalDefaultValue = new huemul_Columns(DecimalType(10,4),false,"datos default Decimal(10,4)")
  DecimalDefaultValue.setDefaultValues ( "10000.345")
  
  val RealDefaultValue = new huemul_Columns(DoubleType,false,"datos default Real")
  RealDefaultValue.setDefaultValues ( "10000.456")
  
  val FloatDefaultValue = new huemul_Columns(FloatType,false,"datos default Float")
  FloatDefaultValue.setDefaultValues ( "10000.567")
  
  val StringDefaultValue = new huemul_Columns(StringType,false,"datos default String")
  StringDefaultValue.setDefaultValues ( "'valor en string'")
  
  val charDefaultValue = new huemul_Columns(StringType,false,"datos default Char")
  charDefaultValue.setDefaultValues ( "cast('hola' as string)")
  
  val timeStampDefaultValue = new huemul_Columns(TimestampType,false,"datos default TimeStamp")
  timeStampDefaultValue.setDefaultValues ( "'2019-01-01'")
  
  
  this.ApplyTableDefinition()
  
}