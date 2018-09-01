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




class tbl_DatosBasicosErrores(HuemulLib: huemul_BigDataGovernance, Control: huemul_Control) extends huemul_Table(HuemulLib,Control) with Serializable {
  this.setTableType(huemulType_Tables.Master)
  this.setDataBase(HuemulLib.GlobalSettings.MASTER_DataBase)
  this.setDescription("Plan pruebas: verificar que todos los tipos de datos sean interpretados de forma correcta")
  this.setGlobalPaths(HuemulLib.GlobalSettings.MASTER_BigFiles_Path)
  this.setLocalPath("planPruebas/")
  this.setStorageType(huemulType_StorageType.PARQUET)
  
  val TipoValor = new huemul_Columns(StringType,true,"Nombre del tipo de valor")
  TipoValor.setIsPK ( true )
  TipoValor.setDQ_MinLen ( 2)
  TipoValor.setDQ_MaxLen ( 50)
  
  //Valida MinMax String
  val Column_DQ_MinLen = new huemul_Columns(StringType,true,"Valida minimo largo de un string")
  Column_DQ_MinLen.setNullable ( true)
  Column_DQ_MinLen.setDQ_MinLen ( 10) //3 errores
  
  val Column_DQ_MaxLen = new huemul_Columns(StringType,true,"Valida máximo largo de un string")
  Column_DQ_MaxLen.setNullable ( true)
  Column_DQ_MaxLen.setDQ_MaxLen ( 10 )//2 errores
  
  //Valida MinMax Decimal
  val Column_DQ_MinDecimalValue = new huemul_Columns(DecimalType(10,4),true,"Valida minimo valor de un decimal")
  Column_DQ_MinDecimalValue.setNullable ( true)
  Column_DQ_MinDecimalValue.setDQ_MinDecimalValue ( Decimal.apply(0))//2 errores
  
  val Column_DQ_MaxDecimalValue = new huemul_Columns(DecimalType(10,4),true,"Valida máximo valor de un decimal")
  Column_DQ_MaxDecimalValue.setNullable ( true)
  Column_DQ_MaxDecimalValue.setDQ_MaxDecimalValue ( Decimal.apply("10.124") ) //1 errores
  
  
  //Valida MinMax DateTime
  val Column_DQ_MinDateTimeValue = new huemul_Columns(TimestampType,true,"Valida minimo valor de una fecha")
  Column_DQ_MinDateTimeValue.setNullable ( true)
  Column_DQ_MinDateTimeValue.setDQ_MinDateTimeValue ( "2017-05-01") //3 errores
  
  val Column_DQ_MaxDateTimeValue = new huemul_Columns(TimestampType,true,"Valida máximo valor de una fecha")
  Column_DQ_MaxDateTimeValue.setNullable ( true)
  Column_DQ_MaxDateTimeValue.setDQ_MaxDateTimeValue ( "2017-05-01" )//2 errores
  
  val Column_NotNull = new huemul_Columns(IntegerType,true,"datos integer - Error nulo")
  Column_NotNull.setNullable ( false)
  
  val Column_IsUnique = new huemul_Columns(StringType,true,"datos string - valor unico")
  Column_IsUnique.setNullable ( true)
  Column_IsUnique.setIsUnique ( true)
  
  
  val Column_OpcionalNoMapeado = new huemul_Columns(IntegerType,false,"datos integer - Opcional no mapeado")
  Column_OpcionalNoMapeado.setDefaultValue ( "10")
  
  //Valida MinMax DateTime
  val Column_NoMapeadoDefault = new huemul_Columns(TimestampType,false,"No Mapeado Default")
  Column_DQ_MinDecimalValue.setNullable ( true)
  Column_DQ_MinDecimalValue.setDefaultValue ( "2017-05-01" )
  
  
  this.ApplyTableDefinition()
  
}