package com.huemulsolutions.bigdata.tables.master

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.tables.huemulType_StorageType.huemulType_StorageType
import com.huemulsolutions.bigdata.tables.{huemul_Columns, _}
import org.apache.spark.sql.types.DataTypes._


class tbl_DatosParticionAcum(HuemulLib: huemul_BigDataGovernance, Control: huemul_Control, TipoTabla: huemulType_StorageType) extends huemul_Table(HuemulLib,Control) with Serializable {
  this.setTableType(huemulType_Tables.Transaction)
  this.setDataBase(HuemulLib.GlobalSettings.MASTER_DataBase)
  this.setDescription("Plan pruebas: Carga datos con varias particiones")
  this.setGlobalPaths(HuemulLib.GlobalSettings.MASTER_BigFiles_Path)
  this.setLocalPath("planPruebas/")
  this.setStorageType(TipoTabla)
  //this.setStorageType(huemulType_StorageType.ORC)
  //this.setStorageType(huemulType_StorageType.PARQUET)
  //this.setDQ_MaxNewRecords_Num(value = 4)
  this.setFrequency(huemulType_Frequency.DAILY)
  
  //Agrega version 1.3
  //this.setNumPartitions(2)
  
  //Agrega versión 2.0
  //this.setSaveBackup(true)

  this.setPK_externalCode("USER_COD_PK")
  
  val periodo: huemul_Columns = new huemul_Columns(DateType,true,"Periodo de los datos")
                            .setIsPK().setPartitionColumn(1,dropBeforeInsert = false, oneValuePerProcess = false)

  val idTx: huemul_Columns = new huemul_Columns(StringType,true,"codigo de la transacción")
    .setIsPK()


  val EmpresA: huemul_Columns = new huemul_Columns(StringType,true,"Empresa que registra ventas")
    .setPartitionColumn(2,dropBeforeInsert = false, oneValuePerProcess = false)

  val app: huemul_Columns = new huemul_Columns(StringType,true,"app que registra ventas")
    .setPartitionColumn(3,dropBeforeInsert = false, oneValuePerProcess = false)

  val producto: huemul_Columns = new huemul_Columns(StringType,true,"producto de la venta")

  val cantidad: huemul_Columns = new huemul_Columns(IntegerType,true,"Cantidad de productos vendidos")

  val precio: huemul_Columns = new huemul_Columns(IntegerType,true,"precio de la venta")


  
  this.ApplyTableDefinition()
  
}