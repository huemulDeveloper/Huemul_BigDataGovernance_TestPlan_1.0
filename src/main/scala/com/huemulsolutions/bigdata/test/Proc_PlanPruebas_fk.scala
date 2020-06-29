package com.huemulsolutions.bigdata.test

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.tables.master.tbl_DatosBasicos
import com.huemulsolutions.bigdata.raw.raw_DatosBasicos
import com.huemulsolutions.bigdata
import org.apache.hadoop.fs.FileSystem
import com.huemulsolutions.bigdata.tables.master.tbl_DatosBasicosErrores
import com.huemulsolutions.bigdata.tables.master.tbl_DatosBasicos_errorFK
import com.huemulsolutions.bigdata.tables.huemulType_StorageType._
import com.huemulsolutions.bigdata.tables.huemulType_StorageType
import com.huemulsolutions.bigdata.tables.huemul_TableConnector
import com.huemulsolutions.bigdata.tables.huemulType_InternalTableType

object Proc_PlanPruebas_fk {
  def main(args: Array[String]): Unit = {
    val huemulLib = new huemul_BigDataGovernance("01 - Plan pruebas Proc_PlanPruebas_fk",args,com.yourcompany.settings.globalSettings.Global)
    val Control = new huemul_Control(huemulLib,null, huemulType_Frequency.MONTHLY)
    
    val Ano = huemulLib.arguments.GetValue("ano", null,"Debe especificar ano de proceso: ejemplo: ano=2017")
    val Mes = huemulLib.arguments.GetValue("mes", null,"Debe especificar mes de proceso: ejemplo: mes=12")
    
    val TestPlanGroup: String = huemulLib.arguments.GetValue("TestPlanGroup", null, "Debe especificar el Grupo de Planes de Prueba")
    val TipoTablaParam: String = huemulLib.arguments.GetValue("TipoTabla", null, "Debe especificar TipoTabla (ORC,PARQUET,HBASE,DELTA)")
    var TipoTabla: huemulType_StorageType = null
    if (TipoTablaParam == "orc")
        TipoTabla = huemulType_StorageType.ORC
    else if (TipoTablaParam == "parquet")
        TipoTabla = huemulType_StorageType.PARQUET
    else if (TipoTablaParam == "delta")
        TipoTabla = huemulType_StorageType.DELTA
    else if (TipoTablaParam == "hbase")
        TipoTabla = huemulType_StorageType.HBASE
    else if (TipoTablaParam == "avro")
        TipoTabla = huemulType_StorageType.AVRO        
    Control.AddParamInformation("TestPlanGroup", TestPlanGroup)
        
    try {
      Control.NewStep("Define DataFrame Original")
      
      val TablaMaster = new tbl_DatosBasicos_errorFK (huemulLib, Control,TipoTabla)      
      TablaMaster.DF_from_SQL("DF_DATOS", """SELECT Codigo, tipoValor FROM (
                                                SELECT 1 as Codigo, null as tipovalor union all --error
                                                SELECT 2 as Codigo, 'Cero-Vacio' as tipovalor union all
                                                SELECT 3 as Codigo, 'Negativo_Maximo' as tipovalor union all
                                                SELECT 4 as Codigo, 'Negativo_Maximo2' as tipovalor union all --error
                                                SELECT 5 as Codigo, 'no existe' as tipovalor --error 
                                                                        ) tabla   
                                          """, false, 1) 
      
   //BORRA HDFS ANTIGUO PARA EFECTOS DEL PLAN DE PRUEBAS
      val a = huemulLib.spark.catalog.listTables(TablaMaster.getCurrentDataBase()).collect()
      if (a.filter { x => x.name.toUpperCase() == TablaMaster.TableName.toUpperCase()  }.length > 0) {
        huemulLib.spark.sql(s"drop table if exists ${TablaMaster.getTable()} ")
      } 
      
      val FullPath = new org.apache.hadoop.fs.Path(s"${TablaMaster.getFullNameWithPath()}")
      val fs = FullPath.getFileSystem(huemulLib.spark.sparkContext.hadoopConfiguration)
      if (fs.exists(FullPath))
        fs.delete(FullPath, true)
        
      if (TipoTablaParam == "hbase") {
        Control.NewStep("borrar tabla")
        val th = new huemul_TableConnector(huemulLib, Control)
        th.tableDeleteHBase(TablaMaster.getHBaseNamespace(huemulType_InternalTableType.Normal), TablaMaster.getHBaseTableName(huemulType_InternalTableType.Normal))
      }
        
   //BORRA HDFS ANTIGUO PARA EFECTOS DEL PLAN DE PRUEBAS
        
      TablaMaster.Codigo.SetMapping("Codigo")
      TablaMaster.TipoValor.SetMapping("tipovalor")

      Control.NewStep("Ejecución")
      TablaMaster.executeFull("DF_Final", org.apache.spark.storage.StorageLevel.MEMORY_ONLY_SER)
      var IdTestPlan: String = ""
      //Column_DQ_MinLen
      var ErrorReg = TablaMaster.DataFramehuemul.getDQResult().filter { x => x.ColumnName != null && x.ColumnName.toLowerCase()  == "TipoValor".toLowerCase() && x.DQ_ErrorCode == 1024 }
      
      if (ErrorReg == null || ErrorReg.length == 0){
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR TipoValor", "error 1024 encontrado en columna TipoValor", "error encontrado", s"error no encontrado", false)
        Control.RegisterTestPlanFeature("FK Error encontrado", IdTestPlan)
      }
      else {
        val ErrorEncontrado = ErrorReg(0)
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR TipoValor", "error 1024 encontrado en columna TipoValor", "ErrorCode = 1024, N° registros con error: 3", s"ErrorCode = 1024, N° registros con error: ${ErrorEncontrado.DQ_NumRowsError}", ErrorEncontrado.DQ_NumRowsError == 3)
        Control.RegisterTestPlanFeature("FK Error encontrado", IdTestPlan)
      }
      
      //Column_DQ_MaxLen
      val numErrores = TablaMaster.DataFramehuemul.getDQResult().filter {x => x.DQ_IsError == true}.length
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "N° errores en DQ = 2", "N° erorres en DQ = 2", "N° errores en DQ = 2", s"N° Errores en DQ = ${numErrores}", numErrores == 2)
      Control.RegisterTestPlanFeature("FK Error encontrado", IdTestPlan)
        
      //calida directamente en la tabla 
      val errores1 = huemulLib.spark.sql(s"select cast(count(1) as int) as Cantidad from production_dqerror.tbl_datosbasicos_errorfk_dq where dq_control_id = '${Control.Control_Id}' ").collect
      var cantidad = errores1(0).getAs[Int]("Cantidad") 
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Guarda errores production_dqerror.tbl_datosbasicos_errorfk_dq ", "errores en la tabla de production_dqerror.tbl_datosbasicos_errorfk_dq ", "Cantidad con errores = 4", s"Cantidad con errores = ${cantidad}", cantidad == 4)
      Control.RegisterTestPlanFeature("FK Error encontrado", IdTestPlan)
      
      val errores2 = huemulLib.spark.sql(s"""select dq_error_columnname
                                                  ,cast(count(1) as int) as Cantidad
                                                  ,cast(max(case when tipovalor = "Negativo_Maximo2" then 1 else 0 end) as Int) as error_01
                                                  ,cast(max(case when tipovalor = "no existe" then 1 else 0 end)        as Int) as error_02
                                                  ,cast(max(case when tipovalor is null then 1 else 0 end)              as Int) as error_03
                        from production_dqerror.tbl_datosbasicos_errorfk_dq 
                        where dq_control_id = '${Control.Control_Id}' 
                        group by dq_error_columnname """).collect
      cantidad = errores2.filter { x => x.getAs[String]("dq_error_columnname").toLowerCase() == "TipoValor".toLowerCase() }(0).getAs[Int]("Cantidad") 
      val error_01 = errores2.filter { x => x.getAs[String]("dq_error_columnname").toLowerCase() == "TipoValor".toLowerCase() }(0).getAs[Int]("error_01") 
      val error_02 = errores2.filter { x => x.getAs[String]("dq_error_columnname").toLowerCase() == "TipoValor".toLowerCase() }(0).getAs[Int]("error_02") 
      val error_03 = errores2.filter { x => x.getAs[String]("dq_error_columnname").toLowerCase() == "TipoValor".toLowerCase() }(0).getAs[Int]("error_03") 
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Guarda errores en tabla _dq TipoValor", "errores en columna TipoValor", "Cantidad con errores = 4", s"Cantidad con errores = ${cantidad}", cantidad == 4)
      Control.RegisterTestPlanFeature("FK Error encontrado", IdTestPlan)
      
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "error encontrado (tipovalor = Negativo_Maximo2)", "error encontrado (tipovalor = Negativo_Maximo2)", "error_01 = 1", s"error_01 = ${error_01}", error_01 == 1)
      Control.RegisterTestPlanFeature("FK Error encontrado", IdTestPlan)
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "error encontrado (tipovalor = Negativo_Maximo2)", "error encontrado (tipovalor = Negativo_Maximo2)", "error_02 = 1", s"error_02 = ${error_02}", error_02 == 1)
      Control.RegisterTestPlanFeature("FK Error encontrado", IdTestPlan)
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "error encontrado (tipovalor = Negativo_Maximo2)", "error encontrado (tipovalor = Negativo_Maximo2)", "error_03 = 1", s"error_03 = ${error_03}", error_03 == 1)
      Control.RegisterTestPlanFeature("FK Error encontrado", IdTestPlan)
      
      
      
          Control.FinishProcessOK
    } catch {
      case e: Exception => 
        val IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR", "ERROR DE PROGRAMA -  no deberia tener errror", "sin error", s"con error: ${e.getMessage}", false)
        Control.RegisterTestPlanFeature("executeFull", IdTestPlan)
        Control.Control_Error.GetError(e, this.getClass.getSimpleName, 1)
        Control.FinishProcessError()
    }
    
    if (Control.TestPlan_CurrentIsOK(7))
      println("Proceso OK")
      
    huemulLib.close()
  }
}