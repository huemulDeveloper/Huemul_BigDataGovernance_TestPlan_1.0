package com.huemulsolutions.bigdata.test

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.tables.master.tbl_DatosBasicos
import com.huemulsolutions.bigdata.raw.raw_DatosBasicos
import com.huemulsolutions.bigdata
import org.apache.hadoop.fs.FileSystem
import com.huemulsolutions.bigdata.tables.master.tbl_DatosBasicosErrores
import com.huemulsolutions.bigdata.tables.master.tbl_DatosBasicos_errorFK


object Proc_PlanPruebas_fk {
  def main(args: Array[String]): Unit = {
    val huemulLib = new huemul_BigDataGovernance("01 - Plan pruebas Proc_PlanPruebas_fk",args,globalSettings.Global)
    val Control = new huemul_Control(huemulLib,null, huemulType_Frequency.MONTHLY)
    
    val Ano = huemulLib.arguments.GetValue("ano", null,"Debe especificar ano de proceso: ejemplo: ano=2017")
    val Mes = huemulLib.arguments.GetValue("mes", null,"Debe especificar mes de proceso: ejemplo: mes=12")
    
    val TestPlanGroup: String = huemulLib.arguments.GetValue("TestPlanGroup", null, "Debe especificar el Grupo de Planes de Prueba")

    Control.AddParamInformation("TestPlanGroup", TestPlanGroup)
        
    try {
      Control.NewStep("Define DataFrame Original")
      
      val TablaMaster = new tbl_DatosBasicos_errorFK (huemulLib, Control)      
      TablaMaster.DF_from_SQL("DF_DATOS", """SELECT Codigo, tipoValor FROM (
                                                SELECT 1 as Codigo, null as tipovalor union all --error
                                                SELECT 2 as Codigo, '     Cero-Vacio' as tipovalor union all
                                                SELECT 3 as Codigo, 'Negativo_Maximo' as tipovalor union all
                                                SELECT 4 as Codigo, 'Negativo_Maximo2' as tipovalor union all --error
                                                SELECT 5 as Codigo, 'no existe' as tipovalor --error 
                                                                        ) tabla   
                                          """, false, 1) 
      
   //BORRA HDFS ANTIGUO PARA EFECTOS DEL PLAN DE PRUEBAS
      val a = huemulLib.spark.catalog.listTables(TablaMaster.GetCurrentDataBase()).collect()
      if (a.filter { x => x.name.toUpperCase() == TablaMaster.TableName.toUpperCase()  }.length > 0) {
        huemulLib.spark.sql(s"drop table if exists ${TablaMaster.GetTable()} ")
      } 
      
      val FullPath = new org.apache.hadoop.fs.Path(s"${TablaMaster.GetFullNameWithPath()}")
      val fs = FileSystem.get(huemulLib.spark.sparkContext.hadoopConfiguration) 
      if (fs.exists(FullPath))
        fs.delete(FullPath, true)
        
   //BORRA HDFS ANTIGUO PARA EFECTOS DEL PLAN DE PRUEBAS
        
      TablaMaster.Codigo.SetMapping("Codigo")
      TablaMaster.TipoValor.SetMapping("tipovalor")

      Control.NewStep("Ejecución")
      TablaMaster.executeFull("DF_Final", org.apache.spark.storage.StorageLevel.MEMORY_ONLY_SER)
      var IdTestPlan: String = ""
      //Column_DQ_MinLen
      var ErrorReg = TablaMaster.DataFramehuemul.getDQResult().filter { x => x.ColumnName == "TipoValor" && x.DQ_ErrorCode == 1024 }
      
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
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "N° errores en DQ = 1", "N° erorres en DQ = 1", "N° errores en DQ = 1", s"N° Errores en DQ = ${numErrores}", numErrores == 1)
      Control.RegisterTestPlanFeature("FK Error encontrado", IdTestPlan)
        
      
      
      
          Control.FinishProcessOK
    } catch {
      case e: Exception => 
        val IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR", "ERROR DE PROGRAMA -  no deberia tener errror", "sin error", s"con error: ${e.getMessage}", false)
        Control.RegisterTestPlanFeature("executeFull", IdTestPlan)
        Control.Control_Error.GetError(e, this.getClass.getSimpleName, 1)
        Control.FinishProcessError()
    }
    
    if (Control.TestPlan_CurrentIsOK(2))
      println("Proceso OK")
      
    huemulLib.close()
  }
}