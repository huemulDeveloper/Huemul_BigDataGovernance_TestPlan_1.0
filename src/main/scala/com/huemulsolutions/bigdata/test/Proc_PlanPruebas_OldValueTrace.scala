package com.huemulsolutions.bigdata.test

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.tables.master.tbl_DatosBasicos
import com.huemulsolutions.bigdata.raw.raw_DatosBasicos
import com.huemulsolutions.bigdata
import org.apache.hadoop.fs.FileSystem
import com.huemulsolutions.bigdata.dataquality.huemul_DataQuality
import com.huemulsolutions.bigdata.dataquality.huemulType_DQQueryLevel
import com.huemulsolutions.bigdata.dataquality.huemulType_DQNotification.huemulType_DQNotification
import com.huemulsolutions.bigdata.dataquality.huemulType_DQNotification
import scala.collection.mutable._
import org.apache.spark.sql.types._
import com.huemulsolutions.bigdata.raw.raw_DatosOldValue
import com.huemulsolutions.bigdata.tables.master.tbl_OldValueTrace

object Proc_PlanPruebas_OldValueTrace {
  def main(args: Array[String]): Unit = {
    val huemulLib = new huemul_BigDataGovernance("01 - Plan pruebas Proc_PlanPruebas_OldValueTrace",args,globalSettings.Global)
    val Control = new huemul_Control(huemulLib,null, huemulType_Frequency.MONTHLY)
    
    val Ano = huemulLib.arguments.GetValue("ano", null,"Debe especificar ano de proceso: ejemplo: ano=2017")
    val Mes = huemulLib.arguments.GetValue("mes", null,"Debe especificar mes de proceso: ejemplo: mes=12")
    
    val TestPlanGroup: String = huemulLib.arguments.GetValue("TestPlanGroup", null, "Debe especificar el Grupo de Planes de Prueba")

    Control.AddParamInformation("TestPlanGroup", TestPlanGroup)
        
    try {
      var IdTestPlan: String = null
      
      Control.NewStep("Define DataFrame Original")
      val DF_RAW =  new raw_DatosOldValue(huemulLib, Control)
      
      if (!DF_RAW.open("DF_RAW", null, Ano.toInt, Mes.toInt, 1, 0, 0, 0,"ini")) {
        Control.RaiseError(s"Error al intentar abrir archivo de datos: ${DF_RAW.Error.ControlError_Message}")
      }
      Control.NewStep("Mapeo de Campos")
      val TablaMaster = new tbl_OldValueTrace (huemulLib, Control)      
      TablaMaster.DataFramehuemul.setDataFrame(DF_RAW.DataFramehuemul.DataFrame, "DF_Original")
      
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
        
        
      TablaMaster.setMappingAuto()
      
      //TODO: cambiar el parámetro "true" por algo.UPDATE O algo.NOUPDATE (en replaceValueOnUpdate
      Control.NewStep("Ejecución")
      val tp_resultado = TablaMaster.executeFull("DF_Final", org.apache.spark.storage.StorageLevel.MEMORY_ONLY ) 
      
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Masterización", "No hay error en masterización", "No hay error en masterización", s"Si hay error en masterización", tp_resultado == true)
      Control.RegisterTestPlanFeature("OldValueTrace - inicial", IdTestPlan)
      
        
      
      
      
      //**************************************************************************/
      val DF_RAW_final =  new raw_DatosOldValue(huemulLib, Control)
      
      if (!DF_RAW_final.open("DF_RAW_final", null, Ano.toInt, Mes.toInt, 1, 0, 0, 0,"fin")) {
        Control.RaiseError(s"Error al intentar abrir archivo de datos fin: ${DF_RAW.Error.ControlError_Message}")
      }
      Control.NewStep("Mapeo de Campos")
      TablaMaster.DataFramehuemul.setDataFrame(DF_RAW_final.DataFramehuemul.DataFrame, "DF_Original")
      TablaMaster.setMappingAuto()
      val tp_resultado_2 = TablaMaster.executeFull("DF_Final_2", org.apache.spark.storage.StorageLevel.MEMORY_ONLY )
     
      Control.FinishProcessOK
    } catch {
      case e: Exception => 
        val IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR", "ERROR DE PROGRAMA -  no deberia tener errror", "sin error", s"con error: ${e.getMessage}", false)
        Control.RegisterTestPlanFeature("executeFull", IdTestPlan)
        Control.Control_Error.GetError(e, this.getClass.getSimpleName, 1)
        Control.FinishProcessError()
    }
    
    if (Control.TestPlan_CurrentIsOK(null))
      println("Proceso OK")
    
    huemulLib.close()
  }
}