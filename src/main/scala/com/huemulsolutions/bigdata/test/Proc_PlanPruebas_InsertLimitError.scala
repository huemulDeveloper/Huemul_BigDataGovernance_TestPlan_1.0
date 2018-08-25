package com.huemulsolutions.bigdata.test

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.raw.raw_DatosBasicos
import com.huemulsolutions.bigdata
import org.apache.hadoop.fs.FileSystem
import com.huemulsolutions.bigdata.tables.master._


object Proc_PlanPruebas_InsertLimitError {
  def main(args: Array[String]): Unit = {
    val huemulLib = new huemul_Library("01 - Plan pruebas error en insert por límite de filas",args,globalSettings.Global)
    val Control = new huemul_Control(huemulLib,null)
    
    val Ano = huemulLib.arguments.GetValue("ano", null,"Debe especificar ano de proceso: ejemplo: ano=2017")
    val Mes = huemulLib.arguments.GetValue("mes", null,"Debe especificar mes de proceso: ejemplo: mes=12")
    
    val TestPlanGroup: String = huemulLib.arguments.GetValue("TestPlanGroup", null, "Debe especificar el Grupo de Planes de Prueba")

    Control.AddParamInfo("TestPlanGroup", TestPlanGroup)
        
    try {
      var IdTestPlan: String = null
      
      Control.NewStep("Define DataFrame Original")
      val DF_RAW =  new raw_DatosBasicos(huemulLib, Control)
      if (!DF_RAW.open("DF_RAW", null, Ano.toInt, Mes.toInt, 1, 0, 0, 0,"")) {
        Control.RaiseError(s"Error al intentar abrir archivo de datos: ${DF_RAW.Error.ControlError_Message}")
      }
      
      Control.NewStep("Mapeo de Campos")
      val TablaMaster = new tbl_DatosBasicosLimitRow(huemulLib, Control)      
      TablaMaster.DataFramehuemul.setDataFrame(DF_RAW.DataFramehuemul.DataFrame, "DF_Original")
      
      
      TablaMaster.TipoValor.SetMapping("TipoValor",true,"coalesce(new.TipoValor,'nulo')","coalesce(new.TipoValor,'nulo')")
      TablaMaster.IntValue.SetMapping("IntValue")
      TablaMaster.BigIntValue.SetMapping("BigIntValue")
      TablaMaster.SmallIntValue.SetMapping("SmallIntValue")
      TablaMaster.TinyIntValue.SetMapping("TinyIntValue")
      TablaMaster.DecimalValue.SetMapping("DecimalValue")
      TablaMaster.RealValue.SetMapping("RealValue")
      TablaMaster.FloatValue.SetMapping("FloatValue")
      TablaMaster.StringValue.SetMapping("StringValue")
      TablaMaster.charValue.SetMapping("charValue")
      TablaMaster.timeStampValue.SetMapping("timeStampValue")
      
      Control.NewStep("PASO 1: INSERTA NORMAL")
      if (!TablaMaster.executeFull("DF_Final_Todo")) {
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Masterización", "No hay error en masterización", "No hay error en masterización", s"Si hay error en masterización", true)
        Control.RegisterTestPlanFeature("DQ_MaxNewRecords_Num", IdTestPlan)     
        Control.RaiseError(s"Error al masterizar (${TablaMaster.Error_Code}): ${TablaMaster.Error_Text}")
      } else {
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Masterización", "No hay error en masterización", "No hay error en masterización", s"No hay error en masterización", false)
        Control.RegisterTestPlanFeature("DQ_MaxNewRecords_Num", IdTestPlan)
      }
      
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Masterización", "No hay error en masterización", "Si hay error en masterización 1005", s"Si hay error en masterización (${TablaMaster.Error_Code})", TablaMaster.Error_Code == 1005)
      Control.RegisterTestPlanFeature("DQ_MaxNewRecords_Num", IdTestPlan)
      
      
          Control.FinishProcessOK
    } catch {
      case e: Exception => 
        val IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR", "ERROR DE PROGRAMA -  no deberia tener errror", "sin error", s"con error: ${e.getMessage}", false)
        Control.RegisterTestPlanFeature("DQ_MaxNewRecords_Num", IdTestPlan)
        Control.Control_Error.GetError(e, this.getClass.getSimpleName, null)
        Control.FinishProcessError()
    }
    
    huemulLib.spark.stop()
  }
}