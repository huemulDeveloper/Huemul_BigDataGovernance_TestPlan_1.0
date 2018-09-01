package com.huemulsolutions.bigdata.test

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.tables.master.tbl_DatosBasicos
import com.huemulsolutions.bigdata.raw.raw_DatosBasicos
import com.huemulsolutions.bigdata

/**
 * Este plan de pruebas valida lo siguiente:
 * Error en PK: hay registros duplicados, lo que se espera es un error de PK
 * el TipodeArchivo usado es Malo01
 */
object Proc_PlanPruebas_PermisosInsert {
  def main(args: Array[String]): Unit = {
    val huemulLib = new huemul_BigDataGovernance("01 - Proc_PlanPruebas_PermisosInsert",args,globalSettings.Global)
    val Control = new huemul_Control(huemulLib,null)
    
    val Ano = huemulLib.arguments.GetValue("ano", null,"Debe especificar ano de proceso: ejemplo: ano=2017")
    val Mes = huemulLib.arguments.GetValue("mes", null,"Debe especificar mes de proceso: ejemplo: mes=12")
    
    val TestPlanGroup: String = huemulLib.arguments.GetValue("TestPlanGroup", null, "Debe especificar el Grupo de Planes de Prueba")
    var IdTestPlan: String = ""
    Control.AddParamInfo("TestPlanGroup", TestPlanGroup)
        
    try {
      Control.NewStep("Define DataFrame Original")
      val DF_RAW =  new raw_DatosBasicos(huemulLib, Control)
      if (!DF_RAW.open("DF_RAW", null, Ano.toInt, Mes.toInt, 1, 0, 0, 0,"Malos01")) {
        Control.RaiseError(s"Error al intentar abrir archivo de datos: ${DF_RAW.Error.ControlError_Message}")
      }
      Control.NewStep("Mapeo de Campos")
      val TablaMaster = new tbl_DatosBasicos(huemulLib, Control)    
      TablaMaster.WhoCanRun_executeOnlyInsert_addAccess("agrega otro", "cualquier clase")
  
      
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "getWhoCanRun_executeOnlyInsert","Pudo agregar acceso", "no Pudo agregar acceso", s"Pudo agregar acceso", false)
      Control.RegisterTestPlanFeature("getWhoCanRun_executeOnlyInsert", IdTestPlan)
      
      
      /////////////////////////////////////////////////////////////////////////////////////////
      /////////////////////////////////////////////////////////////////////////////////////////
      //  I N I C I A   P L A N   D E   P R U E B A S
      /////////////////////////////////////////////////////////////////////////////////////////
      /////////////////////////////////////////////////////////////////////////////////////////
      //valida que respuesta sea negativa
      
      Control.FinishProcessOK
    } catch {
      case e: Exception => 
        
        val IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "getWhoCanRun_executeOnlyInsert", "ERROR DE PROGRAMA -  deberia tener errror", "con error 1033", s"con error: ${Control.Control_Error.ControlError_ErrorCode}", Control.Control_Error.ControlError_ErrorCode == 1033)
        Control.RegisterTestPlanFeature("getWhoCanRun_executeOnlyInsert", IdTestPlan)
        Control.Control_Error.GetError(e, this.getClass.getSimpleName, 1)
        Control.FinishProcessError()
    }
    
    huemulLib.close()
  }
}