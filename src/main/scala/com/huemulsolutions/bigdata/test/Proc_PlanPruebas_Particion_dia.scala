package com.huemulsolutions.bigdata.test

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.raw.raw_DatosParticion
import com.huemulsolutions.bigdata.tables.huemulType_StorageType
import com.huemulsolutions.bigdata.tables.huemulType_StorageType.huemulType_StorageType
import com.huemulsolutions.bigdata.tables.master.tbl_DatosParticion

/**
 * Este plan de pruebas valida lo siguiente:
 * Error en PK: hay registros duplicados, lo que se espera es un error de PK
 * el TipodeArchivo usado es Malo01
 */
object Proc_PlanPruebas_Particion_dia {
  def main(args: Array[String]): Unit = {
    processMaster(null,args)
  }

  def processMaster(huemulLib2: huemul_BigDataGovernance, args: Array[String]): huemul_Control = {
    val huemulLib = if (huemulLib2 == null) new huemul_BigDataGovernance("01 - Proc_PlanPruebas_Particion_dia",args,com.yourcompany.settings.globalSettings.Global) else huemulLib2
    val Control = new huemul_Control(huemulLib,null, huemulType_Frequency.MONTHLY)

    /*
    if (huemulLib.GlobalSettings.getBigDataProvider() == huemulType_bigDataPProc_PlanPruebas_CargaNoTrimrovider.databricks) {
      huemulLib.spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")
    }

     */
    huemulLib.arguments.setArgs(args)
    val Ano = huemulLib.arguments.GetValue("ano", null,"Debe especificar ano de proceso: ejemplo: ano=2017")
    val Mes = huemulLib.arguments.GetValue("mes", null,"Debe especificar mes de proceso: ejemplo: mes=12")
    val dia  = huemulLib.arguments.GetValue("dia", null,"Debe especificar dia de proceso: ejemplo: dia=1")
    val empresa = huemulLib.arguments.GetValue("empresa", null,"Debe especificar una empresa, ejemplo: empresa=super-01")
    val TipoTablaParam: String = huemulLib.arguments.GetValue("TipoTabla", null, "Debe especificar TipoTabla (ORC,PARQUET,HBASE,DELTA)")
    var TipoTabla: huemulType_StorageType = null
    if (TipoTablaParam == "orc")
      TipoTabla = huemulType_StorageType.ORC
    else if (TipoTablaParam == "parquet")
      TipoTabla = huemulType_StorageType.PARQUET
    else if (TipoTablaParam == "delta")
      TipoTabla = huemulType_StorageType.DELTA
    else if (TipoTablaParam == "hbase")
      TipoTabla = huemulType_StorageType.PARQUET
    else if (TipoTablaParam == "avro")
      TipoTabla = huemulType_StorageType.AVRO

    //val TestPlanGroup: String = huemulLib.arguments.GetValue("TestPlanGroup", null, "Debe especificar el Grupo de Planes de Prueba")
    //var IdTestPlan: String = ""
    //Control.AddParamInformation("TestPlanGroup", TestPlanGroup)
        
    try {
      Control.NewStep("Define DataFrame Original")
      val DF_RAW =  new raw_DatosParticion(huemulLib, Control)
      if (!DF_RAW.open("DF_RAW", null, Ano.toInt, Mes.toInt, dia.toInt, 0, 0, 0,empresa)) {
        Control.RaiseError(s"Error al intentar abrir archivo de datos: ${DF_RAW.Error.ControlError_Message}")
      }
      Control.NewStep("Mapeo de Campos")
      val TablaMaster = new tbl_DatosParticion(huemulLib, Control, TipoTabla)

      TablaMaster.DF_from_SQL("df_data",
        """SELECT to_date(periodo,'yyyyMMdd') as periodo,
              empresa,
              app,
              producto,
              cantidad,
              precio,
              idTx
              FROM DF_RAW
          """)

      TablaMaster.periodo.SetMapping("periodo")
      TablaMaster.empresa.SetMapping("empresa")
      TablaMaster.app.SetMapping("app")
      TablaMaster.producto.SetMapping("producto")
      TablaMaster.cantidad.SetMapping("cantidad")
      TablaMaster.precio.SetMapping("precio")
      TablaMaster.idTx.SetMapping("idTx")

      if (!TablaMaster.executeFull("DF_FinalParticion")) {
        Control.RaiseError("Error al masterizar")
        println("error al masterizar")
      }

  
      
      //IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "getWhoCanRun_executeOnlyInsert","Pudo agregar acceso", "no Pudo agregar acceso", s"Pudo agregar acceso", false)
      //Control.RegisterTestPlanFeature("getWhoCanRun_executeOnlyInsert", IdTestPlan)
      
      
      /////////////////////////////////////////////////////////////////////////////////////////
      /////////////////////////////////////////////////////////////////////////////////////////
      //  I N I C I A   P L A N   D E   P R U E B A S
      /////////////////////////////////////////////////////////////////////////////////////////
      /////////////////////////////////////////////////////////////////////////////////////////
      //valida que respuesta sea negativa
      
      Control.FinishProcessOK
    } catch {
      case e: Exception => 
        
        ///val IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "getWhoCanRun_executeOnlyInsert", "ERROR DE PROGRAMA -  deberia tener errror", "con error 1033", s"con error: ${Control.Control_Error.ControlError_ErrorCode}", Control.Control_Error.ControlError_ErrorCode == 1033)
        //Control.RegisterTestPlanFeature("getWhoCanRun_executeOnlyInsert", IdTestPlan)
        Control.Control_Error.GetError(e, this.getClass.getSimpleName, Control.Control_Error.ControlError_ErrorCode)
        Control.FinishProcessError()
    }
    
    //if (Control.TestPlan_CurrentIsOK(null))
    //  println("Proceso OK")
    
    //huemulLib.close()

    Control
  }
}