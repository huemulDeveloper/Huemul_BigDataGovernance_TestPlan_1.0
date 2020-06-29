package com.huemulsolutions.bigdata.test

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.tables.master.tbl_DatosBasicos
import com.huemulsolutions.bigdata.raw.raw_DatosBasicos
import com.huemulsolutions.bigdata
import org.apache.hadoop.fs.FileSystem
import com.huemulsolutions.bigdata.tables.master.tbl_DatosBasicosInsert
import com.huemulsolutions.bigdata.tables.huemulType_StorageType._
import com.huemulsolutions.bigdata.tables.huemulType_StorageType
import com.huemulsolutions.bigdata.tables.huemul_TableConnector
import com.huemulsolutions.bigdata.tables.huemulType_InternalTableType

object Proc_PlanPruebas_OnlyInsertNew_warning {
  def main(args: Array[String]): Unit = {
    val huemulLib = new huemul_BigDataGovernance("01 - Plan pruebas Proc_PlanPruebas_OnlyInsertNew_warning",args,com.yourcompany.settings.globalSettings.Global)
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
      var IdTestPlan: String = null
      
      Control.NewStep("Define DataFrame Original")
      val DF_RAW =  new raw_DatosBasicos(huemulLib, Control)
      if (!DF_RAW.open("DF_RAW", null, Ano.toInt, Mes.toInt, 1, 0, 0, 0,"")) {
        Control.RaiseError(s"Error al intentar abrir archivo de datos: ${DF_RAW.Error.ControlError_Message}")
      }
      
      Control.NewStep("Mapeo de Campos")
      val TablaMaster = new tbl_DatosBasicosInsert(huemulLib, Control,TipoTabla)      
      TablaMaster.DF_from_DF(DF_RAW.DataFramehuemul.DataFrame,"DF_RAW", "DF_Original")
      
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
      if (!TablaMaster.executeFull("DF_Final_Todo", org.apache.spark.storage.StorageLevel.MEMORY_ONLY)) {
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Masterización", "No hay error en masterización", "No hay error en masterización", s"Si hay error en masterización", false)
        Control.RegisterTestPlanFeature("Requiered OK", IdTestPlan)
        Control.RegisterTestPlanFeature("IsPK", IdTestPlan)
        Control.RegisterTestPlanFeature("StorageType parquet", IdTestPlan)
      
        Control.RaiseError(s"Error al masterizar (${TablaMaster.Error_Code}): ${TablaMaster.Error_Text}")
      } else {
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Masterización", "No hay error en masterización", "No hay error en masterización", s"No hay error en masterización", true)
        Control.RegisterTestPlanFeature("Requiered OK", IdTestPlan)
        Control.RegisterTestPlanFeature("IsPK", IdTestPlan)
        Control.RegisterTestPlanFeature("StorageType parquet", IdTestPlan)
      }
      
      
      
      
      Control.NewStep("Define DataFrame iInsert")
      if (!DF_RAW.open("DF_RAW_2", null, Ano.toInt, Mes.toInt, 1, 0, 0, 0,"Mod")) {
        Control.RaiseError(s"Error al intentar abrir archivo de datos: ${DF_RAW.Error.ControlError_Message}")
      }
      Control.NewStep("Mapeo de Campos")      
      TablaMaster.DF_from_DF(DF_RAW.DataFramehuemul.DataFrame,"DF_RAW_2", "DF_Mod")
      
      //TODO: cambiar el parámetro "true" por algo.UPDATE O algo.NOUPDATE (en replaceValueOnUpdate
      Control.NewStep("PASO 2: SOLO INSERTA 1 REGISTRO, NO MODIFICA NI ELMINA NADA --> registra en DQ")
      if (!TablaMaster.executeOnlyInsert("DF_Final_New", org.apache.spark.storage.StorageLevel.MEMORY_ONLY,true)) {
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Masterización", "No hay error en masterización", "No hay error en masterización", s"Si hay error en masterización", false)
        Control.RegisterTestPlanFeature("Requiered OK", IdTestPlan)
        Control.RegisterTestPlanFeature("IsPK", IdTestPlan)
        Control.RegisterTestPlanFeature("executeOnlyInsert_DQ", IdTestPlan)
        Control.RegisterTestPlanFeature("StorageType parquet", IdTestPlan)
      
        Control.RaiseError(s"Error al masterizar (${TablaMaster.Error_Code}): ${TablaMaster.Error_Text}")
      } else {
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Masterización", "No hay error en masterización", "No hay error en masterización", s"No hay error en masterización", true)
        Control.RegisterTestPlanFeature("Requiered OK", IdTestPlan)
        Control.RegisterTestPlanFeature("IsPK", IdTestPlan)
        Control.RegisterTestPlanFeature("executeOnlyInsert_DQ", IdTestPlan)
        Control.RegisterTestPlanFeature("StorageType parquet", IdTestPlan)
      }
        
      
      val DF_Final = huemulLib.DF_ExecuteQuery("DF_Final", s"""select * from ${TablaMaster.getTable()}  """)
      DF_Final.show()
      
      var ErrorReg = TablaMaster.DataFramehuemul.getDQResult().filter { x => x.DQ_ErrorCode == 1055  }
      var CodWarningInsert: String = ""
      if (ErrorReg == null || ErrorReg.length == 0){
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "WARNING Insert only new", "Warning NO encontrado con codigo 1055", "Warning NO encontrado con codigo 1055", s"Warning NO encontrado con codigo 1055", false)
        Control.RegisterTestPlanFeature("DQ_MinLen", IdTestPlan)
      }
      else {
        val ErrorEncontrado = ErrorReg(0)
        
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR Column_DQ_MinLen Codigo", "Warnings encontrados correctamente", "ErrorCode = 1055, N° registros con error: 1", s"ErrorCode = 1055, N° registros con warning: ${ErrorEncontrado.DQ_NumRowsError}", ErrorEncontrado.DQ_NumRowsError == 1)
        Control.RegisterTestPlanFeature("DQ_MinLen", IdTestPlan)
        
        CodWarningInsert = ErrorEncontrado.DQ_Id
      }
      
      Control.NewStep("DF Plan de pruebas: Nuevos ")
      val DF_Resultado = huemulLib.DF_ExecuteQuery("DF_Resultado", s"""SELECT *
                                                                       FROM ${TablaMaster.getTable_DQ()}
                                                                       WHERE dq_control_id = '${Control.Control_Id}'
                                                                       AND   dq_dq_id = '$CodWarningInsert'""")
      var Cantidad = if (DF_Resultado == null) 0 else DF_Resultado.count()
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Registro en DQ Nuevos - TieneRegistros", "Registro Nuevos, debe tener 1 registro", "Cantidad = 1", s"Cantidad = ${Cantidad}", Cantidad == 1)
      Control.RegisterTestPlanFeature("executeOnlyInsert_DQ", IdTestPlan)
      Control.RegisterTestPlanFeature("StorageType parquet", IdTestPlan)
      Control.RegisterTestPlanFeature("autoCast Encendido", IdTestPlan)
      Control.RegisterTestPlanFeature("InsertOnlyNew_InDQ", IdTestPlan)
      
      val Nuevos_Todos = huemulLib.DF_ExecuteQuery("Nuevos_Todos", s"""SELECT case when BigIntValue = 1000                      then true else false end as Cumple_BigIntValue
                                                                                     ,case when IntValue = 1000                      then true else false end as Cumple_IntValue
                                                                                     ,case when SmallIntValue = 1000                      then true else false end as Cumple_SmallIntValue
                                                                                     ,case when TinyIntValue = 1000                          then true else false end as Cumple_TinyIntValue
                                                                                     ,case when DecimalValue = 1000.1230                     then true else false end as Cumple_DecimalValue
                                                                                     ,case when RealValue = 1000.123                        then true else false end as Cumple_RealValue
                                                                                     ,case when FloatValue = cast(1000.123  as float)        then true else false end as Cumple_FloatValue
                                                                                     ,case when StringValue = "TEXTO XXXXXX"                then true else false end as Cumple_StringValue
                                                                                     ,case when charValue = "X"                             then true else false end as Cumple_charValue
                                                                                     ,case when timeStampValue = "2017-12-30 00:00:00"      then true else false end as Cumple_timeStampValue
                                                                                     ,case when IntValue_old is null and          IntValue_fhChange is null and         IntValue_ProcessLog is not null and 
                                                                                                BigIntValue_old is null and       BigIntValue_fhChange is null and      BigIntValue_ProcessLog is not null and 
                                                                                                SmallIntValue_old is null and     SmallIntValue_fhChange is null and    SmallIntValue_ProcessLog is not null and 
                                                                                                TinyIntValue_old is null and      TinyIntValue_fhChange is null and     TinyIntValue_ProcessLog is not null and 
                                                                                                DecimalValue_old is null and      DecimalValue_fhChange is null and     DecimalValue_ProcessLog is not null and 
                                                                                                RealValue_old is null and         RealValue_fhChange is null and        RealValue_ProcessLog is not null and 
                                                                                                FloatValue_old is null and        FloatValue_fhChange is null and       FloatValue_ProcessLog is not null and 
                                                                                                StringValue_old is null and       StringValue_fhChange is null and      StringValue_ProcessLog is not null and 
                                                                                                charValue_old is null and         charValue_fhChange is null and        charValue_ProcessLog is not null and 
                                                                                                timeStampValue_old is null and    timeStampValue_fhChange is null and   timeStampValue_ProcessLog is not null then true else false end as Cumple_MDM
                                                                               FROM DF_Resultado
      """)
                                                                               //WHERE tipoValor = 'Nuevos'""")
      
      
      //**************************
      //****  C O M P A R A C I O N     N U E V O S  *************
      //**************************
      val Nuevos = Nuevos_Todos.first()
      val BigIntValue =  Nuevos.getAs[Boolean]("Cumple_BigIntValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Nuevos - BigIntValue", "Registro Nuevos, Campo BigIntValue", "Valor = 1000", s"Valor = ??", BigIntValue)
      Control.RegisterTestPlanFeature("executeOnlyInsert_DQ", IdTestPlan)
      val IntValue =  Nuevos.getAs[Boolean]("Cumple_IntValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Nuevos - IntValue", "Registro Nuevos, Campo IntValue", "Valor = 1000", s"Valor = ??", IntValue)
      Control.RegisterTestPlanFeature("executeOnlyInsert_DQ", IdTestPlan)
      val SmallIntValue =  Nuevos.getAs[Boolean]("Cumple_SmallIntValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Nuevos - SmallIntValue", "Registro Nuevos, Campo SmallIntValue", "Valor = 1000", s"Valor = ??", SmallIntValue)
      Control.RegisterTestPlanFeature("executeOnlyInsert_DQ", IdTestPlan)
      val TinyIntValue =  Nuevos.getAs[Boolean]("Cumple_TinyIntValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Nuevos - TinyIntValue", "Registro Nuevos, Campo TinyIntValue", "Valor = 1000", s"Valor = ??", TinyIntValue)
      Control.RegisterTestPlanFeature("executeOnlyInsert_DQ", IdTestPlan)
      val DecimalValue =  Nuevos.getAs[Boolean]("Cumple_DecimalValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Nuevos - DecimalValue", "Registro Nuevos, Campo DecimalValue", s"Valor = 1000.1230", s"Valor = ??", DecimalValue)
      Control.RegisterTestPlanFeature("executeOnlyInsert_DQ", IdTestPlan)
      val RealValue =  Nuevos.getAs[Boolean]("Cumple_RealValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Nuevos - RealValue", "Registro Nuevos, Campo RealValue", "Valor = 1000.1230", s"Valor = ??", RealValue)
      Control.RegisterTestPlanFeature("executeOnlyInsert_DQ", IdTestPlan)
      val FloatValue =  Nuevos.getAs[Boolean]("Cumple_FloatValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Nuevos - FloatValue", "Registro Nuevos, Campo FloatValue", "Valor = 1000.1230", s"Valor = ??", FloatValue)
      Control.RegisterTestPlanFeature("executeOnlyInsert_DQ", IdTestPlan)
      val StringValue =  Nuevos.getAs[Boolean]("Cumple_StringValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Nuevos - StringValue", "Registro Nuevos, Campo StringValue", "Valor = TEXTO XXXXXX", s"Valor = ??", StringValue)
      Control.RegisterTestPlanFeature("executeOnlyInsert_DQ", IdTestPlan)
      val charValue =  Nuevos.getAs[Boolean]("Cumple_charValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Nuevos - charValue", "Registro Nuevos, Campo charValue", "Valor = X", s"Valor = ??", charValue)
      Control.RegisterTestPlanFeature("executeOnlyInsert_DQ", IdTestPlan)
      val timeStampValue =  Nuevos.getAs[Boolean]("Cumple_timeStampValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Nuevos - timeStampValue", "Registro Nuevos, Campo timeStampValue", "Valor = '2017-12-31 00:00:00.0'", s"Valor = ??", timeStampValue)
      Control.RegisterTestPlanFeature("executeOnlyInsert_DQ", IdTestPlan)
      val MDM =  Nuevos.getAs[Boolean]("Cumple_MDM")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Nuevos - MDM", "Registro Nuevos, Campo MDM", "Valor = true", s"Valor = ${MDM}", MDM)
      Control.RegisterTestPlanFeature("MDM_EnableDTLog", IdTestPlan)
      Control.RegisterTestPlanFeature("MDM_EnableOldValue", IdTestPlan)
      Control.RegisterTestPlanFeature("MDM_EnableProcessLog", IdTestPlan)
      /////////////////////////////////////////////////////////////////////////////////////////
      /////////////////////////////////////////////////////////////////////////////////////////
      //  I N I C I A   P L A N   D E   P R U E B A S
      /////////////////////////////////////////////////////////////////////////////////////////
      /////////////////////////////////////////////////////////////////////////////////////////
      Control.NewStep("Muestra de los datos ")
      TablaMaster.DataFramehuemul.DataFrame.show()
      
      
      
          Control.FinishProcessOK
    } catch {
      case e: Exception => 
        val IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR", "ERROR DE PROGRAMA -  no deberia tener errror", "sin error", s"con error: ${e.getMessage}", false)
        Control.RegisterTestPlanFeature("executeOnlyInsert_DQ", IdTestPlan)
        Control.Control_Error.GetError(e, this.getClass.getSimpleName, 1)
        Control.FinishProcessError()
    }
    
    if (Control.TestPlan_CurrentIsOK(null))
      println("Proceso OK")
    
    huemulLib.close()
  }
}