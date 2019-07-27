package com.huemulsolutions.bigdata.test

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.tables.master.tbl_DatosBasicos
import com.huemulsolutions.bigdata.raw.raw_DatosBasicos
import com.huemulsolutions.bigdata
import org.apache.hadoop.fs.FileSystem
import com.huemulsolutions.bigdata.tables.master.tbl_DatosBasicosErrores


object Proc_PlanPruebas_Errores {
  def main(args: Array[String]): Unit = {
    val huemulLib = new huemul_BigDataGovernance("01 - Plan pruebas Proc_PlanPruebas_CargaMaster",args,globalSettings.Global)
    val Control = new huemul_Control(huemulLib,null, huemulType_Frequency.MONTHLY)
    
    val Ano = huemulLib.arguments.GetValue("ano", null,"Debe especificar ano de proceso: ejemplo: ano=2017")
    val Mes = huemulLib.arguments.GetValue("mes", null,"Debe especificar mes de proceso: ejemplo: mes=12")
    
    val TestPlanGroup: String = huemulLib.arguments.GetValue("TestPlanGroup", null, "Debe especificar el Grupo de Planes de Prueba")

    Control.AddParamInformation("TestPlanGroup", TestPlanGroup)
        
    try {
      Control.NewStep("Define DataFrame Original")
      val DF_RAW =  new raw_DatosBasicos(huemulLib, Control)
      if (!DF_RAW.open("DF_RAW", null, Ano.toInt, Mes.toInt, 1, 0, 0, 0,"")) {
        Control.RaiseError(s"Error al intentar abrir archivo de datos: ${DF_RAW.Error.ControlError_Message}")
      }
      Control.NewStep("Mapeo de Campos")
      val TablaMaster = new tbl_DatosBasicosErrores(huemulLib, Control)      
      TablaMaster.DF_from_DF(DF_RAW.DataFramehuemul.DataFrame, "DF_RAW", "DF_Original")
      
   //BORRA HDFS ANTIGUO PARA EFECTOS DEL PLAN DE PRUEBAS
      val a = huemulLib.spark.catalog.listTables(TablaMaster.getCurrentDataBase()).collect()
      if (a.filter { x => x.name.toUpperCase() == TablaMaster.TableName.toUpperCase()  }.length > 0) {
        huemulLib.spark.sql(s"drop table if exists ${TablaMaster.getTable()} ")
      } 
      
      val FullPath = new org.apache.hadoop.fs.Path(s"${TablaMaster.getFullNameWithPath()}")
      val fs = FileSystem.get(huemulLib.spark.sparkContext.hadoopConfiguration) 
      if (fs.exists(FullPath))
        fs.delete(FullPath, true)
        
   //BORRA HDFS ANTIGUO PARA EFECTOS DEL PLAN DE PRUEBAS
        
      TablaMaster.TipoValor.SetMapping("TipoValor",true,"coalesce(new.TipoValor,'nulo')","coalesce(new.TipoValor,'nulo')")
      TablaMaster.Column_DQ_MaxDateTimeValue.SetMapping("timeStampValue")
      TablaMaster.Column_DQ_MaxDecimalValue.SetMapping("DecimalValue")
      TablaMaster.Column_DQ_MaxLen.SetMapping("StringValue")
      TablaMaster.Column_DQ_MinDateTimeValue.SetMapping("timeStampValue")
      TablaMaster.Column_DQ_MinDecimalValue.SetMapping("DecimalValue")
      TablaMaster.Column_DQ_MinLen.SetMapping("StringValue")
      TablaMaster.Column_IsUnique.SetMapping("StringValue")
      TablaMaster.Column_NoMapeadoDefault.SetMapping("")
      TablaMaster.Column_NotNull.SetMapping("IntValue")
      
      
      huemulLib.spark.sql("select StringValue, length(StringValue) as largo, case when StringValue is null then 1 else 0 end esNulo from DF_Original").show()
      //TODO: cambiar el parámetro "true" por algo.UPDATE O algo.NOUPDATE (en replaceValueOnUpdate
      Control.NewStep("Ejecución")
      TablaMaster.executeFull("DF_Final", org.apache.spark.storage.StorageLevel.MEMORY_ONLY_SER)
      var IdTestPlan: String = ""
      //Column_DQ_MinLen
      var ErrorReg = TablaMaster.DataFramehuemul.getDQResult().filter { x => x.ColumnName == "Column_DQ_MinLen" && x.DQ_ErrorCode == 1020 }
      if (ErrorReg == null || ErrorReg.length == 0){
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR Column_DQ_MinLen", "No hay error encontrado", "ErrorCode = 1020", s"SIN REGISTRO", false)
        Control.RegisterTestPlanFeature("DQ_MinLen", IdTestPlan)
      }
      else {
        val ErrorEncontrado = ErrorReg(0)
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR Column_DQ_MinLen Codigo", "Errores encontrados correctamente", "ErrorCode = 1020, N° registros con error: 3", s"ErrorCode = 1020, N° registros con error: ${ErrorEncontrado.DQ_NumRowsError}", ErrorEncontrado.DQ_NumRowsError == 3)
        Control.RegisterTestPlanFeature("DQ_MinLen", IdTestPlan)
      }
      
      //Column_DQ_MaxLen
      ErrorReg = TablaMaster.DataFramehuemul.getDQResult().filter { x => x.ColumnName == "Column_DQ_MaxLen" && x.DQ_ErrorCode == 1020 }
      if (ErrorReg == null || ErrorReg.length == 0){
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR Column_DQ_MaxLen", "No hay error encontrado", "ErrorCode = 1020", s"SIN REGISTRO", false)
        Control.RegisterTestPlanFeature("DQ_MaxLen", IdTestPlan)
      }
      else {
        val ErrorEncontrado = ErrorReg(0)
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR Column_DQ_MaxLen Codigo", "Errores encontrados correctamente", "ErrorCode = 1020, N° registros con error: 2", s"ErrorCode = 1020, N° registros con error: ${ErrorEncontrado.DQ_NumRowsError}", ErrorEncontrado.DQ_NumRowsError == 2)
        Control.RegisterTestPlanFeature("DQ_MaxLen", IdTestPlan)
      }
      
      //Column_DQ_MinDecimalValue
      ErrorReg = TablaMaster.DataFramehuemul.getDQResult().filter { x => x.ColumnName == "Column_DQ_MinDecimalValue" && x.DQ_ErrorCode == 1021 }
      if (ErrorReg == null || ErrorReg.length == 0){
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR Column_DQ_MinDecimalValue", "No hay error encontrado", "ErrorCode = 1021", s"SIN REGISTRO", false)
        Control.RegisterTestPlanFeature("DQ_MinDecimalValue", IdTestPlan)
      }
      else {
        val ErrorEncontrado = ErrorReg(0)
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR Column_DQ_MinDecimalValue Codigo", "Errores encontrados correctamente", "ErrorCode = 1021, N° registros con error: 2", s"ErrorCode = 1021, N° registros con error: ${ErrorEncontrado.DQ_NumRowsError}", ErrorEncontrado.DQ_NumRowsError == 2)
        Control.RegisterTestPlanFeature("DQ_MinDecimalValue", IdTestPlan)
      }
      
      //Column_DQ_MaxDecimalValue
      ErrorReg = TablaMaster.DataFramehuemul.getDQResult().filter { x => x.ColumnName == "Column_DQ_MaxDecimalValue" && x.DQ_ErrorCode == 1021 }
      if (ErrorReg == null || ErrorReg.length == 0){
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR Column_DQ_MaxDecimalValue", "No hay error encontrado", "ErrorCode = 1021", s"SIN REGISTRO", false)
        Control.RegisterTestPlanFeature("DQ_MaxDecimalValue", IdTestPlan)
      }
      else {
        val ErrorEncontrado = ErrorReg(0)
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR Column_DQ_MaxDecimalValue Codigo", "Errores encontrados correctamente", "ErrorCode = 1021, N° registros con error: 1", s"ErrorCode = 1021, N° registros con error: ${ErrorEncontrado.DQ_NumRowsError}", ErrorEncontrado.DQ_NumRowsError == 1)
        Control.RegisterTestPlanFeature("DQ_MaxDecimalValue", IdTestPlan)
      }
      
      //Column_DQ_MinDateTimeValue
      ErrorReg = TablaMaster.DataFramehuemul.getDQResult().filter { x => x.ColumnName == "Column_DQ_MinDateTimeValue" && x.DQ_ErrorCode == 1022 }
      if (ErrorReg == null || ErrorReg.length == 0){
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR Column_DQ_MinDateTimeValue", "No hay error encontrado", "ErrorCode = 1022", s"SIN REGISTRO", false)
        Control.RegisterTestPlanFeature("DQ_MinDateTimeValue", IdTestPlan)
      }
      else {
        val ErrorEncontrado = ErrorReg(0)
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR Column_DQ_MinDateTimeValue Codigo", "Errores encontrados correctamente", "ErrorCode = 1022, N° registros con error: 3", s"ErrorCode = 1022, N° registros con error: ${ErrorEncontrado.DQ_NumRowsError}", ErrorEncontrado.DQ_NumRowsError == 3)
        Control.RegisterTestPlanFeature("DQ_MinDateTimeValue", IdTestPlan)
      }
      
      //Column_DQ_MaxDateTimeValue
      ErrorReg = TablaMaster.DataFramehuemul.getDQResult().filter { x => x.ColumnName == "Column_DQ_MaxDateTimeValue" && x.DQ_ErrorCode == 1022 }
      if (ErrorReg == null || ErrorReg.length == 0){
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR Column_DQ_MaxDateTimeValue", "No hay error encontrado", "ErrorCode = 1022", s"SIN REGISTRO", false)
        Control.RegisterTestPlanFeature("DQ_MaxDateTimeValue", IdTestPlan)
      }
      else {
        val ErrorEncontrado = ErrorReg(0)
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR Column_DQ_MaxDateTimeValue Codigo", "Errores encontrados correctamente", "ErrorCode = 1022, N° registros con error: 2", s"ErrorCode = 1022, N° registros con error: ${ErrorEncontrado.DQ_NumRowsError}", ErrorEncontrado.DQ_NumRowsError == 2)
        Control.RegisterTestPlanFeature("DQ_MaxDateTimeValue", IdTestPlan)
      }
      
      //Column_NotNull
      ErrorReg = TablaMaster.DataFramehuemul.getDQResult().filter { x => x.ColumnName == "Column_NotNull" && x.DQ_ErrorCode == 1023 }
      if (ErrorReg == null || ErrorReg.length == 0) {
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR Column_NotNull", "No hay error encontrado", "ErrorCode = 1023", s"SIN REGISTRO", false)
        Control.RegisterTestPlanFeature("Nullable", IdTestPlan)
      }
      else {
        val ErrorEncontrado = ErrorReg(0)
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR Column_NotNull Codigo", "Errores encontrados correctamente", "ErrorCode = 1023, N° registros con error: 1", s"ErrorCode = 1023, N° registros con error: ${ErrorEncontrado.DQ_NumRowsError}", ErrorEncontrado.DQ_NumRowsError == 1)
        Control.RegisterTestPlanFeature("Nullable", IdTestPlan)
      }
      
      //Column_IsUnique
      ErrorReg = TablaMaster.DataFramehuemul.getDQResult().filter { x => x.ColumnName == "Column_IsUnique" && x.DQ_ErrorCode == 2006 }
      if (ErrorReg == null || ErrorReg.length == 0){
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR Column_IsUnique", "No hay error encontrado", "ErrorCode = 2006", s"SIN REGISTRO", false)
        Control.RegisterTestPlanFeature("IsUnique", IdTestPlan)
      }
      else {
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR Column_IsUnique Codigo", "Errores encontrados correctamente", "ErrorCode = 2006, N° errores = 1", s"ErrorCode = 2006, N° errores = ${ErrorReg.length}", ErrorReg.length == 1)
        Control.RegisterTestPlanFeature("IsUnique", IdTestPlan)
        /*
         * comentado, descomentar cuando se agregue la captura de los registros que no fueron únicos.
        val ErrorEncontrado = ErrorReg(0)
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR Column_IsUnique Codigo", "Errores encontrados correctamente", "ErrorCode = 2006, N° registros Not Unique: 2", s"ErrorCode = 2006, N° registros con error: ${ErrorEncontrado.DQ_NumRowsError}", ErrorEncontrado.DQ_NumRowsError == 2)
        Control.RegisterTestPlanFeature("IsUnique", IdTestPlan)
        * 
        */
      }
      
      //N° Total de errores por DQ debe ser = 8
      ErrorReg = TablaMaster.DataFramehuemul.getDQResult().filter { x => x.DQ_IsError }
      if (ErrorReg == null || ErrorReg.length == 0){
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR N° DQ fallados", "No hay error encontrado", "Cantidad de errores DQ = 8", s"Cantidad de errores DQ = ${ErrorReg.length}", false)
        Control.RegisterTestPlanFeature("executeFull", IdTestPlan)
      }
      else {
        val ErrorEncontrado = ErrorReg.length
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR N° DQ Fallados cod", "Errores encontrados correctamente", "Cantidad de errores DQ = 8", s"Cantidad de errores DQ = ${ErrorEncontrado}", ErrorEncontrado == 8)
        Control.RegisterTestPlanFeature("executeFull", IdTestPlan)
      }
      
      println(s"detalle del 100% de DQ aplicado en esta prueba")
      TablaMaster.DataFramehuemul.getDQResult().foreach { x => 
        println(s"DQ_Name:[${x.DQ_Name}], ErrorCode:${x.DQ_ErrorCode} BBDD_Name:${x.BBDD_Name}, Table_Name:${x.Table_Name}, ColumnName:${x.ColumnName}, DQ_NumRowsTotal:${x.DQ_NumRowsTotal}, DQ_NumRowsOK:${x.DQ_NumRowsOK}, DQ_NumRowsError:${x.DQ_NumRowsError}")
      }
      
      val errores = huemulLib.spark.sql(s"select dq_error_columnname, cast(count(1) as int) as Cantidad from production_dqerror.tbl_datosbasicoserrores_dq where dq_control_id = '${Control.Control_Id}' group by dq_error_columnname ").collect
      var cantidad = errores.filter { x => x.getAs[String]("dq_error_columnname") == "Column_DQ_MaxDateTimeValue" }(0).getAs[Int]("Cantidad") 
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Guarda errores en tabla _dq Column_DQ_MaxDateTimeValue", "errores en columna Column_DQ_MaxDateTimeValue", "Cantidad con errores = 2", s"Cantidad con errores = ${cantidad}", cantidad == 2)
      Control.RegisterTestPlanFeature("ControlErrores", IdTestPlan)
      
      cantidad = errores.filter { x => x.getAs[String]("dq_error_columnname") == "Column_DQ_MaxDecimalValue" }(0).getAs[Int]("Cantidad") 
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Guarda errores en tabla _dq Column_DQ_MaxDecimalValue", "errores en columna Column_DQ_MaxDecimalValue", "Cantidad con errores = 1", s"Cantidad con errores = ${cantidad}", cantidad == 1)
      Control.RegisterTestPlanFeature("ControlErrores", IdTestPlan)
      
      cantidad = errores.filter { x => x.getAs[String]("dq_error_columnname") == "Column_DQ_MaxLen" }(0).getAs[Int]("Cantidad") 
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Guarda errores en tabla _dq Column_DQ_MaxLen", "errores en columna Column_DQ_MaxLen", "Cantidad con errores = 2", s"Cantidad con errores = ${cantidad}", cantidad == 2)
      Control.RegisterTestPlanFeature("ControlErrores", IdTestPlan)

      cantidad = errores.filter { x => x.getAs[String]("dq_error_columnname") == "Column_DQ_MinDateTimeValue" }(0).getAs[Int]("Cantidad") 
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Guarda errores en tabla _dq Column_DQ_MinDateTimeValue", "errores en columna Column_DQ_MinDateTimeValue", "Cantidad con errores = 3", s"Cantidad con errores = ${cantidad}", cantidad == 3)
      Control.RegisterTestPlanFeature("ControlErrores", IdTestPlan)

      cantidad = errores.filter { x => x.getAs[String]("dq_error_columnname") == "Column_DQ_MinDecimalValue" }(0).getAs[Int]("Cantidad") 
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Guarda errores en tabla _dq Column_DQ_MinDecimalValue", "errores en columna Column_DQ_MinDecimalValue", "Cantidad con errores = 2", s"Cantidad con errores = ${cantidad}", cantidad == 2)
      Control.RegisterTestPlanFeature("ControlErrores", IdTestPlan)

      cantidad = errores.filter { x => x.getAs[String]("dq_error_columnname") == "Column_DQ_MinLen" }(0).getAs[Int]("Cantidad") 
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Guarda errores en tabla _dq Column_DQ_MinLen", "errores en columna Column_DQ_MinLen", "Cantidad con errores = 3", s"Cantidad con errores = ${cantidad}", cantidad == 3)
      Control.RegisterTestPlanFeature("ControlErrores", IdTestPlan)

      cantidad = errores.filter { x => x.getAs[String]("dq_error_columnname") == "Column_NotNull" }(0).getAs[Int]("Cantidad") 
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Guarda errores en tabla _dq Column_NotNull", "errores en columna Column_NotNull", "Cantidad con errores = 1", s"Cantidad con errores = ${cantidad}", cantidad == 1)
      Control.RegisterTestPlanFeature("ControlErrores", IdTestPlan)

      
          Control.FinishProcessOK
    } catch {
      case e: Exception => 
        val IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR", "ERROR DE PROGRAMA -  no deberia tener errror", "sin error", s"con error: ${e.getMessage}", false)
        Control.RegisterTestPlanFeature("executeFull", IdTestPlan)
        Control.Control_Error.GetError(e, this.getClass.getSimpleName, 1)
        Control.FinishProcessError()
    }
    
    if (Control.TestPlan_CurrentIsOK(16))
      println("Proceso OK")
      
    huemulLib.close()
  }
}