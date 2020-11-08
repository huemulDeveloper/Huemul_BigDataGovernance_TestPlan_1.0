package com.huemulsolutions.bigdata.test

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.raw.raw_DatosBasicos
//import org.apache.hadoop.fs.FileSystem
import com.huemulsolutions.bigdata.dataquality.huemul_DataQuality
import com.huemulsolutions.bigdata.dataquality.huemulType_DQQueryLevel
import com.huemulsolutions.bigdata.dataquality.huemulType_DQNotification
import scala.collection.mutable._
import org.apache.spark.sql.types._
import com.huemulsolutions.bigdata.tables.master.tbl_DatosBasicos_mes
import org.apache.spark.sql.functions._
import com.huemulsolutions.bigdata.tables.huemulType_StorageType._
import com.huemulsolutions.bigdata.tables.huemulType_StorageType
import com.huemulsolutions.bigdata.tables.huemul_TableConnector
import com.huemulsolutions.bigdata.tables.huemulType_InternalTableType

//OJO:
//el resultado de suma de double con valores nulos da distinto a sumar los valores sin nulos (el decimal 15 da diferente)
//por otro lado, un float cuando se compara linea a linea con un decimal el resultado es distinto.
//la recomendación es usar solo datos decimal.

object Proc_PlanPruebas_CargaMaster_mes {
  def main(args: Array[String]): Unit = {
    val huemulLib = new huemul_BigDataGovernance("01 - Plan pruebas Proc_PlanPruebas_CargaMaster_mes",args,com.yourcompany.settings.globalSettings.Global)
    val Control = new huemul_Control(huemulLib,null, huemulType_Frequency.MONTHLY)
    
    val Ano = "2018"//huemulLib.arguments.GetValue("ano", null,"Debe especificar ano de proceso: ejemplo: ano=2017")
    val Mes = "09"//huemulLib.arguments.GetValue("mes", null,"Debe especificar mes de proceso: ejemplo: mes=12")
    
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
      val TablaMaster = new tbl_DatosBasicos_mes(huemulLib, Control, TipoTabla)
      
      val periodo_mes = huemulLib.ReplaceWithParams("{{YYYY}}-{{MM}}-{{DD}}", Ano.toInt, Mes.toInt, 1, 0, 0, 0, null)
      val df_final = DF_RAW.DataFramehuemul.DataFrame.withColumn("periodo_mes", lit(periodo_mes))
      TablaMaster.DF_from_DF(df_final, "DF_RAW", "DF_Original")
      
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
      
      TablaMaster.periodo_mes.setMapping("periodo_mes")
      TablaMaster.TipoValor.setMapping("TipoValor",true,"coalesce(new.TipoValor,'nulo')","coalesce(new.TipoValor,'nulo')")
      TablaMaster.IntValue.setMapping("IntValue")
      TablaMaster.BigIntValue.setMapping("BigIntValue")
      TablaMaster.SmallIntValue.setMapping("SmallIntValue")
      TablaMaster.TinyIntValue.setMapping("TinyIntValue")
      TablaMaster.DecimalValue.setMapping("DecimalValue")
      TablaMaster.RealValue.setMapping("RealValue")
      TablaMaster.FloatValue.setMapping("FloatValue")
      TablaMaster.StringValue.setMapping("StringValue")
      TablaMaster.charValue.setMapping("charValue")
      TablaMaster.timeStampValue.setMapping("timeStampValue")
      //TODO: cambiar el parámetro "true" por algo.UPDATE O algo.NOUPDATE (en replaceValueOnUpdate
      Control.NewStep("Ejecución")
      if (!TablaMaster.executeFull("DF_Final")) {
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Masterización", "No hay error en masterización", "No hay error en masterización", s"Si hay error en masterización", false)
        Control.RegisterTestPlanFeature("Requiered OK", IdTestPlan)
        Control.RegisterTestPlanFeature("IsPK", IdTestPlan)
        Control.RegisterTestPlanFeature("executeFull", IdTestPlan)
        Control.RegisterTestPlanFeature("StorageType parquet", IdTestPlan)
      
        Control.RaiseError(s"Error al masterizar (${TablaMaster.Error_Code}): ${TablaMaster.Error_Text}")
      } else {
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Masterización", "No hay error en masterización", "No hay error en masterización", s"No hay error en masterización", true)
        Control.RegisterTestPlanFeature("Requiered OK", IdTestPlan)
        Control.RegisterTestPlanFeature("IsPK", IdTestPlan)
        Control.RegisterTestPlanFeature("executeFull", IdTestPlan)
        Control.RegisterTestPlanFeature("StorageType parquet", IdTestPlan)
      }
        
      
      /////////////////////////////////////////////////////////////////////////////////////////
      /////////////////////////////////////////////////////////////////////////////////////////
      //  I N I C I A   P L A N   D E   P R U E B A S
      /////////////////////////////////////////////////////////////////////////////////////////
      /////////////////////////////////////////////////////////////////////////////////////////
      Control.NewStep("Muestra de los datos ")
      TablaMaster.DataFramehuemul.DataFrame.show()
      
      Control.NewStep("DF Plan de pruebas: Cero-Vacio ")
      val Cero_Vacio_Todos = huemulLib.DF_ExecuteQuery("Cero_Vacio_Todos", s"""SELECT case when BigIntValue = 0                           then true else false end as Cumple_BigIntValue
                                                                                     ,case when IntValue = 0                         then true else false end as Cumple_IntValue
                                                                                     ,case when SmallIntValue = 0                         then true else false end as Cumple_SmallIntValue
                                                                                     ,case when TinyIntValue = 0                          then true else false end as Cumple_TinyIntValue
                                                                                     ,case when DecimalValue = 0.0000                     then true else false end as Cumple_DecimalValue
                                                                                     ,case when RealValue = 0.0000                        then true else false end as Cumple_RealValue
                                                                                     ,case when FloatValue = 0.0000                       then true else false end as Cumple_FloatValue
                                                                                     ,case when StringValue = ""                          then true else false end as Cumple_StringValue
                                                                                     ,case when charValue = ""                            then true else false end as Cumple_charValue
                                                                                     ,case when timeStampValue = "1900-01-01 00:00:00.0" or timeStampValue = "1900-01-01 00:00:00"  then true else false end as Cumple_timeStampValue
                                                                               FROM DF_Final
                                                                               WHERE tipoValor = 'Cero-Vacio'""")
      
      var Cantidad: Long = if (Cero_Vacio_Todos == null) 0 else Cero_Vacio_Todos.count()
      
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Cero_Vacio - TieneRegistros", "Registro Cero_Vacio, debe tener 1 registro", "Cantidad = 1", s"Cantidad = ${Cantidad}", Cantidad == 1)
      Control.RegisterTestPlanFeature("executeFull", IdTestPlan)
      Control.RegisterTestPlanFeature("StorageType parquet", IdTestPlan)
      Control.RegisterTestPlanFeature("autoCast Encendido", IdTestPlan)
      Control.RegisterTestPlanFeature("IsPK", IdTestPlan)
      Control.RegisterTestPlanFeature("RAW - realiza trim", IdTestPlan)
      val Cero_Vacio = Cero_Vacio_Todos.first()
      
      Control.NewStep("DF Plan de pruebas: Negativo_Maximo ")
      val Negativo_Maximo_Todos = huemulLib.DF_ExecuteQuery("Negativo_Maximo_Todos", s"""SELECT case when BigIntValue = -10                      then true else false end as Cumple_BigIntValue
                                                                                     ,case when IntValue = -10                         then true else false end as Cumple_IntValue
                                                                                     ,case when SmallIntValue = -10                         then true else false end as Cumple_SmallIntValue
                                                                                     ,case when TinyIntValue = -10                          then true else false end as Cumple_TinyIntValue
                                                                                     ,case when DecimalValue = -10.1230                     then true else false end as Cumple_DecimalValue
                                                                                     ,case when RealValue = -10.123                         then true else false end as Cumple_RealValue
                                                                                     ,case when FloatValue = cast(-10.123 as float)         then true else false end as Cumple_FloatValue
                                                                                     ,case when StringValue = "TEXTO ZZZZZZ"                then true else false end as Cumple_StringValue
                                                                                     ,case when charValue = "z"                             then true else false end as Cumple_charValue
                                                                                     ,case when timeStampValue = "2017-12-31 00:00:00"      then true else false end as Cumple_timeStampValue
                                                                                     ,FloatValue - -cast(10.123 as float) as res
                                                                                     ,FloatValue 
                                                                               FROM DF_Final
                                                                               WHERE tipoValor = 'Negativo_Maximo'""")
      Cantidad = if (Negativo_Maximo_Todos == null) 0 else Negativo_Maximo_Todos.count()
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Negativo_Maximo - TieneRegistros", "Registro Negativo_Maximo, debe tener 1 registro", "Cantidad = 1", s"Cantidad = $Cantidad", Cantidad == 1)
      Control.RegisterTestPlanFeature("executeFull", IdTestPlan)
      Control.RegisterTestPlanFeature("StorageType parquet", IdTestPlan)
      Control.RegisterTestPlanFeature("autoCast Encendido", IdTestPlan)      
      Control.RegisterTestPlanFeature("IsPK", IdTestPlan)
      Control.RegisterTestPlanFeature("RAW - realiza trim", IdTestPlan)
      val Negativo_Maximo = Negativo_Maximo_Todos.first()
      
      Control.NewStep("DF Plan de pruebas: Negativo_Minimo ")
      val Negativo_Minimo_Todos = huemulLib.DF_ExecuteQuery("Negativo_Minimo_Todos", s"""SELECT case when BigIntValue = -100                      then true else false end as Cumple_BigIntValue
                                                                                     ,case when IntValue = -100                         then true else false end as Cumple_IntValue
                                                                                     ,case when SmallIntValue = -100                         then true else false end as Cumple_SmallIntValue
                                                                                     ,case when TinyIntValue = -100                          then true else false end as Cumple_TinyIntValue
                                                                                     ,case when DecimalValue = -100.1230                     then true else false end as Cumple_DecimalValue
                                                                                     ,case when RealValue = -100.123                        then true else false end as Cumple_RealValue
                                                                                     ,case when FloatValue = cast(-100.123 as float)        then true else false end as Cumple_FloatValue
                                                                                     ,case when StringValue = "TEXTO AA"                    then true else false end as Cumple_StringValue
                                                                                     ,case when charValue = "a"                             then true else false end as Cumple_charValue
                                                                                     ,case when timeStampValue = "2017-01-01 00:00:00"      then true else false end as Cumple_timeStampValue
                                                                               FROM DF_Final
                                                                               WHERE tipoValor = 'Negativo_Minimo'""")
      Cantidad = if (Negativo_Minimo_Todos == null) 0 else Negativo_Minimo_Todos.count()
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Negativo_Minimo - TieneRegistros", "Registro Negativo_Minimo, debe tener 1 registro", "Cantidad = 1", s"Cantidad = ${Cantidad}", Cantidad == 1)
      Control.RegisterTestPlanFeature("executeFull", IdTestPlan)
      Control.RegisterTestPlanFeature("StorageType parquet", IdTestPlan)
      Control.RegisterTestPlanFeature("autoCast Encendido", IdTestPlan)
      Control.RegisterTestPlanFeature("IsPK", IdTestPlan)
      Control.RegisterTestPlanFeature("RAW - realiza trim", IdTestPlan)
      val Negativo_Minimo = Negativo_Minimo_Todos.first()
      
      Control.NewStep("DF Plan de pruebas: Positivo_Minimo ")
      val Positivo_Minimo_Todos = huemulLib.DF_ExecuteQuery("Positivo_Minimo_Todos", s"""SELECT case when BigIntValue = 10                      then true else false end as Cumple_BigIntValue
                                                                                     ,case when IntValue = 10                         then true else false end as Cumple_IntValue
                                                                                     ,case when SmallIntValue = 10                         then true else false end as Cumple_SmallIntValue
                                                                                     ,case when TinyIntValue = 10                          then true else false end as Cumple_TinyIntValue
                                                                                     ,case when DecimalValue = 10.1230                     then true else false end as Cumple_DecimalValue
                                                                                     ,case when RealValue = 10.123                        then true else false end as Cumple_RealValue
                                                                                     ,case when FloatValue = cast(10.123  as float)         then true else false end as Cumple_FloatValue
                                                                                     ,case when StringValue = "TEXTO AA"                    then true else false end as Cumple_StringValue
                                                                                     ,case when charValue = "a"                             then true else false end as Cumple_charValue
                                                                                     ,case when timeStampValue = "2017-01-01 00:00:00"      then true else false end as Cumple_timeStampValue
                                                                               FROM DF_Final
                                                                               WHERE tipoValor = 'Positivo_Minimo'""")
      Cantidad = if (Positivo_Minimo_Todos == null) 0 else Positivo_Minimo_Todos.count()
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Positivo_Minimo - TieneRegistros", "Registro Positivo_Minimo, debe tener 1 registro", "Cantidad = 1", s"Cantidad = ${Cantidad}", Cantidad == 1)
      Control.RegisterTestPlanFeature("executeFull", IdTestPlan)
      Control.RegisterTestPlanFeature("StorageType parquet", IdTestPlan)
      Control.RegisterTestPlanFeature("autoCast Encendido", IdTestPlan)
      Control.RegisterTestPlanFeature("IsPK", IdTestPlan)
      Control.RegisterTestPlanFeature("RAW - realiza trim", IdTestPlan)
      val Positivo_Minimo = Positivo_Minimo_Todos.first()
      
      Control.NewStep("DF Plan de pruebas: Positivo_Maximo ")
      val Positivo_Maximo_Todos = huemulLib.DF_ExecuteQuery("Positivo_Maximo_Todos", s"""SELECT case when BigIntValue = 100                      then true else false end as Cumple_BigIntValue
                                                                                     ,case when IntValue = 100                         then true else false end as Cumple_IntValue
                                                                                     ,case when SmallIntValue = 100                         then true else false end as Cumple_SmallIntValue
                                                                                     ,case when TinyIntValue = 100                          then true else false end as Cumple_TinyIntValue
                                                                                     ,case when DecimalValue = 100.1230                     then true else false end as Cumple_DecimalValue
                                                                                     ,case when RealValue = 100.123                        then true else false end as Cumple_RealValue
                                                                                     ,case when FloatValue = cast(100.123  as float)        then true else false end as Cumple_FloatValue
                                                                                     ,case when StringValue = "TEXTO ZZZZZZ"                then true else false end as Cumple_StringValue
                                                                                     ,case when charValue = "z"                             then true else false end as Cumple_charValue
                                                                                     ,case when timeStampValue = "2017-12-31 00:00:00"      then true else false end as Cumple_timeStampValue
                                                                               FROM DF_Final
                                                                               WHERE tipoValor = 'Positivo_Maximo'""")
      Cantidad = if (Positivo_Maximo_Todos == null) 0 else Positivo_Maximo_Todos.count()
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Positivo_Maximo - TieneRegistros", "Registro Positivo_Maximo, debe tener 1 registro", "Cantidad = 1", s"Cantidad = ${Cantidad}", Cantidad == 1)
      Control.RegisterTestPlanFeature("executeFull", IdTestPlan)
      Control.RegisterTestPlanFeature("StorageType parquet", IdTestPlan)
      Control.RegisterTestPlanFeature("autoCast Encendido", IdTestPlan)
      Control.RegisterTestPlanFeature("IsPK", IdTestPlan)
      Control.RegisterTestPlanFeature("RAW - realiza trim", IdTestPlan)
      val Positivo_Maximo = Positivo_Maximo_Todos.first()
      
      Control.NewStep("DF Plan de pruebas: Null ")
      val ValorNull_Todos = huemulLib.DF_ExecuteQuery("ValorNull_Todos", s"""SELECT case when BigIntValue IS NULL                       then true else false end as Cumple_BigIntValue
                                                                                     ,case when IntValue IS NULL                    then true else false end as Cumple_IntValue
                                                                                     ,case when SmallIntValue IS NULL                    then true else false end as Cumple_SmallIntValue
                                                                                     ,case when TinyIntValue IS NULL                     then true else false end as Cumple_TinyIntValue
                                                                                     ,case when DecimalValue IS NULL                     then true else false end as Cumple_DecimalValue
                                                                                     ,case when RealValue IS NULL                        then true else false end as Cumple_RealValue
                                                                                     ,case when FloatValue IS NULL                       then true else false end as Cumple_FloatValue
                                                                                     ,case when StringValue IS NULL                      then true else false end as Cumple_StringValue
                                                                                     ,case when charValue IS NULL                        then true else false end as Cumple_charValue
                                                                                     ,case when timeStampValue IS NULL                   then true else false end as Cumple_timeStampValue
                                                                                     ,StringValue
                                                                               FROM DF_Final
                                                                               WHERE tipoValor = 'nulo'""")
      Cantidad = if (ValorNull_Todos == null) 0 else ValorNull_Todos.count()
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ValorNull - TieneRegistros", "Registro ValorNull, debe tener 1 registro", "Cantidad = 1", s"Cantidad = ${Cantidad}", Cantidad == 1)
      Control.RegisterTestPlanFeature("executeFull", IdTestPlan)
      Control.RegisterTestPlanFeature("StorageType parquet", IdTestPlan)
      Control.RegisterTestPlanFeature("autoCast Encendido", IdTestPlan)
      Control.RegisterTestPlanFeature("IsPK", IdTestPlan)
      Control.RegisterTestPlanFeature("RAW - Convierte string null a null", IdTestPlan)
      val ValorNull = ValorNull_Todos.first()
      
      
   
      
      /*Valida las siguientes funcionalidades:
       * -- Subir datos de tipos numéricos enteros, decimales, texto y fecha, validar que los datos suban correctamente
       * -- Validar la funcionalidad SQL_Insert
       */
      
      Control.NewStep("DF Plan de pruebas: Aplicando validaciones ")
      //**************************
      //****  C O M P A R A C I O N   C E R O - V A C I O  *************
      //**************************
      var BigIntValue =  Cero_Vacio.getAs[Boolean]("Cumple_BigIntValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Cero_Vacio - BigIntValue", "Registro Cero_Vacio, Campo BigIntValue", "Valor = 0", s"Valor = ??", BigIntValue)
      Control.RegisterTestPlanFeature("Datos de tipo BigIntType", IdTestPlan)
      var IntValue =  Cero_Vacio.getAs[Boolean]("Cumple_IntValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Cero_Vacio - IntValue", "Registro Cero_Vacio, Campo IntValue", "Valor = 0", s"Valor = ??", IntValue)
      Control.RegisterTestPlanFeature("Datos de tipo IntegerType", IdTestPlan)
      var SmallIntValue =  Cero_Vacio.getAs[Boolean]("Cumple_SmallIntValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Cero_Vacio - SmallIntValue", "Registro Cero_Vacio, Campo SmallIntValue", "Valor = 0", s"Valor = ??", SmallIntValue)
      Control.RegisterTestPlanFeature("Datos de tipo ShortType", IdTestPlan)
      var TinyIntValue =  Cero_Vacio.getAs[Boolean]("Cumple_TinyIntValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Cero_Vacio - TinyIntValue", "Registro Cero_Vacio, Campo TinyIntValue", "Valor = 0", s"Valor = ??", TinyIntValue)
      Control.RegisterTestPlanFeature("Datos de tipo ShortType", IdTestPlan)
      var DecimalValue =  Cero_Vacio.getAs[Boolean]("Cumple_DecimalValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Cero_Vacio - DecimalValue", "Registro Cero_Vacio, Campo DecimalValue", s"Valor = 0", s"Valor = ??", DecimalValue)
      Control.RegisterTestPlanFeature("Datos de tipo DecimalType", IdTestPlan)
      var RealValue =  Cero_Vacio.getAs[Boolean]("Cumple_RealValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Cero_Vacio - RealValue", "Registro Cero_Vacio, Campo RealValue", "Valor = 0", s"Valor = ??", RealValue)
      Control.RegisterTestPlanFeature("Datos de tipo DoubleType", IdTestPlan)
      var FloatValue =  Cero_Vacio.getAs[Boolean]("Cumple_FloatValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Cero_Vacio - FloatValue", "Registro Cero_Vacio, Campo FloatValue", "Valor = 0", s"Valor = ??", FloatValue)
      Control.RegisterTestPlanFeature("Datos de tipo FloatType", IdTestPlan)
      var StringValue =  Cero_Vacio.getAs[Boolean]("Cumple_StringValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Cero_Vacio - StringValue", "Registro Cero_Vacio, Campo StringValue", "Valor = ", s"Valor = ??", StringValue)
      Control.RegisterTestPlanFeature("Datos de tipo StringType", IdTestPlan)
      var charValue =  Cero_Vacio.getAs[Boolean]("Cumple_charValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Cero_Vacio - charValue", "Registro Cero_Vacio, Campo charValue", "Valor = ", s"Valor = ??", charValue)
      Control.RegisterTestPlanFeature("Datos de tipo StringType", IdTestPlan)
      var timeStampValue =  Cero_Vacio.getAs[Boolean]("Cumple_timeStampValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Cero_Vacio - timeStampValue", "Registro Cero_Vacio, Campo timeStampValue", "Valor = '1900-01-01 00:00:00.0'", s"Valor = ??", timeStampValue)
      Control.RegisterTestPlanFeature("Datos de tipo TimestampType", IdTestPlan)
      
      //**************************
      //****  C O M P A R A C I O N     N E G A T I V O   M A X I M O  *************
      //**************************
      BigIntValue =  Negativo_Maximo.getAs[Boolean]("Cumple_BigIntValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Negativo_Maximo - BigIntValue", "Registro Negativo_Maximo, Campo BigIntValue", "Valor = -10", s"Valor = ??", BigIntValue)
      Control.RegisterTestPlanFeature("Datos de tipo BigIntType", IdTestPlan)
      IntValue =  Negativo_Maximo.getAs[Boolean]("Cumple_IntValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Negativo_Maximo - IntValue", "Registro Negativo_Maximo, Campo IntValue", "Valor = -10", s"Valor = ??", IntValue)
      Control.RegisterTestPlanFeature("Datos de tipo IntegerType", IdTestPlan)
      SmallIntValue =  Negativo_Maximo.getAs[Boolean]("Cumple_SmallIntValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Negativo_Maximo - SmallIntValue", "Registro Negativo_Maximo, Campo SmallIntValue", "Valor = -10", s"Valor = ??", SmallIntValue)
      Control.RegisterTestPlanFeature("Datos de tipo ShortType", IdTestPlan)
      TinyIntValue =  Negativo_Maximo.getAs[Boolean]("Cumple_TinyIntValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Negativo_Maximo - TinyIntValue", "Registro Negativo_Maximo, Campo TinyIntValue", "Valor = -10", s"Valor = ??", TinyIntValue)
      Control.RegisterTestPlanFeature("Datos de tipo ShortType", IdTestPlan)
      DecimalValue =  Negativo_Maximo.getAs[Boolean]("Cumple_DecimalValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Negativo_Maximo - DecimalValue", "Registro Negativo_Maximo, Campo DecimalValue", s"Valor = -10.1230", s"Valor = ??", DecimalValue)
      Control.RegisterTestPlanFeature("Datos de tipo DecimalType", IdTestPlan)
      RealValue =  Negativo_Maximo.getAs[Boolean]("Cumple_RealValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Negativo_Maximo - RealValue", "Registro Negativo_Maximo, Campo RealValue", "Valor = -10.1230", s"Valor = ??", RealValue)
      Control.RegisterTestPlanFeature("Datos de tipo DoubleType", IdTestPlan)
      FloatValue =  Negativo_Maximo.getAs[Boolean]("Cumple_FloatValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Negativo_Maximo - FloatValue", "Registro Negativo_Maximo, Campo FloatValue", "Valor = -10.1230", s"Valor = ??", FloatValue)
      Control.RegisterTestPlanFeature("Datos de tipo FloatType", IdTestPlan)
      StringValue =  Negativo_Maximo.getAs[Boolean]("Cumple_StringValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Negativo_Maximo - StringValue", "Registro Negativo_Maximo, Campo StringValue", "Valor = TEXTO ZZZZZZ", s"Valor = ??", StringValue)
      Control.RegisterTestPlanFeature("Datos de tipo StringType", IdTestPlan)
      charValue =  Negativo_Maximo.getAs[Boolean]("Cumple_charValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Negativo_Maximo - charValue", "Registro Negativo_Maximo, Campo charValue", "Valor = z", s"Valor = ??", charValue)
      Control.RegisterTestPlanFeature("Datos de tipo StringType", IdTestPlan)
      timeStampValue =  Negativo_Maximo.getAs[Boolean]("Cumple_timeStampValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Negativo_Maximo - timeStampValue", "Registro Negativo_Maximo, Campo timeStampValue", "Valor = '2017-12-31 00:00:00.0'", s"Valor = ??", timeStampValue)
      Control.RegisterTestPlanFeature("Datos de tipo TimestampType", IdTestPlan)
      
      
      //**************************
      //****  C O M P A R A C I O N     N E G A T I V O   M I N I M O  *************
      //**************************
      BigIntValue =  Negativo_Minimo.getAs[Boolean]("Cumple_BigIntValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Negativo_Minimo - BigIntValue", "Registro Negativo_Minimo, Campo BigIntValue", "Valor = -100", s"Valor = ??", BigIntValue)
      Control.RegisterTestPlanFeature("Datos de tipo BigIntType", IdTestPlan)
      IntValue =  Negativo_Minimo.getAs[Boolean]("Cumple_IntValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Negativo_Minimo - IntValue", "Registro Negativo_Minimo, Campo IntValue", "Valor = -100", s"Valor = ??", IntValue)
      Control.RegisterTestPlanFeature("Datos de tipo IntegerType", IdTestPlan)
      SmallIntValue =  Negativo_Minimo.getAs[Boolean]("Cumple_SmallIntValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Negativo_Minimo - SmallIntValue", "Registro Negativo_Minimo, Campo SmallIntValue", "Valor = -100", s"Valor = ??", SmallIntValue)
      Control.RegisterTestPlanFeature("Datos de tipo ShortType", IdTestPlan)
      TinyIntValue =  Negativo_Minimo.getAs[Boolean]("Cumple_TinyIntValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Negativo_Minimo - TinyIntValue", "Registro Negativo_Minimo, Campo TinyIntValue", "Valor = -100", s"Valor = ??", TinyIntValue)
      Control.RegisterTestPlanFeature("Datos de tipo ShortType", IdTestPlan)
      DecimalValue =  Negativo_Minimo.getAs[Boolean]("Cumple_DecimalValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Negativo_Minimo - DecimalValue", "Registro Negativo_Minimo, Campo DecimalValue", s"Valor = -100.1230", s"Valor = ??", DecimalValue)
      Control.RegisterTestPlanFeature("Datos de tipo DecimalType", IdTestPlan)
      RealValue =  Negativo_Minimo.getAs[Boolean]("Cumple_RealValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Negativo_Minimo - RealValue", "Registro Negativo_Minimo, Campo RealValue", "Valor = -100.1230", s"Valor = ??", RealValue)
      Control.RegisterTestPlanFeature("Datos de tipo DoubleType", IdTestPlan)
      FloatValue =  Negativo_Minimo.getAs[Boolean]("Cumple_FloatValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Negativo_Minimo - FloatValue", "Registro Negativo_Minimo, Campo FloatValue", "Valor = -100.1230", s"Valor = ??", FloatValue)
      Control.RegisterTestPlanFeature("Datos de tipo FloatType", IdTestPlan)
      StringValue =  Negativo_Minimo.getAs[Boolean]("Cumple_StringValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Negativo_Minimo - StringValue", "Registro Negativo_Minimo, Campo StringValue", "Valor = TEXTO AA", s"Valor = ??", StringValue)
      Control.RegisterTestPlanFeature("Datos de tipo StringType", IdTestPlan)
      charValue =  Negativo_Minimo.getAs[Boolean]("Cumple_charValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Negativo_Minimo - charValue", "Registro Negativo_Minimo, Campo charValue", "Valor = a", s"Valor = ??", charValue)
      Control.RegisterTestPlanFeature("Datos de tipo StringType", IdTestPlan)
      timeStampValue =  Negativo_Minimo.getAs[Boolean]("Cumple_timeStampValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Negativo_Minimo - timeStampValue", "Registro Negativo_Minimo, Campo timeStampValue", "Valor = '2017-01-01 00:00:00.0'", s"Valor = ??", timeStampValue)
      Control.RegisterTestPlanFeature("Datos de tipo TimestampType", IdTestPlan)
    
      //**************************
      //****  C O M P A R A C I O N     P O S I T I V O   M I N I M O  *************
      //**************************
      BigIntValue =  Positivo_Minimo.getAs[Boolean]("Cumple_BigIntValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Positivo_Minimo - BigIntValue", "Registro Positivo_Minimo, Campo BigIntValue", "Valor = 10", s"Valor = ??", BigIntValue)
      Control.RegisterTestPlanFeature("Datos de tipo BigIntType", IdTestPlan)
      IntValue =  Positivo_Minimo.getAs[Boolean]("Cumple_IntValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Positivo_Minimo - IntValue", "Registro Positivo_Minimo, Campo IntValue", "Valor = 10", s"Valor = ??", IntValue)
      Control.RegisterTestPlanFeature("Datos de tipo IntegerType", IdTestPlan)
      SmallIntValue =  Positivo_Minimo.getAs[Boolean]("Cumple_SmallIntValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Positivo_Minimo - SmallIntValue", "Registro Positivo_Minimo, Campo SmallIntValue", "Valor = 10", s"Valor = ??", SmallIntValue)
      Control.RegisterTestPlanFeature("Datos de tipo ShortType", IdTestPlan)
      TinyIntValue =  Positivo_Minimo.getAs[Boolean]("Cumple_TinyIntValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Positivo_Minimo - TinyIntValue", "Registro Positivo_Minimo, Campo TinyIntValue", "Valor = 10", s"Valor = ??", TinyIntValue)
      Control.RegisterTestPlanFeature("Datos de tipo ShortType", IdTestPlan)
      DecimalValue =  Positivo_Minimo.getAs[Boolean]("Cumple_DecimalValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Positivo_Minimo - DecimalValue", "Registro Positivo_Minimo, Campo DecimalValue", s"Valor = 10.1230", s"Valor = ??", DecimalValue)
      Control.RegisterTestPlanFeature("Datos de tipo DecimalType", IdTestPlan)
      RealValue =  Positivo_Minimo.getAs[Boolean]("Cumple_RealValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Positivo_Minimo - RealValue", "Registro Positivo_Minimo, Campo RealValue", "Valor = 10.1230", s"Valor = ??", RealValue)
      Control.RegisterTestPlanFeature("Datos de tipo DoubleType", IdTestPlan)
      FloatValue =  Positivo_Minimo.getAs[Boolean]("Cumple_FloatValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Positivo_Minimo - FloatValue", "Registro Positivo_Minimo, Campo FloatValue", "Valor = 10.1230", s"Valor = ??", FloatValue)
      Control.RegisterTestPlanFeature("Datos de tipo FloatType", IdTestPlan)
      StringValue =  Positivo_Minimo.getAs[Boolean]("Cumple_StringValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Positivo_Minimo - StringValue", "Registro Positivo_Minimo, Campo StringValue", "Valor = TEXTO AA", s"Valor = ??", StringValue)
      Control.RegisterTestPlanFeature("Datos de tipo StringType", IdTestPlan)
      charValue =  Positivo_Minimo.getAs[Boolean]("Cumple_charValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Positivo_Minimo - charValue", "Registro Positivo_Minimo, Campo charValue", "Valor = a", s"Valor = ??", charValue)
      Control.RegisterTestPlanFeature("Datos de tipo StringType", IdTestPlan)
      timeStampValue =  Positivo_Minimo.getAs[Boolean]("Cumple_timeStampValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Positivo_Minimo - timeStampValue", "Registro Positivo_Minimo, Campo timeStampValue", "Valor = '2017-01-01 00:00:00.0'", s"Valor = ??", timeStampValue)
      Control.RegisterTestPlanFeature("Datos de tipo TimestampType", IdTestPlan)
      
     
      //**************************
      //****  C O M P A R A C I O N     P O S I T I V O   M A X I M O  *************
      //**************************
      BigIntValue =  Positivo_Maximo.getAs[Boolean]("Cumple_BigIntValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Positivo_Maximo - BigIntValue", "Registro Positivo_Maximo, Campo BigIntValue", "Valor = 100", s"Valor = ??", BigIntValue)
      Control.RegisterTestPlanFeature("Datos de tipo BigIntType", IdTestPlan)
      IntValue =  Positivo_Maximo.getAs[Boolean]("Cumple_IntValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Positivo_Maximo - IntValue", "Registro Positivo_Maximo, Campo IntValue", "Valor = 100", s"Valor = ??", IntValue)
      Control.RegisterTestPlanFeature("Datos de tipo IntegerType", IdTestPlan)
      SmallIntValue =  Positivo_Maximo.getAs[Boolean]("Cumple_SmallIntValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Positivo_Maximo - SmallIntValue", "Registro Positivo_Maximo, Campo SmallIntValue", "Valor = 100", s"Valor = ??", SmallIntValue)
      Control.RegisterTestPlanFeature("Datos de tipo ShortType", IdTestPlan)
      TinyIntValue =  Positivo_Maximo.getAs[Boolean]("Cumple_TinyIntValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Positivo_Maximo - TinyIntValue", "Registro Positivo_Maximo, Campo TinyIntValue", "Valor = 100", s"Valor = ??", TinyIntValue)
      Control.RegisterTestPlanFeature("Datos de tipo ShortType", IdTestPlan)
      DecimalValue =  Positivo_Maximo.getAs[Boolean]("Cumple_DecimalValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Positivo_Maximo - DecimalValue", "Registro Positivo_Maximo, Campo DecimalValue", s"Valor = 100.1230", s"Valor = ??", DecimalValue)
      Control.RegisterTestPlanFeature("Datos de tipo DecimalType", IdTestPlan)
      RealValue =  Positivo_Maximo.getAs[Boolean]("Cumple_RealValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Positivo_Maximo - RealValue", "Registro Positivo_Maximo, Campo RealValue", "Valor = 100.1230", s"Valor = ??", RealValue)
      Control.RegisterTestPlanFeature("Datos de tipo DoubleType", IdTestPlan)
      FloatValue =  Positivo_Maximo.getAs[Boolean]("Cumple_FloatValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Positivo_Maximo - FloatValue", "Registro Positivo_Maximo, Campo FloatValue", "Valor = 100.1230", s"Valor = ??", FloatValue)
      Control.RegisterTestPlanFeature("Datos de tipo FloatType", IdTestPlan)
      StringValue =  Positivo_Maximo.getAs[Boolean]("Cumple_StringValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Positivo_Maximo - StringValue", "Registro Positivo_Maximo, Campo StringValue", "Valor = TEXTO ZZZZZZ", s"Valor = ??", StringValue)
      Control.RegisterTestPlanFeature("Datos de tipo StringType", IdTestPlan)
      charValue =  Positivo_Maximo.getAs[Boolean]("Cumple_charValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Positivo_Maximo - charValue", "Registro Positivo_Maximo, Campo charValue", "Valor = z", s"Valor = ??", charValue)
      Control.RegisterTestPlanFeature("Datos de tipo StringType", IdTestPlan)
      timeStampValue =  Positivo_Maximo.getAs[Boolean]("Cumple_timeStampValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Positivo_Maximo - timeStampValue", "Registro Positivo_Maximo, Campo timeStampValue", "Valor = '2017-12-31 00:00:00.0'", s"Valor = ??", timeStampValue)
      Control.RegisterTestPlanFeature("Datos de tipo TimestampType", IdTestPlan)
      
      //**************************
      //****  C O M P A R A C I O N      N U L O S  *************
      //**************************
      BigIntValue =  ValorNull.getAs[Boolean]("Cumple_BigIntValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ValorNull - BigIntValue", "Registro ValorNull, Campo BigIntValue", "Valor = null", s"Valor = ??", BigIntValue)
      Control.RegisterTestPlanFeature("Datos de tipo BigIntType", IdTestPlan)
      Control.RegisterTestPlanFeature("RAW - Convierte string null a null", IdTestPlan)
      IntValue =  ValorNull.getAs[Boolean]("Cumple_IntValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ValorNull - IntValue", "Registro ValorNull, Campo IntValue", "Valor = null", s"Valor = ??", IntValue)
      Control.RegisterTestPlanFeature("Datos de tipo IntegerType", IdTestPlan)
      Control.RegisterTestPlanFeature("RAW - Convierte string null a null", IdTestPlan)
      SmallIntValue =  ValorNull.getAs[Boolean]("Cumple_SmallIntValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ValorNull - SmallIntValue", "Registro ValorNull, Campo SmallIntValue", "Valor = null", s"Valor = ??", SmallIntValue)
      Control.RegisterTestPlanFeature("Datos de tipo ShortType", IdTestPlan)
      Control.RegisterTestPlanFeature("RAW - Convierte string null a null", IdTestPlan)
      TinyIntValue =  ValorNull.getAs[Boolean]("Cumple_TinyIntValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ValorNull - TinyIntValue", "Registro ValorNull, Campo TinyIntValue", "Valor = null", s"Valor = ??", TinyIntValue)
      Control.RegisterTestPlanFeature("Datos de tipo ShortType", IdTestPlan)
      Control.RegisterTestPlanFeature("RAW - Convierte string null a null", IdTestPlan)
      DecimalValue =  ValorNull.getAs[Boolean]("Cumple_DecimalValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ValorNull - DecimalValue", "Registro ValorNull, Campo DecimalValue", s"Valor = null", s"Valor = ??", DecimalValue)
      Control.RegisterTestPlanFeature("Datos de tipo DecimalType", IdTestPlan)
      Control.RegisterTestPlanFeature("RAW - Convierte string null a null", IdTestPlan)
      RealValue =  ValorNull.getAs[Boolean]("Cumple_RealValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ValorNull - RealValue", "Registro ValorNull, Campo RealValue", "Valor = null", s"Valor = ??", RealValue)
      Control.RegisterTestPlanFeature("Datos de tipo DoubleType", IdTestPlan)
      Control.RegisterTestPlanFeature("RAW - Convierte string null a null", IdTestPlan)
      FloatValue =  ValorNull.getAs[Boolean]("Cumple_FloatValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ValorNull - FloatValue", "Registro ValorNull, Campo FloatValue", "Valor = null", s"Valor = ??", FloatValue)
      Control.RegisterTestPlanFeature("Datos de tipo FloatType", IdTestPlan)
      Control.RegisterTestPlanFeature("RAW - Convierte string null a null", IdTestPlan)
      StringValue =  ValorNull.getAs[Boolean]("Cumple_StringValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ValorNull - StringValue", "Registro ValorNull, Campo StringValue", "Valor = null", s"Valor = ??", StringValue)
      Control.RegisterTestPlanFeature("Datos de tipo StringType", IdTestPlan)
      Control.RegisterTestPlanFeature("RAW - Convierte string null a null", IdTestPlan)
      charValue =  ValorNull.getAs[Boolean]("Cumple_charValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ValorNull - charValue", "Registro ValorNull, Campo charValue", "Valor = null", s"Valor = ??", charValue)
      Control.RegisterTestPlanFeature("Datos de tipo StringType", IdTestPlan)
      Control.RegisterTestPlanFeature("RAW - Convierte string null a null", IdTestPlan)
      timeStampValue =  ValorNull.getAs[Boolean]("Cumple_timeStampValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ValorNull - timeStampValue", "Registro ValorNull, Campo timeStampValue", "Valor = null", s"Valor = ??", timeStampValue)
      Control.RegisterTestPlanFeature("Datos de tipo TimestampType", IdTestPlan)
      Control.RegisterTestPlanFeature("RAW - Convierte string null a null", IdTestPlan)
      
      
      //**************************
      //****  C O M P A R A C I O N     P O S I T I V O   M A X I M O  *************
      //**************************
     
      //******************************************************************
      //DataQuality solo warning, sin errores
      //******************************************************************
      
      val DQRules = new ArrayBuffer[huemul_DataQuality]()
      val DQ_ComparaAgrupado = new huemul_DataQuality(null,"Suma Float = Suma Decimal","sum(DecimalValue) = sum(FloatValue)",2,huemulType_DQQueryLevel.Aggregate)
      DQ_ComparaAgrupado.setDQ_ExternalCode("EC_001")
      DQRules.append(DQ_ComparaAgrupado)
      val DQ_ComparaFila = new huemul_DataQuality(null,"coalesce(Double,0) = coalesce(Decimal,0)","coalesce(DecimalValue,0) = coalesce(RealValue,0)",3)
      DQ_ComparaFila.setDQ_ExternalCode("EC_002")
      DQRules.append(DQ_ComparaFila)
      
      val DQ_ComparaAgrupado_LanzaWarning = new huemul_DataQuality(null,"Suma Double is null","sum(DecimalValue) is null",4,huemulType_DQQueryLevel.Aggregate, huemulType_DQNotification.WARNING, true, "EC_003")
      DQRules.append(DQ_ComparaAgrupado_LanzaWarning)
      val DQ_ComparaFila_LanzaWarning = new huemul_DataQuality(null,"Double <> Decimal","DecimalValue <> FloatValue",5,huemulType_DQQueryLevel.Row, huemulType_DQNotification.WARNING)
      DQRules.append(DQ_ComparaFila_LanzaWarning)
      
      val DQResultManual = TablaMaster.DataFramehuemul.DF_RunDataQuality(DQRules, null, TablaMaster)
      
      val conError = Control.RegisterTestPlan(TestPlanGroup, "DQ - no debe tener error", "ejecuta la validación, no debe tener error", "IsError = false", s"IsErorr = ${DQResultManual.isError} (errorcode: ${DQResultManual.Error_Code}) ${DQResultManual.Description}", DQResultManual.isError == false)
      Control.RegisterTestPlanFeature("DQManual_Notification_Warning", conError)
      Control.RegisterTestPlanFeature("DQManual_Notification_Error", conError)
      Control.RegisterTestPlanFeature("DQManual_Aggregate", conError)
      Control.RegisterTestPlanFeature("DQManual_Row", conError)
      val NumDQ = Control.RegisterTestPlan(TestPlanGroup, "DQ - N° Ejecuciones", "ejecuta la validación, debe tener 6 ejecuciones", "N° Ejecuciones = 4", s"N° Ejecuciones = ${DQResultManual.getDQResult().length}", DQResultManual.getDQResult().length == 4)
      Control.RegisterTestPlanFeature("DQManual_Notification_Warning", NumDQ)
      Control.RegisterTestPlanFeature("DQManual_Notification_Error", NumDQ)
      Control.RegisterTestPlanFeature("DQManual_Aggregate", NumDQ)
      Control.RegisterTestPlanFeature("DQManual_Row", NumDQ)
      
      val NumDQ_conWarning = Control.RegisterTestPlan(TestPlanGroup, "DQ - N° Ejecuciones con Warning", "ejecuta la validación, debe tener 2 ejecuciones con warnings", "N° Ejecuciones = 2", s"N° Ejecuciones = ${DQResultManual.getDQResult().filter { x => x.DQ_IsWarning} .length}", DQResultManual.getDQResult().filter { x => x.DQ_IsWarning} .length == 2)
      Control.RegisterTestPlanFeature("DQManual_Notification_Warning", NumDQ_conWarning)
      Control.RegisterTestPlanFeature("DQManual_Aggregate", NumDQ_conWarning)
      Control.RegisterTestPlanFeature("DQManual_Row", NumDQ_conWarning)
      
      val NumDQ_conError = Control.RegisterTestPlan(TestPlanGroup, "DQ - N° Ejecuciones con Error", "ejecuta la validación, debe tener 0 ejecuciones con error", "N° Ejecuciones = 0", s"N° Ejecuciones = ${DQResultManual.getDQResult().filter { x => x.DQ_IsError} .length}", DQResultManual.getDQResult().filter { x => x.DQ_IsError} .length == 0)
      Control.RegisterTestPlanFeature("DQManual_Notification_Error", NumDQ_conError)
      Control.RegisterTestPlanFeature("DQManual_Aggregate", NumDQ_conError)
      Control.RegisterTestPlanFeature("DQManual_Row", NumDQ_conError)
      
      
      
      //******************************************************************
      //DataQuality con errores y tolerancia %
      //******************************************************************
      
      val DQRulesConError = new ArrayBuffer[huemul_DataQuality]()
      val DQ_ComparaAgrupado_conError = new huemul_DataQuality(null,"Suma Double > Suma Decimal","sum(DecimalValue) > sum(FloatValue)",6,huemulType_DQQueryLevel.Aggregate)
      DQRulesConError.append(DQ_ComparaAgrupado_conError)
      val DQ_ComparaFila_conError = new huemul_DataQuality(null,"Double > Decimal","DecimalValue > FloatValue",7)
      DQRulesConError.append(DQ_ComparaFila_conError)
     
      val DQ_ComparaFila_Tolerancia = new huemul_DataQuality(null,"DecimalValue > 0.2","DecimalValue > 0",8)
      DQ_ComparaFila_Tolerancia.setTolerance(null, Decimal.apply(0.66)) 
      DQRulesConError.append(DQ_ComparaFila_Tolerancia)
      
      val DQ_ComparaFila_ToleranciaSinError = new huemul_DataQuality(null,"DecimalValue > 0.1","DecimalValue > 0.1",9)
      DQ_ComparaFila_ToleranciaSinError.setTolerance(null,Decimal.apply(0.67))
      DQRulesConError.append(DQ_ComparaFila_ToleranciaSinError)
      
      
      val DQ_ComparaFila_ToleranciaRow = new huemul_DataQuality(null,"DecimalValue > 0.2","DecimalValue > 0",10)
      DQ_ComparaFila_ToleranciaRow.setTolerance(2, null) 
      DQRulesConError.append(DQ_ComparaFila_ToleranciaRow)
      
      val DQ_ComparaFila_ToleranciaSinErrorRow = new huemul_DataQuality(null,"DecimalValue > 0.1","DecimalValue > 0.1",11)
      DQ_ComparaFila_ToleranciaSinErrorRow.setTolerance(5,null)
      DQRulesConError.append(DQ_ComparaFila_ToleranciaSinErrorRow)
      
      
      val DQResultManual_Errores = TablaMaster.DataFramehuemul.DF_RunDataQuality(DQRulesConError, null, TablaMaster)
      
      val NumDQ_ErrorCode = Control.RegisterTestPlan(TestPlanGroup, "DQ - codigo error = 8", "ejecuta la validación, debe tener un error de codigo 8", "error_code = 8", s"error_code = ?", DQResultManual_Errores.getDQResult().filter { x => x.DQ_ErrorCode == 8} .length == 1)
      Control.RegisterTestPlanFeature("DQManual_Notification_Warning", NumDQ_ErrorCode)
      Control.RegisterTestPlanFeature("DQManual_Notification_Error", NumDQ_ErrorCode)
      Control.RegisterTestPlanFeature("DQManual_Aggregate", NumDQ_ErrorCode)
      Control.RegisterTestPlanFeature("DQManual_Row", NumDQ_ErrorCode)
      
      val NumDQ_ErrorCode10 = Control.RegisterTestPlan(TestPlanGroup, "DQ - codigo error = 10", "ejecuta la validación, debe tener un error de codigo 10", "error_code = 10", s"error_code = ?", DQResultManual_Errores.getDQResult().filter { x => x.DQ_ErrorCode == 10} .length == 1)
      Control.RegisterTestPlanFeature("DQManual_Notification_Warning", NumDQ_ErrorCode10)
      Control.RegisterTestPlanFeature("DQManual_Notification_Error", NumDQ_ErrorCode10)
      Control.RegisterTestPlanFeature("DQManual_Aggregate", NumDQ_ErrorCode10)
      Control.RegisterTestPlanFeature("DQManual_Row", NumDQ_ErrorCode10)
      
      val NumDQ_ErrorCodeSinError = Control.RegisterTestPlan(TestPlanGroup, "DQ - DecimalValue > 0.1 sin error", "ejecuta la validación, no debe tener error", "DecimalValue > 0.1 sin error", s"DecimalValue > 0.1 ?", DQResultManual_Errores.getDQResult().filter { x => x.DQ_SQLFormula == "DecimalValue > 0.1"}(0).DQ_IsError == false )
      Control.RegisterTestPlanFeature("DQManual_Notification_Warning", NumDQ_ErrorCodeSinError)
      Control.RegisterTestPlanFeature("DQManual_Notification_Error", NumDQ_ErrorCodeSinError)
      Control.RegisterTestPlanFeature("DQManual_Aggregate", NumDQ_ErrorCodeSinError)
      Control.RegisterTestPlanFeature("DQManual_Row", NumDQ_ErrorCodeSinError)
     
      val NumDQ_conErrores = Control.RegisterTestPlan(TestPlanGroup, "DQ - N° Ejecuciones con Error (V2)", "ejecuta la validación, debe tener 4 ejecuciones con error (warnings) (v2)", "N° Ejecuciones = 4", s"N° Ejecuciones = ${DQResultManual_Errores.getDQResult().filter { x => x.DQ_IsError} .length}", DQResultManual_Errores.getDQResult().filter { x => x.DQ_IsError} .length == 4)
      Control.RegisterTestPlanFeature("DQManual_Notification_Warning", NumDQ_conErrores)
      Control.RegisterTestPlanFeature("DQManual_Notification_Error", NumDQ_conErrores)
      Control.RegisterTestPlanFeature("DQManual_Aggregate", NumDQ_conErrores)
      Control.RegisterTestPlanFeature("DQManual_Row", NumDQ_conErrores)
      
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