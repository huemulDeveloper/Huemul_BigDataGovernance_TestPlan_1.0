package com.huemulsolutions.bigdata.test

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.tables.master.tbl_DatosBasicos
import com.huemulsolutions.bigdata.raw.raw_DatosBasicos
import com.huemulsolutions.bigdata
import org.apache.hadoop.fs.FileSystem
import com.huemulsolutions.bigdata.tables.master.tbl_DatosBasicosAVRO


object Proc_PlanPruebas_CargaAVRO {
  def main(args: Array[String]): Unit = {
    val huemulLib = new huemul_BigDataGovernance("01 - Plan pruebas Proc_PlanPruebas_CargaMaster",args,globalSettings.Global)
    val Control = new huemul_Control(huemulLib,null, huemulType_Frequency.MONTHLY)
    
    val Ano = huemulLib.arguments.GetValue("ano", null,"Debe especificar ano de proceso: ejemplo: ano=2017")
    val Mes = huemulLib.arguments.GetValue("mes", null,"Debe especificar mes de proceso: ejemplo: mes=12")
    
    val TestPlanGroup: String = huemulLib.arguments.GetValue("TestPlanGroup", null, "Debe especificar el Grupo de Planes de Prueba")

    Control.AddParamInformation("TestPlanGroup", TestPlanGroup)
        
    try {
      var IdTestPlan: String = null
      
      Control.NewStep("Define DataFrame Original")
      val DF_RAW =  new raw_DatosBasicos(huemulLib, Control)
      if (!DF_RAW.open("DF_RAW", null, Ano.toInt, Mes.toInt, 1, 0, 0, 0,"")) {
        Control.RaiseError(s"Error al intentar abrir archivo de datos: ${DF_RAW.Error.ControlError_Message}")
      }
      Control.NewStep("Mapeo de Campos")
      val TablaMaster = new tbl_DatosBasicosAVRO(huemulLib, Control)      
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
      //TODO: cambiar el parámetro "true" por algo.UPDATE O algo.NOUPDATE (en replaceValueOnUpdate
      Control.NewStep("Ejecución")
      if (!TablaMaster.executeFull("DF_Final")) {
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Masterización", "No hay error en masterización", "No hay error en masterización", s"Si hay error en masterización(${TablaMaster.Error_Code}): ${TablaMaster.Error_Text}", false)
        Control.RegisterTestPlanFeature("StorageType avro", IdTestPlan)
        Control.RegisterTestPlanFeature("Requiered OK", IdTestPlan)
        Control.RegisterTestPlanFeature("IsPK", IdTestPlan)
        Control.RegisterTestPlanFeature("executeFull", IdTestPlan)
      
        Control.RaiseError(s"Error al masterizar (${TablaMaster.Error_Code}): ${TablaMaster.Error_Text}")
      } else {
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Masterización", "No hay error en masterización", "No hay error en masterización", s"No hay error en masterización", true)
        Control.RegisterTestPlanFeature("StorageType avro", IdTestPlan)
        Control.RegisterTestPlanFeature("Requiered OK", IdTestPlan)
        Control.RegisterTestPlanFeature("IsPK", IdTestPlan)
        Control.RegisterTestPlanFeature("executeFull", IdTestPlan)
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
                                                                                     ,case when timeStampValue = "1900-01-01 00:00:00.0"  then true else false end as Cumple_timeStampValue
                                                                               FROM DF_Final
                                                                               WHERE tipoValor = 'Cero-Vacio'""")
      
      var Cantidad: Long = if (Cero_Vacio_Todos == null) 0 else Cero_Vacio_Todos.count()
      
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Cero_Vacio - TieneRegistros", "Registro Cero_Vacio, debe tener 1 registro", "Cantidad = 1", s"Cantidad = ${Cantidad}", Cantidad == 1)
      Control.RegisterTestPlanFeature("executeFull", IdTestPlan)
      Control.RegisterTestPlanFeature("StorageType avro", IdTestPlan)
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
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Negativo_Maximo - TieneRegistros", "Registro Negativo_Maximo, debe tener 1 registro", "Cantidad = 1", s"Cantidad = ${Cantidad}", Cantidad == 1)
      Control.RegisterTestPlanFeature("executeFull", IdTestPlan)
      Control.RegisterTestPlanFeature("StorageType avro", IdTestPlan)
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
      Control.RegisterTestPlanFeature("StorageType avro", IdTestPlan)
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
      Control.RegisterTestPlanFeature("StorageType avro", IdTestPlan)
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
      Control.RegisterTestPlanFeature("StorageType avro", IdTestPlan)
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
      Control.RegisterTestPlanFeature("StorageType avro", IdTestPlan)
      Control.RegisterTestPlanFeature("autoCast Encendido", IdTestPlan)
      Control.RegisterTestPlanFeature("IsPK", IdTestPlan)
      Control.RegisterTestPlanFeature("RAW - Convierte string null a null", IdTestPlan)
      val ValorNull = ValorNull_Todos.first()
      
      
      Control.NewStep("DF Plan de pruebas: ValoresDefault ")
      val ValoresDefault_Todos = huemulLib.DF_ExecuteQuery("ValoresDefault_Todos", s"""SELECT case when BigIntDefaultValue = 10000                      then true else false end as Cumple_BigIntDefaultValue
                                                                                     ,case when IntDefaultValue = 10000                         then true else false end as Cumple_IntDefaultValue
                                                                                     ,case when SmallIntDefaultValue = 10000                         then true else false end as Cumple_SmallIntDefaultValue
                                                                                     ,case when TinyIntDefaultValue = 10000                          then true else false end as Cumple_TinyIntDefaultValue
                                                                                     ,case when DecimalDefaultValue = 10000.345                     then true else false end as Cumple_DecimalDefaultValue
                                                                                     ,case when RealDefaultValue = 10000.456                        then true else false end as Cumple_RealDefaultValue
                                                                                     ,case when FloatDefaultValue = cast(10000.567  as float)        then true else false end as Cumple_FloatDefaultValue
                                                                                     ,case when StringDefaultValue = "valor en string"                then true else false end as Cumple_StringDefaultValue
                                                                                     ,case when charDefaultValue = cast('hola' as string)                             then true else false end as Cumple_charDefaultValue
                                                                                     ,case when timeStampDefaultValue = "2019-01-01"      then true else false end as Cumple_timeStampDefaultValue
                                                                               FROM (select distinct BigIntDefaultValue, IntDefaultValue, SmallIntDefaultValue, TinyIntDefaultValue, DecimalDefaultValue, RealDefaultValue, FloatDefaultValue, StringDefaultValue, charDefaultValue, timeStampDefaultValue   FROM DF_Final) a
                                                                               """)
      Cantidad = if (ValoresDefault_Todos == null) 0 else ValoresDefault_Todos.count()
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ValoresDefault - TieneRegistros", "Registro ValoresDefault, debe tener 1 registro", "Cantidad = 1", s"Cantidad = ${Cantidad}", Cantidad == 1)
      Control.RegisterTestPlanFeature("executeFull", IdTestPlan)
      Control.RegisterTestPlanFeature("StorageType avro", IdTestPlan)
      Control.RegisterTestPlanFeature("autoCast Encendido", IdTestPlan)
      Control.RegisterTestPlanFeature("IsPK", IdTestPlan)
      Control.RegisterTestPlanFeature("RAW - realiza trim", IdTestPlan)
      val ValoresDefault = ValoresDefault_Todos.first()
      
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
      BigIntValue =  ValoresDefault.getAs[Boolean]("Cumple_BigIntDefaultValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ValoresDefault - BigIntDefaultValue", "Registro ValoresDefault, Campo BigIntDefaultValue", "Valor = 10000", s"Valor = ??", BigIntValue)
      Control.RegisterTestPlanFeature("Datos de tipo BigIntType", IdTestPlan)
      IntValue =  ValoresDefault.getAs[Boolean]("Cumple_IntDefaultValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ValoresDefault - IntDefaultValue", "Registro ValoresDefault, Campo IntDefaultValue", "Valor = 10000", s"Valor = ??", IntValue)
      Control.RegisterTestPlanFeature("Datos de tipo IntegerType", IdTestPlan)
      SmallIntValue =  ValoresDefault.getAs[Boolean]("Cumple_SmallIntDefaultValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ValoresDefault - SmallIntDefaultValue", "Registro ValoresDefault, Campo SmallIntDefaultValue", "Valor = 10000", s"Valor = ??", SmallIntValue)
      Control.RegisterTestPlanFeature("Datos de tipo ShortType", IdTestPlan)
      TinyIntValue =  ValoresDefault.getAs[Boolean]("Cumple_TinyIntDefaultValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ValoresDefault - TinyIntDefaultValue", "Registro ValoresDefault, Campo TinyIntDefaultValue", "Valor = 10000", s"Valor = ??", TinyIntValue)
      Control.RegisterTestPlanFeature("Datos de tipo ShortType", IdTestPlan)
      DecimalValue =  ValoresDefault.getAs[Boolean]("Cumple_DecimalDefaultValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ValoresDefault - DecimalDefaultValue", "Registro ValoresDefault, Campo DecimalDefaultValue", s"Valor = 10000.345", s"Valor = ??", DecimalValue)
      Control.RegisterTestPlanFeature("Datos de tipo DecimalType", IdTestPlan)
      RealValue =  ValoresDefault.getAs[Boolean]("Cumple_RealDefaultValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ValoresDefault - RealDefaultValue", "Registro ValoresDefault, Campo RealDefaultValue", "Valor = 10000.456", s"Valor = ??", RealValue)
      Control.RegisterTestPlanFeature("Datos de tipo DoubleType", IdTestPlan)
      FloatValue =  ValoresDefault.getAs[Boolean]("Cumple_FloatDefaultValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ValoresDefault - FloatDefaultValue", "Registro ValoresDefault, Campo FloatDefaultValue", "Valor = 10000.567", s"Valor = ??", FloatValue)
      Control.RegisterTestPlanFeature("Datos de tipo FloatType", IdTestPlan)
      StringValue =  ValoresDefault.getAs[Boolean]("Cumple_StringDefaultValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ValoresDefault - StringDefaultValue", "Registro ValoresDefault, Campo StringDefaultValue", "Valor = valor en string", s"Valor = ??", StringValue)
      Control.RegisterTestPlanFeature("Datos de tipo StringType", IdTestPlan)
      charValue =  ValoresDefault.getAs[Boolean]("Cumple_charDefaultValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ValoresDefault - charDefaultValue", "Registro ValoresDefault, Campo charDefaultValue", "Valor = hola", s"Valor = ??", charValue)
      Control.RegisterTestPlanFeature("Datos de tipo StringType", IdTestPlan)
      timeStampValue =  ValoresDefault.getAs[Boolean]("Cumple_timeStampDefaultValue")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ValoresDefault - timeStampDefaultValue", "Registro ValoresDefault, Campo timeStampDefaultValue", "Valor = '2019-01-01'", s"Valor = ??", timeStampValue)
      Control.RegisterTestPlanFeature("Datos de tipo TimestampType", IdTestPlan)
      
      
          Control.FinishProcessOK
    } catch {
      case e: Exception => 
        val IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR", "ERROR DE PROGRAMA -  no deberia tener errror", "sin error", s"con error: ${e.getMessage}", false)
        Control.RegisterTestPlanFeature("executeFull", IdTestPlan)
        Control.RegisterTestPlanFeature("StorageType avro", IdTestPlan)
        Control.Control_Error.GetError(e, this.getClass.getSimpleName, 1)
        Control.FinishProcessError()
    }
    
    huemulLib.close()
  }
}