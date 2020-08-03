package com.huemulsolutions.bigdata.test

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.tables.huemulType_StorageType
import com.huemulsolutions.bigdata.tables.huemulType_StorageType.huemulType_StorageType
import com.huemulsolutions.bigdata.tables.master.tbl_DatosParticion

/**
 * Este plan de pruebas valida lo siguiente:
 * Error en PK: hay registros duplicados, lo que se espera es un error de PK
 * el TipodeArchivo usado es Malo01
 */
object Proc_PlanPruebas_Particion_dia_dia2p2 {
  def main(args: Array[String]): Unit = {
    val huemulLib = new huemul_BigDataGovernance("01 - Proc_PlanPruebas_Particion_dia_dia2p2",args,com.yourcompany.settings.globalSettings.Global)
    val Control = new huemul_Control(huemulLib,null, huemulType_Frequency.MONTHLY)
    var empresaName: String = "EmpresA"

    //val empresa = huemulLib.arguments.GetValue("empresa", null,"Debe especificar una empresa, ejemplo: empresa=super-01")
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
    else if (TipoTablaParam == "avro") {
      TipoTabla = huemulType_StorageType.AVRO
      empresaName = empresaName.toLowerCase()
    }

    val TestPlanGroup: String = huemulLib.arguments.GetValue("TestPlanGroup", null, "Debe especificar el Grupo de Planes de Prueba")
    var IdTestPlan: String = ""
    Control.AddParamInformation("TestPlanGroup", TestPlanGroup)
        
    try {
      Control.NewStep("Ejecuta pruebas con dia 01")
      val args_01: Array[String] = new Array[String](1)
      val Ano = 2017
      val Mes = 5
      val dia  = 2
      val empresa = "super-02"
      args_01(0) = s"Environment=production,ano=$Ano,mes=$Mes,dia=$dia,empresa=$empresa,TipoTabla=$TipoTablaParam"
      val control_resultado01 = Proc_PlanPruebas_Particion_dia.processMaster(huemulLib, args_01)

      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Resultado Ejecucion dia 02, p2", "Resultado Ejecución 01", "error = false", s"error = ${control_resultado01.Control_Error.ControlError_IsError}", !control_resultado01.Control_Error.ControlError_IsError)

      //abre instancia de tabla para obtener algunos parámetros
      val TablaMaster = new tbl_DatosParticion(huemulLib, Control, TipoTabla)

      //valida que existan las particiones esperadas + los datos anteriores
      val path_20170501_super01_internet = TablaMaster.getFullNameWithPath().concat(s"/periodo=2017-05-01/$empresaName=super-01/app=internet")
      val path_20170501_super01_tienda = TablaMaster.getFullNameWithPath().concat(s"/periodo=2017-05-01/$empresaName=super-01/app=tienda")
      val path_20170501_super02_internet = TablaMaster.getFullNameWithPath().concat(s"/periodo=2017-05-01/$empresaName=super-02/app=internet")
      val path_20170501_super02_telefono = TablaMaster.getFullNameWithPath().concat(s"/periodo=2017-05-01/$empresaName=super-02/app=telefono")

      val path_20170502_super01_internet = TablaMaster.getFullNameWithPath().concat(s"/periodo=2017-05-02/$empresaName=super-01/app=internet")
      val path_20170502_super01_tienda = TablaMaster.getFullNameWithPath().concat(s"/periodo=2017-05-02/$empresaName=super-01/app=tienda")
      val path_20170502_super03_internet = TablaMaster.getFullNameWithPath().concat(s"/periodo=2017-05-02/$empresaName=super-03/app=internet")
      val path_20170502_super03_tienda = TablaMaster.getFullNameWithPath().concat(s"/periodo=2017-05-02/$empresaName=super-03/app=tienda")

      val path_20170502_super02_internet = TablaMaster.getFullNameWithPath().concat(s"/periodo=2017-05-02/$empresaName=super-02/app=internet")
      val path_20170502_super02_telefono = TablaMaster.getFullNameWithPath().concat(s"/periodo=2017-05-02/$empresaName=super-02/app=telefono")


      Control.NewStep(s"buscando path $path_20170501_super01_internet")
      var path_existe = huemulLib.hdfsPath_exists(path_20170501_super01_internet)
      IdTestPlan = Control.RegisterTestPlan(
           TestPlanGroup
        , "dia 01 - existe path path_20170501_super01_internet"
        , s"path buscado = $path_20170501_super01_internet"
        , "path existe = true"
        , s"path existe = $path_existe"
        , path_existe)

      Control.NewStep(s"buscando path $path_20170501_super01_tienda")
      path_existe = huemulLib.hdfsPath_exists(path_20170501_super01_tienda)
      IdTestPlan = Control.RegisterTestPlan(
        TestPlanGroup
        , "dia 01 - existe path path_20170501_super01_tienda"
        , s"path buscado = $path_20170501_super01_tienda"
        , "path existe = true"
        , s"path existe = $path_existe"
        , path_existe)


      Control.NewStep(s"buscando path $path_20170501_super02_internet")
      path_existe = huemulLib.hdfsPath_exists(path_20170501_super02_internet)
      IdTestPlan = Control.RegisterTestPlan(
        TestPlanGroup
        , "dia 01 - existe path path_20170501_super02_internet"
        , s"path buscado = $path_20170501_super02_internet"
        , "path existe = true"
        , s"path existe = $path_existe"
        , path_existe)

      Control.NewStep(s"buscando path $path_20170501_super02_telefono")
      path_existe = huemulLib.hdfsPath_exists(path_20170501_super02_telefono)
      IdTestPlan = Control.RegisterTestPlan(
        TestPlanGroup
        , "dia 01 - existe path path_20170501_super02_telefono"
        , s"path buscado = $path_20170501_super02_telefono"
        , "path existe = true"
        , s"path existe = $path_existe"
        , path_existe)


      Control.NewStep(s"buscando path $path_20170502_super01_internet")
      path_existe = huemulLib.hdfsPath_exists(path_20170502_super01_internet)
      IdTestPlan = Control.RegisterTestPlan(
        TestPlanGroup
        , "dia 02 - existe path path_20170502_super01_internet"
        , s"path buscado = $path_20170502_super01_internet"
        , "path existe = true"
        , s"path existe = $path_existe"
        , path_existe)

      Control.NewStep(s"buscando path $path_20170502_super01_tienda")
      path_existe = huemulLib.hdfsPath_exists(path_20170502_super01_tienda)
      IdTestPlan = Control.RegisterTestPlan(
        TestPlanGroup
        , "dia 02 - existe path path_20170502_super01_tienda"
        , s"path buscado = $path_20170502_super01_tienda"
        , "path existe = true"
        , s"path existe = $path_existe"
        , path_existe)

      Control.NewStep(s"buscando path $path_20170502_super03_internet")
      path_existe = huemulLib.hdfsPath_exists(path_20170502_super03_internet)
      IdTestPlan = Control.RegisterTestPlan(
        TestPlanGroup
        , "dia 02 - existe path path_20170502_super03_internet"
        , s"path buscado = $path_20170502_super03_internet"
        , "path existe = true"
        , s"path existe = $path_existe"
        , path_existe)

      Control.NewStep(s"buscando path $path_20170502_super03_tienda")
      path_existe = huemulLib.hdfsPath_exists(path_20170502_super03_tienda)
      IdTestPlan = Control.RegisterTestPlan(
        TestPlanGroup
        , "dia 02 - existe path path_20170502_super03_tienda"
        , s"path buscado = $path_20170502_super03_tienda"
        , "path existe = true"
        , s"path existe = $path_existe"
        , path_existe)


      Control.NewStep(s"buscando path $path_20170502_super02_internet")
      path_existe = huemulLib.hdfsPath_exists(path_20170502_super02_internet)
      IdTestPlan = Control.RegisterTestPlan(
        TestPlanGroup
        , "dia 02 - existe path path_20170502_super02_internet"
        , s"path buscado = $path_20170502_super02_internet"
        , "path existe = true"
        , s"path existe = $path_existe"
        , path_existe)

      Control.NewStep(s"buscando path $path_20170502_super02_telefono")
      path_existe = huemulLib.hdfsPath_exists(path_20170502_super02_telefono)
      IdTestPlan = Control.RegisterTestPlan(
        TestPlanGroup
        , "dia 02 - existe path path_20170502_super02_telefono"
        , s"path buscado = $path_20170502_super02_telefono"
        , "path existe = true"
        , s"path existe = $path_existe"
        , path_existe)


      huemulLib.spark.sql("select * from production_master.tbl_datosparticion").show()



      val sql_01 = s"""
           SELECT periodo, empresa, app, cast(count(1) as Integer) as cantidad
           FROM production_master.tbl_datosparticion
           GROUP BY periodo, empresa, app
           """
      println(sql_01)

      //valida que datos particionados existan
      val DF_valida01 = huemulLib.spark.sql(
        sql_01)

      DF_valida01.show()

      val count_01 = DF_valida01.count()
      //muestra datos ejemplos
      IdTestPlan = Control.RegisterTestPlan(
        TestPlanGroup
        , "dia 01 - calida cantidad de datos totales"
        , s"debe arrojar 10 registros"
        , "registros = 10"
        , s"registros = $count_01"
        , count_01 == 10)

      val registro01_01 = DF_valida01.where("""periodo = "2017-05-01" and empresa = "super-01" and app = "internet"""").select("cantidad").first()
      if (registro01_01 == null) {
        IdTestPlan = Control.RegisterTestPlan(
          TestPlanGroup
          , "dia 01 - existe registro 2017-05-01 | super-01 | internet"
          , s"debe arrojar 2 registros"
          , "registros = 2"
          , s"registros no encontrados"
          , p_testPlan_IsOK = false)
      }
      else {
        IdTestPlan = Control.RegisterTestPlan(
          TestPlanGroup
          , "dia 01 - existe registro 2017-05-01 | super-01 | internet"
          , s"debe arrojar 2 registros"
          , "registros = 2"
          , s"registros = ${registro01_01.getAs[Integer]("cantidad")}"
          , registro01_01.getAs[Integer]("cantidad") == 2)
      }

      val registro01_02 = DF_valida01.where("periodo = '2017-05-01' and empresa = 'super-01' and app = 'tienda'").select("cantidad").first()
      if (registro01_02 == null) {
        IdTestPlan = Control.RegisterTestPlan(
          TestPlanGroup
          , "dia 01 - existe registro 2017-05-01 | super-01 | tieda"
          , s"debe arrojar 2 registros"
          , "registros = 2"
          , s"registros no encontrados"
          , p_testPlan_IsOK = false)
      }
      else {
        IdTestPlan = Control.RegisterTestPlan(
          TestPlanGroup
          , "dia 01 - existe registro 2017-05-01 | super-01 | tieda"
          , s"debe arrojar 2 registros"
          , "registros = 2"
          , s"registros = ${registro01_02.getAs[Integer]("cantidad")}"
          , registro01_02.getAs[Integer]("cantidad") == 2)
      }

      val registro01_03 = DF_valida01.where("periodo = '2017-05-01' and empresa = 'super-02' and app = 'internet'").select("cantidad").first()
      if (registro01_03 == null) {
        IdTestPlan = Control.RegisterTestPlan(
          TestPlanGroup
          , "dia 01 - existe registro 2017-05-01 | super-02 | internet"
          , s"debe arrojar 2 registros"
          , "registros = 2"
          , s"registros no encontrados"
          , p_testPlan_IsOK = false)
      }
      else {
        IdTestPlan = Control.RegisterTestPlan(
          TestPlanGroup
          , "dia 01 - existe registro 2017-05-01 | super-02 | internet"
          , s"debe arrojar 2 registros"
          , "registros = 2"
          , s"registros = ${registro01_03.getAs[Integer]("cantidad")}"
          , registro01_03.getAs[Integer]("cantidad") == 2)
      }

      val registro01_04 = DF_valida01.where("periodo = '2017-05-01' and empresa = 'super-02' and app = 'telefono'").select("cantidad").first()
      if (registro01_04 == null) {
        IdTestPlan = Control.RegisterTestPlan(
          TestPlanGroup
          , "dia 01 - existe registro 2017-05-01 | super-02 | telefono"
          , s"debe arrojar 2 registros"
          , "registros = 2"
          , s"registros no encontrados"
          , p_testPlan_IsOK = false)
      }
      else {
        IdTestPlan = Control.RegisterTestPlan(
          TestPlanGroup
          , "dia 01 - existe registro 2017-05-01 | super-02 | telefono"
          , s"debe arrojar 2 registros"
          , "registros = 2"
          , s"registros = ${registro01_04.getAs[Integer]("cantidad")}"
          , registro01_04.getAs[Integer]("cantidad") == 2)
      }



      val registro02_01 = DF_valida01.where("periodo = '2017-05-02' and empresa = 'super-01' and app = 'internet'").select("cantidad").first()
      if (registro02_01 == null) {
        IdTestPlan = Control.RegisterTestPlan(
          TestPlanGroup
          , "dia 02 - existe registro 2017-05-02 | super-01 | internet"
          , s"debe arrojar 2 registros"
          , "registros = 2"
          , s"registros no encontrados"
          , p_testPlan_IsOK = false)
      }
      else {
        IdTestPlan = Control.RegisterTestPlan(
          TestPlanGroup
          , "dia 02 - existe registro 2017-05-02 | super-01 | internet"
          , s"debe arrojar 2 registros"
          , "registros = 2"
          , s"registros = ${registro02_01.getAs[Integer]("cantidad")}"
          , registro02_01.getAs[Integer]("cantidad") == 2)
      }

      val registro02_02 = DF_valida01.where("periodo = '2017-05-02' and empresa = 'super-01' and app = 'tienda'").select("cantidad").first()
      if (registro02_02 == null) {
        IdTestPlan = Control.RegisterTestPlan(
          TestPlanGroup
          , "dia 02 - existe registro 2017-05-02 | super-01 | tienda"
          , s"debe arrojar 2 registros"
          , "registros = 2"
          , s"registros no encontrados"
          , p_testPlan_IsOK = false)
      }
      else {
        IdTestPlan = Control.RegisterTestPlan(
          TestPlanGroup
          , "dia 02 - existe registro 2017-05-02 | super-01 | tienda"
          , s"debe arrojar 2 registros"
          , "registros = 2"
          , s"registros = ${registro02_02.getAs[Integer]("cantidad")}"
          , registro02_02.getAs[Integer]("cantidad") == 2)
      }

      val registro02_03 = DF_valida01.where("periodo = '2017-05-02' and empresa = 'super-03' and app = 'internet'").select("cantidad").first()
      if (registro02_03 == null) {
        IdTestPlan = Control.RegisterTestPlan(
          TestPlanGroup
          , "dia 02 - existe registro 2017-05-02 | super-03 | internet"
          , s"debe arrojar 2 registros"
          , "registros = 2"
          , s"registros no encontrados"
          , p_testPlan_IsOK = false)
      }
      else {
        IdTestPlan = Control.RegisterTestPlan(
          TestPlanGroup
          , "dia 02 - existe registro 2017-05-02 | super-03 | internet"
          , s"debe arrojar 2 registros"
          , "registros = 2"
          , s"registros = ${registro02_03.getAs[Integer]("cantidad")}"
          , registro02_03.getAs[Integer]("cantidad") == 2)
      }

      val registro02_04 = DF_valida01.where("periodo = '2017-05-02' and empresa = 'super-03' and app = 'tienda'").select("cantidad").first()
      if (registro02_04 == null) {
        IdTestPlan = Control.RegisterTestPlan(
          TestPlanGroup
          , "dia 02 - existe registro 2017-05-02 | super-03 | tienda"
          , s"debe arrojar 2 registros"
          , "registros = 2"
          , s"registros no encontrados"
          , p_testPlan_IsOK = false)
      }
      else {
        IdTestPlan = Control.RegisterTestPlan(
          TestPlanGroup
          , "dia 02 - existe registro 2017-05-02 | super-03 | tienda"
          , s"debe arrojar 2 registros"
          , "registros = 2"
          , s"registros = ${registro02_04.getAs[Integer]("cantidad")}"
          , registro02_04.getAs[Integer]("cantidad") == 2)
      }



      val registro01_06 = DF_valida01.where("periodo = '2017-05-02' and empresa = 'super-02' and app = 'internet'").select("cantidad").first()
      if (registro01_06 == null) {
        IdTestPlan = Control.RegisterTestPlan(
          TestPlanGroup
          , "dia 01 - existe registro 2017-05-02 | super-02 | internet"
          , s"debe arrojar 2 registros"
          , "registros = 2"
          , s"registros no encontrados"
          , p_testPlan_IsOK = false)
      }
      else {
        IdTestPlan = Control.RegisterTestPlan(
          TestPlanGroup
          , "dia 01 - existe registro 2017-05-02 | super-02 | internet"
          , s"debe arrojar 2 registros"
          , "registros = 2"
          , s"registros = ${registro01_06.getAs[Integer]("cantidad")}"
          , registro01_06.getAs[Integer]("cantidad") == 2)
      }

      val registro02_05 = DF_valida01.where("periodo = '2017-05-02' and empresa = 'super-02' and app = 'telefono'").select("cantidad").first()
      if (registro02_05 == null) {
        IdTestPlan = Control.RegisterTestPlan(
          TestPlanGroup
          , "dia 01 - existe registro 2017-05-02 | super-02 | telefono"
          , s"debe arrojar 2 registros"
          , "registros = 2"
          , s"registros no encontrados"
          , p_testPlan_IsOK = false)
      }
      else {
        IdTestPlan = Control.RegisterTestPlan(
          TestPlanGroup
          , "dia 01 - existe registro 2017-05-02 | super-02 | telefono"
          , s"debe arrojar 2 registros"
          , "registros = 2"
          , s"registros = ${registro02_05.getAs[Integer]("cantidad")}"
          , registro02_05.getAs[Integer]("cantidad") == 2)
      }
      
      /////////////////////////////////////////////////////////////////////////////////////////
      /////////////////////////////////////////////////////////////////////////////////////////
      //  I N I C I A   P L A N   D E   P R U E B A S
      /////////////////////////////////////////////////////////////////////////////////////////
      /////////////////////////////////////////////////////////////////////////////////////////
      //valida que respuesta sea negativa
      
      Control.FinishProcessOK
    } catch {
      case e: Exception => 
        
        Control.RegisterTestPlan(TestPlanGroup, "Error en pruebas", "ERROR DE PROGRAMA -  NO deberia tener errror", "sin errores", s"con error: ${e.getMessage}", p_testPlan_IsOK = false)
        Control.Control_Error.GetError(e, this.getClass.getSimpleName, 1)
        Control.FinishProcessError()
    }
    
    if (Control.TestPlan_CurrentIsOK(22))
      println("Proceso OK")
    
    huemulLib.close()
  }
}