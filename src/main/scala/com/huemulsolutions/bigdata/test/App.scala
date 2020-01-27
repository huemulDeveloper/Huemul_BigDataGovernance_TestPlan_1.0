package com.huemulsolutions.bigdata.test


import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.yourcompany.settings.globalSettings
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import java.io.{FileNotFoundException, IOException}
//import com.huemulsolutions.bigdata.test.Proc_PlanPruebas_PermisosUpdate

/**
 * @author ${user.name}
 */

object App {

  def getKeyFromFile(fileName: String): String = {
    var key: String = null
    
   try {
      val openFile = Source.fromFile(fileName)
      key = openFile.getLines.mkString
      openFile.close()
    } catch {
        case e: FileNotFoundException => println(s"Couldn't find that file: ${fileName}")
        case e: IOException => println(s"(${fileName}). Got an IOException! ${e.getLocalizedMessage}")
        case e: Exception => println(s"exception opening ${fileName}")
    }
    
    return key
  }
  
  def main(args : Array[String]) {
    //val huemulLib = new huemul_BigDataGovernance("Pruebas Inicialización de Clases",args,globalSettings.Global)
    //val Control = new huemul_Control(huemulLib,null)
    
    /*
    println(s"${huemulLib.huemul_getDateForLog()}")
    val resultado = huemulLib.ExecuteJDBC_NoResulSet(huemulLib.GlobalSettings.GetPath(huemulLib, huemulLib.GlobalSettings.POSTGRE_Setting), "insert into tempSeba values (10) ")
    println(s"${huemulLib.huemul_getDateForLog()}")
    
    
    val resultado2 = huemulLib.ExecuteJDBC_WithResult(huemulLib.GlobalSettings.GetPath(huemulLib, huemulLib.GlobalSettings.POSTGRE_Setting), "select * from tempSeba ")
    println(s"${huemulLib.huemul_getDateForLog()}")
    println(s"N° de registros resultantes: ${resultado2.ResultSet.length}")
    resultado2.ResultSet.foreach { x => println(s"campo 0: ${x.get(0)} ") }
    resultado2.ResultSet.foreach { x => println(s"campo campo: ${x.getAs("campo")} ") }
    * 
    */
    
    var validacionTablas = new ArrayBuffer[String]()
    validacionTablas.append("production_mdm_oldvalue.tbl_datosbasicos_oldvalue")
    validacionTablas.append("production_mdm_oldvalue.tbl_datosbasicosupdate_oldvalue")
    validacionTablas.append("production_mdm_oldvalue.tbl_oldvaluetrace_oldvalue")
    validacionTablas.append("production_dqerror.tbl_datosbasicos_dq")
    validacionTablas.append("production_dqerror.tbl_datosbasicos_errorfk_dq")
    validacionTablas.append("production_dqerror.tbl_datosbasicos_mes_exclude_dq")
    validacionTablas.append("production_dqerror.tbl_datosbasicoserrores_dq")
    validacionTablas.append("production_dqerror.tbl_datosbasicosinsert_dq")
    validacionTablas.append("production_dqerror.tbl_datosbasicosinsert_exclude_dq")
    validacionTablas.append("production_master.tbl_datosbasicos")
    validacionTablas.append("production_master.tbl_datosbasicos_mes")
    validacionTablas.append("production_master.tbl_datosbasicos_mes_exclude")
    validacionTablas.append("production_master.tbl_datosbasicosinsert")
    validacionTablas.append("production_master.tbl_datosbasicosinsert_exclude")
    validacionTablas.append("production_master.tbl_datosbasicosnuevos")
    validacionTablas.append("production_master.tbl_datosbasicosnuevosperc")
    validacionTablas.append("production_master.tbl_datosbasicosupdate")
    validacionTablas.append("production_master.tbl_oldvaluetrace")
    
    var metadata_hive_active: Boolean = false
    var metadata_spark_active: Boolean = false
    
    val huemulLib_ini = new huemul_BigDataGovernance("Pruebas Inicialización de Clases",args,com.yourcompany.settings.globalSettings.Global)
    
    
    com.yourcompany.settings.globalSettings.Global.HIVE_HourToUpdateMetadata=6
    
    metadata_hive_active = huemulLib_ini.arguments.GetValue("metadata_hive_active", "false").toBoolean
    metadata_spark_active = huemulLib_ini.arguments.GetValue("metadata_spark_active", "false").toBoolean
        
    huemulLib_ini.close()
    
    com.yourcompany.settings.globalSettings.Global.externalBBDD_conf.Using_HIVE.setActive(metadata_hive_active).setActiveForHBASE(metadata_hive_active)
    if (metadata_hive_active) {
      val HIVE_Setting = new ArrayBuffer[huemul_KeyValuePath]()
       val localPath: String = System.getProperty("user.dir").concat("/")
       println(s"path: ${localPath}")
      HIVE_Setting.append(new huemul_KeyValuePath("production",getKeyFromFile(s"${localPath}prod-demo-setting-hive-connection.set")))
      HIVE_Setting.append(new huemul_KeyValuePath("experimental",getKeyFromFile(s"${localPath}prod-demo-setting-hive-connection.set")))
   
      
      com.yourcompany.settings.globalSettings.Global.externalBBDD_conf.Using_HIVE.setConnectionStrings(HIVE_Setting)
      println(s"""num reg: ${com.yourcompany.settings.globalSettings.Global.externalBBDD_conf.Using_HIVE.getJDBC_connection(huemulLib_ini).ExecuteJDBC_WithResult("select 1 as uno").ResultSet.length}""")
    }
    com.yourcompany.settings.globalSettings.Global.externalBBDD_conf.Using_SPARK.setActive(metadata_spark_active).setActiveForHBASE(false)
    
    
    com.huemulsolutions.bigdata.raw.raw_LargoDinamico.main(args)
    Proc_PlanPruebas_CargaMaster_SelectiveUpdate.main(args)

    Proc_PlanPruebas_PermisosFull.main(args)
    Proc_PlanPruebas_PermisosInsert.main(args)
    Proc_PlanPruebas_PermisosUpdate.main(args)
    
    
    Proc_PlanPruebas_CargaMaster.main(args)
    Proc_PlanPruebas_fk.main(args)
    Proc_PlanPruebas_CargaMaster_mes.main(args)
    Proc_PlanPruebas_CargaMaster_mes_2.main(args)
    Proc_PlanPruebas_CargaMaster_mes_paso_2_selective.main(args)
    
    Proc_PlanPruebas_InsertLimitErrorPorc.main(args)
    Proc_PlanPruebas_InsertLimitError.main(args)
    
    Proc_PlanPruebas_NoMapped.main(args)
    
    Proc_PlanPruebas_OnlyInsertNew_warning.main(args)
    Proc_PlanPruebas_OnlyInsertNew.main(args)
    Proc_PlanPruebas_OnlyUpdate.main(args)
     
    Proc_PlanPruebas_AutoCastOff.main(args)
    //Proc_PlanPruebas_CargaAVRO.main(args)
    
    Proc_PlanPruebas_Errores.main(args)
    Proc_PlanPruebas_Malos01.main(args)
    Proc_PlanPruebas_CargaNoTrim.main(args)
    Proc_PlanPruebas_OldValueTrace.main(args)
    Proc_PlanPruebas_CargaMaster_mes_exclude.main(args)
    Proc_PlanPruebas_OnlyInsertNew_exclude.main(args)
    
    
    
    //Validación que todo está OK
    val huemulLib = new huemul_BigDataGovernance("Pruebas Inicialización de Clases",args,com.yourcompany.settings.globalSettings.Global)
    val Control = new huemul_Control(huemulLib,null, huemulType_Frequency.MONTHLY)
    
    if (!huemulLib.hdfsPath_exists("hdfs:///user/data/production/te")) 
      println("prueba 1 exitosa: no existe")
    else 
      println("prueba 1 error")
    
    if (huemulLib.hdfsPath_exists("hdfs:///user/data/production/temp/")) 
      println("prueba 2 exitosa: existe")
    else 
      println("prueba 2 error")
      
    if (huemulLib.hiveTable_exists("production_master", "tbl_DatosBasicos_mes"))
      println("prueba 3 exitosa: tabla existe")
    else 
      println("prueba 3 error")
      
    if (!huemulLib.hiveTable_exists("production_master", "tbl_DatosBasicos_mes234"))
      println("prueba 4 exitosa: tabla no existe")
    else 
      println("prueba 4 error")
    
    val TestPlanGroup: String = huemulLib.arguments.GetValue("TestPlanGroup", null, "Debe especificar el Grupo de Planes de Prueba")

    
    /*Valida existencia de tablas en hive
     */
    
    
    var error_existe_tablas_en_hive: Boolean = false
    if (metadata_hive_active) {
      val connectionHIVE = huemulLib.GlobalSettings.externalBBDD_conf.Using_HIVE.getJDBC_connection(huemulLib)
      println("validando existencia de tablas en hive jdbc")
      
      validacionTablas.foreach { x =>  
        println(s"valida en HIVE existencia tabla $x")
        val valida01 = connectionHIVE.ExecuteJDBC_WithResult(s"select count(1) from ${x}")
        if (valida01.IsError) {
          error_existe_tablas_en_hive = true
          println(valida01.ErrorDescription)
          Control.RegisterTestPlan(TestPlanGroup, s"hive ${x}", s"detalle error: ${valida01.ErrorDescription}", "no raiserror", s"raiserror", false) 
    
        }
      }
        
    }
    Control.RegisterTestPlan(TestPlanGroup, "error_existe_tablas_en_hive", "error_existe_tablas_en_hive", "error_existe_tablas_en_hive = false", s"error_existe_tablas_en_hive = ${error_existe_tablas_en_hive}", error_existe_tablas_en_hive == false) 
    
    
   /*Valida existencia de tablas en hive
     */
    
    println("validando existencia de tablas en spark")
    var error_existe_tablas_en_spark: Boolean = false
    if (metadata_spark_active) {
      
      validacionTablas.foreach { x => 
        try {
          println(s"valida en SPARK existencia tabla $x")
          val valida01 = huemulLib.spark.sql(s"select count(1) from ${x}")
          valida01.show()
        } catch {
          case e: Exception => {
            error_existe_tablas_en_spark = true
            println(e.getMessage)
            Control.RegisterTestPlan(TestPlanGroup, s"hive ${x}", s"detalle error: ${e.getMessage}", "no raiserror", s"raiserror", false)
          }
        }
      }
    }
    Control.RegisterTestPlan(TestPlanGroup, "error_existe_tablas_en_spark", "error_existe_tablas_en_spark", "error_existe_tablas_en_spark = false", s"error_existe_tablas_en_spark = ${error_existe_tablas_en_spark}", error_existe_tablas_en_spark == false) 
    Control.TestPlan_CurrentIsOK(2)
    
    
    
    if (Control.TestPlan_IsOkById(TestPlanGroup, 24))
      println ("TODO OK")
    else
      println ("ERRORES")
      
    huemulLib.close()
    
    
    
  }

}
