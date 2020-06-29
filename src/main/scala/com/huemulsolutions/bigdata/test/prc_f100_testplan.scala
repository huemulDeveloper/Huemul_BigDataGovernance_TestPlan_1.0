package com.huemulsolutions.bigdata.test

import java.sql.Date

import com.huemulsolutions.bigdata.common.huemul_BigDataGovernance
import com.huemulsolutions.bigdata.control.{huemulType_Frequency, huemul_Control}
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

object prc_f100_testplan {
  var control:huemul_Control = _
  var huemulBigDataGov:huemul_BigDataGovernance = _

  def main(args : Array[String]): Unit ={

    huemulBigDataGov  = new huemul_BigDataGovernance(
      s"Masterizacion tabla <paises> - ${this.getClass.getSimpleName}"
      , args, com.yourcompany.settings.globalSettings.Global)

    control = new huemul_Control(huemulBigDataGov, null, huemulType_Frequency.ANY_MOMENT )

    //comentado, el archivo ya existe en el directorio HDFS
    //dataGeneration(huemulBigDataGov)

    // Ejecuta el plan de prueba WARNING_EXCLUDE
    //val TestPlanGroup: .arguments.GetValue("TestPlanGroup", null, "Debe especificar el Grupo de Planes de Prueba")
    val TestPlanGroup: String = huemulBigDataGov.arguments.GetValue("TestPlanGroup", null, "Debe especificar el Grupo de Planes de Prueba")
    val outputDatabase = huemulBigDataGov.GlobalSettings.GetDataBase(huemulBigDataGov, huemulBigDataGov.GlobalSettings.DQError_DataBase)

    try {

      val dataF100ControlWEX: huemul_Control = prc_dataf100_WEX.processMaster(huemulBigDataGov: huemul_BigDataGovernance, control, 2020, 1)

      val hasErrorWEX = dataF100ControlWEX.Control_Error.ControlError_IsError
      val dqControlIdWEX = dataF100ControlWEX.Control_Id
      println(s"dqControlId=$dqControlIdWEX")
      val tblDataF100WexDQ = s"$outputDatabase.dataf100_wex_dq"

      //------------------------------------------------------------------------------------------------------------
      control.NewStep("WARNING_EXCLUDE - Valida que el process ejecute ok")
      val tbExec = control.RegisterTestPlan(
        TestPlanGroup
        , "Process_Run"
        , ""
        , "hasError = true"
        , s"hasError = $hasErrorWEX"
        , hasErrorWEX)
      control.RegisterTestPlanFeature("warning_Exclude", tbExec)

      //------------------------------------------------------------------------------------------------------------
      saveTestplanCount(
        dqControlIdWEX
        , TestPlanGroup
        , "WARNING_EXCLUDE - Warning_Exclude_Total"
        , "Valida cantidad total de warning_exclude"
        , tblDataF100WexDQ
        , null
        , "WARNING_EXCLUDE"
        , null
        , 24L
      )

      //------------------------------------------------------------------------------------------------------------
      saveTestplanCount(
        dqControlIdWEX
        , TestPlanGroup
        , "WARNING_EXCLUDE - Warning_Total"
        , "Valida cantidad total de warning"
        , tblDataF100WexDQ
        , null
        , "WARNING"
        , null
        , 1L
      )

      //------------------------------------------------------------------------------------------------------------
      saveTestplanCount(
        dqControlIdWEX
        , TestPlanGroup
        , "WARNING_EXCLUDE - ERROR_Total"
        , "Valida cantidad total de error"
        , tblDataF100WexDQ
        , null
        , "ERROR"
        , null
        , 1L
      )

      testPlanCases(dqControlIdWEX, TestPlanGroup,"WEX", tblDataF100WexDQ, "WARNING_EXCLUDE")

      //------------------------------------------------------------------------------------------------------------
      //------------------------------------------------------------------------------------------------------------
      val dataF100ControlW: huemul_Control = prc_dataf100_W.processMaster(huemulBigDataGov: huemul_BigDataGovernance, control, 2020, 1)

      val hasErrorW = dataF100ControlW.Control_Error.ControlError_IsError
      val dqControlIdW = dataF100ControlW.Control_Id
      println(s"dqControlId=$dqControlIdW")
      val tblDataF100WDQ = s"$outputDatabase.dataf100_w_dq"

      //------------------------------------------------------------------------------------------------------------
      control.NewStep("WARNING - Valida que el process ejecute ok")
      val tbExecW = control.RegisterTestPlan(
        TestPlanGroup
        , "WARNING - Process_Run"
        , ""
        , "hasError = true"
        , s"hasError = $hasErrorW"
        , hasErrorW)
      control.RegisterTestPlanFeature("warning_Exclude", tbExecW)

      //------------------------------------------------------------------------------------------------------------
      saveTestplanCount(
        dqControlIdW
        , TestPlanGroup
        , s"WARNING - Warning_Exclude_Total"
        , "Valida cantiadad total de warning_exclude"
        , tblDataF100WDQ
        , null
        , "WARNING_EXCLUDE"
        , null
        , 1L
      )

      //------------------------------------------------------------------------------------------------------------
      saveTestplanCount(
        dqControlIdW
        , TestPlanGroup
        , "WARNING - Warning_Total"
        , "Valida cantiadad total de warning"
        , tblDataF100WDQ
        , null
        , "WARNING"
        , null
        , 24L
      )

      //------------------------------------------------------------------------------------------------------------
      saveTestplanCount(
        dqControlIdW
        , TestPlanGroup
        , "WARNING - ERROR_Total"
        , "Valida cantiadad total de error"
        , tblDataF100WDQ
        , null
        , "ERROR"
        , null
        , 1L
      )

      testPlanCases(dqControlIdW, TestPlanGroup, "W", tblDataF100WDQ, "WARNING")

      //------------------------------------------------------------------------------------------------------------
      //------------------------------------------------------------------------------------------------------------
      val dataF100ControlE: huemul_Control = prc_dataf100_E.processMaster(huemulBigDataGov: huemul_BigDataGovernance, control, 2020, 1)

      val hasErrorE = dataF100ControlE.Control_Error.ControlError_IsError
      val dqControlIdE = dataF100ControlE.Control_Id
      println(s"dqControlId=$dqControlIdE")
      val tblDataF100EDQ = s"$outputDatabase.dataf100_E_dq"

      //------------------------------------------------------------------------------------------------------------
      control.NewStep("ERROR - Valida que el process ejecute ok")
      val tbExecE = control.RegisterTestPlan(
        TestPlanGroup
        , "ERROR - Process_Run"
        , ""
        , "hasError = true"
        , s"hasError = $hasErrorE"
        , hasErrorE)
      control.RegisterTestPlanFeature("warning_Exclude", tbExecE)

      //------------------------------------------------------------------------------------------------------------
      saveTestplanCount(
        dqControlIdE
        , TestPlanGroup
        , s"ERROR - Warning_Exclude_Total"
        , "Valida cantiadad total de warning_exclude"
        , tblDataF100EDQ
        , null
        , "WARNING_EXCLUDE"
        , null
        , 1L
      )

      //------------------------------------------------------------------------------------------------------------
      saveTestplanCount(
        dqControlIdE
        , TestPlanGroup
        , "ERROR - Warning_Total"
        , "Valida cantiadad total de warning"
        , tblDataF100EDQ
        , null
        , "WARNING"
        , null
        , 1L
      )

      //------------------------------------------------------------------------------------------------------------
      saveTestplanCount(
        dqControlIdE
        , TestPlanGroup
        , "ERROR - ERROR_Total"
        , "Valida cantiadad total de error"
        , tblDataF100EDQ
        , null
        , "ERROR"
        , null
        , 24L
      )

      testPlanCases(dqControlIdE, TestPlanGroup,"E", tblDataF100EDQ, "ERROR")

      //------------------------------------------------------------------------------------------------------------
      //------------------------------------------------------------------------------------------------------------
      val dataF100ControlWexW: huemul_Control = prc_dataf100_WEX_W.processMaster(huemulBigDataGov: huemul_BigDataGovernance, control, 2020, 1)

      val hasErrorWexW = dataF100ControlWexW.Control_Error.ControlError_IsError
      val dqControlIdWexW = dataF100ControlWexW.Control_Id
      val tblDataF100WexWDQ = s"$outputDatabase.dataf100_WEX_W_dq"

      //------------------------------------------------------------------------------------------------------------
      control.NewStep(s"Valida que el process ejecute ok")
      val tbExecWexW = control.RegisterTestPlan(
        TestPlanGroup
        , "WEX_W - Process_Run"
        , ""
        , "hasError = true"
        , s"hasError = $hasErrorWexW"
        , hasErrorWexW)
      control.RegisterTestPlanFeature("warning-warning_Exclude", tbExecWexW)

      //------------------------------------------------------------------------------------------------------------
      saveTestplanCount(
        dqControlIdWexW
        , TestPlanGroup
        , s"WEX_W - Warning_Exclude_Total"
        , "Valida cantidad total de warning_exclude"
        , tblDataF100WexWDQ
        , null
        , "WARNING_EXCLUDE"
        , null
        , 6L
      )

      //------------------------------------------------------------------------------------------------------------
      saveTestplanCount(
        dqControlIdWexW
        , TestPlanGroup
        , "WEX_W - Warning_Total"
        , "Valida cantidad total de warning"
        , tblDataF100WexWDQ
        , null
        , "WARNING"
        , null
        , 13L
      )

      //------------------------------------------------------------------------------------------------------------
      saveTestplanCount(
        dqControlIdWexW
        , TestPlanGroup
        , "WEX_W - ERROR_Total"
        , "Valida cantidad total de error"
        , tblDataF100WexWDQ
        , null
        , "ERROR"
        , null
        , 1L
      )

      //------------------------------------------------------------------------------------------------------------
      saveTestplanCount(
        dqControlIdWexW
        , TestPlanGroup
        , "WEX_W_WARNING_LENGTH"
        , "Valida registros DQ MIN LENGTH"
        , tblDataF100WexWDQ
        , "LENGTHCOL"
        , "WARNING"
        , 1020
        , 2L
      )

      saveTestplanCount(
        dqControlIdWexW
        , TestPlanGroup
        , "WEX_W_WARNING_EXCLUDE_LENGTH"
        , "Valida registros DQ MAX LENGTH"
        , tblDataF100WexWDQ
        , "LENGTHCOL"
        , "WARNING_EXCLUDE"
        , 1020
        , 1L
      )

      //------------------------------------------------------------------------------------------------------------
      saveTestplanCount(
        dqControlIdWexW
        , TestPlanGroup
        , "WEX_W_WARNING_EXCLUDE_DECIMAL"
        , "Valida registros DQ MAX DECIMAL"
        , tblDataF100WexWDQ
        , "DECIMALCOL"
        , "WARNING_EXCLUDE"
        , 1021
        , 1L
      )

      saveTestplanCount(
        dqControlIdWexW
        , TestPlanGroup
        , "WEX_W_WARNING_DECIMAL"
        , "Valida registros DQ MIN DECIMAL"
        , tblDataF100WexWDQ
        , "DECIMALCOL"
        , "WARNING"
        , 1021
        , 3L
      )

      //------------------------------------------------------------------------------------------------------------
      saveTestplanCount(
        dqControlIdWexW
        , TestPlanGroup
        , "WEX_W_WARNING_EXCLUDE_DATE"
        , "Valida registros DQ MAX DATE"
        , tblDataF100WexWDQ
        , "DATECOL"
        , "WARNING_EXCLUDE"
        , 1022
        , 3L
      )

      saveTestplanCount(
        dqControlIdWexW
        , TestPlanGroup
        , "WEX_W_WARNING_DATE"
        , "Valida registros DQ MIN DATE"
        , tblDataF100WexWDQ
        , "DATECOL"
        , "WARNING"
        , 1022
        , 1L
      )

      //----------------------------------------------------------------------------
      //----------------------------------------------------------------------------
      val dataF100ControlWEXTrx: huemul_Control = prc_dataf100_WEX_TRX.processMaster(huemulBigDataGov: huemul_BigDataGovernance, control, 2020, 1)

      val hasErrorWEXTrx = dataF100ControlWEXTrx.Control_Error.ControlError_IsError
      val dqControlIdWEXTrx = dataF100ControlWEXTrx.Control_Id
      println(s"dqControlId=$dqControlIdWEX")
      val tblDataF100WexTrxDQ = s"$outputDatabase.dataf100_wex_trx_dq"
      val tblDataF100WexTrx = s"$outputDatabase.dataf100_wex_trx"

      //------------------------------------------------------------------------------------------------------------
      control.NewStep("WARNING_EXCLUDE - Valida que el process ejecute ok")
      val tbExecTrx = control.RegisterTestPlan(
        TestPlanGroup
        , "WEX_TRX-Process_Run"
        , ""
        , "hasError = false"
        , s"hasError = $hasErrorWEXTrx"
        , !hasErrorWEXTrx)
      control.RegisterTestPlanFeature("warning_Exclude", tbExecTrx)

      //------------------------------------------------------------------------------------------------------------
      saveTestplanCount(
        dqControlIdWEXTrx
        , TestPlanGroup
        , "\"WEX_TRX-WARNING_EXCLUDE - Warning_Exclude_Total"
        , "Valida cantidad total de warning_exclude"
        , tblDataF100WexTrxDQ
        , null
        , "WARNING_EXCLUDE"
        , null
        , 24L
      )

      //------------------------------------------------------------------------------------------------------------
      saveTestplanCount(
        dqControlIdWEXTrx
        , TestPlanGroup
        , "\"WEX_TRX-WARNING_EXCLUDE - Warning_Total"
        , "Valida cantidad total de warning"
        , tblDataF100WexTrxDQ
        , null
        , "WARNING"
        , null
        , 1L
      )

      //------------------------------------------------------------------------------------------------------------
      saveTestplanCount(
        dqControlIdWEXTrx
        , TestPlanGroup
        , "WEX_TRX-WARNING_EXCLUDE - ERROR_Total"
        , "Valida cantidad total de error"
        , tblDataF100WexTrxDQ
        , null
        , "ERROR"
        , null
        , 0L
      )

      testPlanCases(dqControlIdWEXTrx, TestPlanGroup,"WEX_TRX", tblDataF100WexTrxDQ,"WARNING_EXCLUDE")

      control.NewStep(s"Valida Cantida de registros cargados")
      val dfSQL = huemulBigDataGov.spark.sql(
        s"""select count(*) as cc
           | from (select DISTINCT pkcol1,pkcol2 from $tblDataF100WexTrx) A
           | where pkcol1 = 1 and (pkcol2 = 3 or pkcol2 = 7)
         """.stripMargin)

      val testplanReg = control.RegisterTestPlan(
        TestPlanGroup
        , "WEX_TRX-WARNING_EXCLUDE - Registros Cargados"
        , "Valida Cantida de registros cargados"
        , s"count = 2"
        , s"count = ${dfSQL.take(1)(0).getLong(0)}"
        , dfSQL.take(1)(0).getLong(0) == 2)



      if (control.TestPlan_CurrentIsOK(78)) //null todo oks
      println("Resultado ok")
        else
        println("Resultado ERROR")


    } catch {
      case e:Throwable =>
        println(e)
        println(e.printStackTrace())
        control.TestPlan_CurrentIsOK(-1)
    }

    huemulBigDataGov.close()

  }

  /** Genera casos de pruebas de count para una columna especifica, tipo de notificacion y valor esperado
   *
   * @param dqControlId     dqControlId
   * @param testPlanGroup   testPlanGroup
   * @param testPlanName    testPlanName
   * @param testPlanDesc    testPlanDesc
   * @param tableName       tableName
   * @param columnName      columnName
   * @param notification    notification
   * @param errorCode       errorCode
   * @param resultExpected  resultExpected
   */
  private def saveTestplanCount( dqControlId:String
                                 , testPlanGroup:String
                                 , testPlanName:String
                                 , testPlanDesc:String
                                 , tableName:String
                                 , columnName:String
                                 , notification:String
                                 , errorCode:Integer
                                 , resultExpected:Long):Unit = {
    control.NewStep(s"Valida $notification - $testPlanName")
    val dfSQL = huemulBigDataGov.spark.sql(
      s"""select count(*) as cc
         | from $tableName
         | where dq_control_id = '$dqControlId'
         | ${if (columnName != null) s"and lower(dq_error_columnname)='${columnName.toLowerCase()}'" else "" }
         | ${if (errorCode != null) s"and dq_error_code=$errorCode" else "" }
         | and dq_error_notification='$notification' """.stripMargin)

    val testplan = control.RegisterTestPlan(
      testPlanGroup
      , testPlanName
      , testPlanDesc
      , s"count = $resultExpected"
      , s"count = ${dfSQL.take(1)(0).getLong(0)}"
      , dfSQL.take(1)(0).getLong(0) == resultExpected)

    control.RegisterTestPlanFeature(notification, testplan)
  }

  /** Pruebas reproticas de vaidacion egun tipo de notificacion
   *
   * @param dqControlId       dqControlId
   * @param TestPlanGroup     TestPlanGroup
   * @param tblDataF100WexDQ  tblDataF100WexDQ
   * @param notification      notification
   */
  private def testPlanCases(dqControlId: String
                            , TestPlanGroup: String
                            , testPlanId:String
                            , tblDataF100WexDQ: String
                            , notification: String) = {
    //------------------------------------------------------------------------------------------------------------
    saveTestplanCount(
      dqControlId
      , TestPlanGroup
      , testPlanId +"-"+ notification + "_PK"
      , "valida registros duplicados PK excluido"
      , tblDataF100WexDQ
      , "PKCOL1"
      , notification
      , null
      , 4L
    )

    //------------------------------------------------------------------------------------------------------------
    saveTestplanCount(
      dqControlId
      , TestPlanGroup
      , testPlanId +"-"+notification + "_UK"
      , "valida registros duplicados UK excluido"
      , tblDataF100WexDQ
      , "UNIQUECOL"
      , notification
      , null
      , 2L
    )

    //------------------------------------------------------------------------------------------------------------
    saveTestplanCount(
      dqControlId
      , TestPlanGroup
      , testPlanId +"-"+notification + "_Null"
      , "Valida registros DQ NULL"
      , tblDataF100WexDQ
      , "VACIOCOL"
      , notification
      , 1023
      , 2L
    )

    //------------------------------------------------------------------------------------------------------------
    saveTestplanCount(
      dqControlId
      , TestPlanGroup
      , testPlanId +"-"+notification + "_LENGTH"
      , "Valida registros DQ LENGTH"
      , tblDataF100WexDQ
      , "LENGTHCOL"
      , notification
      , 1020
      , 3L
    )

    //------------------------------------------------------------------------------------------------------------
    saveTestplanCount(
      dqControlId
      , TestPlanGroup
      , testPlanId +"-"+notification + "_DECIMAL"
      , "Valida registros DQ DECIMAL"
      , tblDataF100WexDQ
      , "DECIMALCOL"
      , notification
      , 1021
      , 4L
    )

    //------------------------------------------------------------------------------------------------------------
    saveTestplanCount(
      dqControlId
      , TestPlanGroup
      , testPlanId +"-"+notification + "_DATE"
      , "Valida registros DQ DATE"
      , tblDataF100WexDQ
      , "DATECOL"
      , notification
      , 1022
      , 4L
    )


    //------------------------------------------------------------------------------------------------------------
    saveTestplanCount(
      dqControlId
      , TestPlanGroup
      , testPlanId +"-"+notification + "_REGEX"
      , "Valida registros DQ REGEX"
      , tblDataF100WexDQ
      , "REGEXCOL"
      , notification
      , 1041
      , 1L
    )

    //------------------------------------------------------------------------------------------------------------
    saveTestplanCount(
      dqControlId
      , TestPlanGroup
      , testPlanId +"-"+notification + "_HIERARCHYTEST01_ERROR"
      , "Valida jerarquia de notificacion WEX-ERROR"
      , tblDataF100WexDQ
      , "HIERARCHYTEST01_E"
      , notification
      , 1020
      , 1L
    )

    //------------------------------------------------------------------------------------------------------------
    saveTestplanCount(
      dqControlId
      , TestPlanGroup
      , testPlanId +"-"+notification + "_HIERARCHYTEST01_WARNING"
      , "Valida jerarquia de notificacion WEX-WARNING"
      , tblDataF100WexDQ
      , "HIERARCHYTEST01_W"
      , notification
      , 1020
      , 1L
    )

    //------------------------------------------------------------------------------------------------------------
    saveTestplanCount(
      dqControlId
      , TestPlanGroup
      , testPlanId +"-"+notification + "_HIERARCHYTEST01_WEX"
      , "Valida jerarquia de notificacion WEX-WEX"
      , tblDataF100WexDQ
      , "HIERARCHYTEST01_WEX"
      , notification
      , 1020
      , 1L
    )

    //    //------------------------------------------------------------------------------------------------------------
    //    saveTestplanCount(
    //      dqControlId
    //      , TestPlanGroup
    //      , testPlanId +"-"+notification + "_HIERARCHYTEST02_ERROR"
    //      , "Valida jerarquia de notificacion NONE-ERROR"
    //      , tblDataF100WexDQ
    //      , "HIERARCHYTEST02_E"
    //      , "ERROR"
    //      , 1020
    //      , 1L
    //    )

    //------------------------------------------------------------------------------------------------------------
    saveTestplanCount(
      dqControlId
      , TestPlanGroup
      , testPlanId +"-"+notification + "_HIERARCHYTEST02_WARNING"
      , "Valida jerarquia de notificacion NONE-WARNING"
      , tblDataF100WexDQ
      , "HIERARCHYTEST02_W"
      , "WARNING"
      , 1020
      , 1L
    )

    //------------------------------------------------------------------------------------------------------------
    saveTestplanCount(
      dqControlId
      , TestPlanGroup
      , testPlanId +"-"+notification + "_HIERARCHYTEST02_WEX"
      , "Valida jerarquia de notificacion NONE-WEX"
      , tblDataF100WexDQ
      , "HIERARCHYTEST02_WEX"
      , "WARNING_EXCLUDE"
      , 1020
      , 1L
    )
  }

  private def dataGeneration(huemulBigDataGov: huemul_BigDataGovernance): Unit = {
    val path = huemulBigDataGov.GlobalSettings.GetPath(huemulBigDataGov,huemulBigDataGov.GlobalSettings.RAW_BigFiles_Path )
    println(s"INFO path : $path")

    val pathTemp = huemulBigDataGov.GlobalSettings.GetPath(huemulBigDataGov,huemulBigDataGov.GlobalSettings.TEMPORAL_Path )
    println(s"INFO path : $pathTemp")
    //Genera archivo de pruebas
    val someData = Seq(
      Row(1,1,1,null,"1",1,Date.valueOf("2020-06-01"),"AAAA",1,1),
      Row(1,2,2,1,"12",12,Date.valueOf("2020-06-02"),"AAAB",12,12),
      Row(1,1,3,2,"123",123,Date.valueOf("2020-06-03"),"AABB",123,123),
      Row(1,3,7,3,"1234",1234,Date.valueOf("2020-06-04"),"ABBB",1234,1234),
      Row(1,4,4,null,"12345",12345,Date.valueOf("2020-06-05"),"ABBB",12345,12345),
      Row(1,4,4,4,"1234567",1234567,Date.valueOf("2020-06-06"),"CCAA",1234567,1234567),
      Row(1,5,5,5,"12345678",12345678,Date.valueOf("2020-06-07"),"",12345678,12345678),
      Row(1,6,6,6,"1234567",1234567,Date.valueOf("2020-06-02"),"AAAA",123456789,12345678),
      Row(1,7,8,8,"1234567",1234567,Date.valueOf("2020-06-02"),"AAAA",12345678,123456789),
      Row(1,8,9,6,"1234567",1234567,Date.valueOf("2020-06-02"),"AAAA",12345678,1234567890)
    )

    val someSchema = List(
      StructField("PKCOL1", IntegerType, nullable = true),
      StructField("PKCOL2", IntegerType, nullable = true),
      StructField("UNIQUECOL", IntegerType, nullable = true),
      StructField("NULLCOL", IntegerType, nullable = true),
      StructField("LENGTHCOL", StringType, nullable = true),
      StructField("DECIMALCOL", IntegerType, nullable = true),
      StructField("DATECOL", DateType, nullable = true),
      StructField("REGEXCOL", StringType, nullable = true),
      StructField("HIERARCHYTEST01", IntegerType, nullable = true),
      StructField("HIERARCHYTEST02", IntegerType, nullable = true)
    )

    val someDF = huemulBigDataGov.spark.createDataFrame(
      huemulBigDataGov.spark.sparkContext.parallelize[Row](someData),
      StructType(someSchema)
    )

    //Need save to hdfs path with filename TestDataF100.csv
    val fileTmp= s"${pathTemp}TestDataF100.tmp"
    val fileOut= s"${path}poc/TestDataF100.csv"

    someDF
      .coalesce(1)
      .write
      .format("csv")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(fileTmp)


    println(s"fileTmp=$fileTmp")
    println(s"fileOut=$fileOut")

    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)

    val pathFiles = new Path(fileTmp)
    val fileNames = hdfs.listFiles(pathFiles, false)
    var fileNamesList = scala.collection.mutable.MutableList[String]()
    while (fileNames.hasNext) {
      fileNamesList += fileNames.next().getPath.getName
    }

    val partFileName = fileNamesList.filterNot(filenames => filenames == "_SUCCESS")
    println(s"partFileName=${partFileName.mkString}")
    val partFileSourcePath = new Path(fileTmp+"/"+partFileName.mkString)
    val desiredCsvTargetPath = new Path(fileOut)

    hdfs.delete(desiredCsvTargetPath, true)
    hdfs.rename(partFileSourcePath , desiredCsvTargetPath)

    val partFileSourcePathTmp = new Path(fileTmp)
    hdfs.delete(partFileSourcePathTmp, true)
  }
}
