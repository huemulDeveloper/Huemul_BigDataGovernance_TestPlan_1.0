package com.huemulsolutions.bigdata.test

import java.util.Calendar

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.raw.raw_dataf100
import com.huemulsolutions.bigdata.tables.huemulType_StorageType
import com.huemulsolutions.bigdata.tables.huemulType_StorageType.huemulType_StorageType
import com.huemulsolutions.bigdata.tables.master.dataf100_WEX_TRX

/**
 *
 */
object prc_dataf100_WEX_TRX {

  /**
   * Main que se ejecuta cuando se llama el JAR desde spark2-submit. el código esta preparado para hacer re-procesamiento masivo
   *
   * @param args  Parámetros de invocación
   */
  def main(args : Array[String]) {
    val arguments: huemul_Args = new huemul_Args()
    arguments.setArgs(args)



    val line="*********************************************************************************************************"
    println(line)
    val huemulBigDataGov  = new huemul_BigDataGovernance(s"Masterizacion tabla <paises> - ${this.getClass.getSimpleName}", args, com.yourcompany.settings.globalSettings.Global)
    println(line)

    /*************** PARÁMETROS **********************/

    println(huemulBigDataGov.spark.sparkContext.getConf.getInt("spark.executor.instances", 1))
    huemulBigDataGov.spark.sparkContext.getExecutorMemoryStatus.foreach(println)

    println(line)

    var paramAno = huemulBigDataGov.arguments.GetValue("ano", null,"Debe especificar ano de proceso: ejemplo: ano=2017").toInt
    var paramMes = huemulBigDataGov.arguments.GetValue("mes", null,"Debe especificar mes de proceso: ejemplo: mes=12").toInt

    val paramDia:Int = 1

    val paramNumMeses = huemulBigDataGov.arguments.GetValue("num_meses", "1").toInt

    /*************** CICLO RE-PROCESO MASIVO **********************/
    var i: Int = 1
    val Fecha = huemulBigDataGov.setDateTime(paramAno, paramMes, paramDia, 0, 0, 0)

    while (i <= paramNumMeses) {
      paramAno = huemulBigDataGov.getYear(Fecha)
      paramMes = huemulBigDataGov.getMonth(Fecha)
      println(s"Procesando Año $paramAno, Mes $paramMes ($i de $paramNumMeses)")

      //Ejecuta código
      val finControl = processMaster(huemulBigDataGov, null, paramAno, paramMes)

      if (finControl.Control_Error.IsOK())
        i+=1
      else {
        println(s"ERROR Procesando Año $paramAno, Mes $paramMes ($i de $paramNumMeses)")
        i=paramNumMeses+1
      }

      Fecha.add(Calendar.MONTH, 1)
    }

    huemulBigDataGov.close
  }

  /**
   * Proceso principal que procesa la reglas de carga de la fuente raw_nation
   *
   * @param huemulBigDataGov  Clase inicial de la librería Huemul Big Data Governance
   * @param ControlParent     Clase que posibilita la integración del desarrollo con el gobierno de datos
   * @param paramAno         Año del archivo a procesar
   * @param paramMes         Mes del archivo a procesar
   * @return                  Retorna clases huemul_Control
   */
  def processMaster(huemulBigDataGov: huemul_BigDataGovernance, ControlParent: huemul_Control
                    , paramAno: Integer, paramMes: Integer): huemul_Control = {
    val Control = new huemul_Control(huemulBigDataGov, ControlParent,  huemulType_Frequency.MONTHLY)

    try {
      /*************** AGREGAR PARÁMETROS A CONTROL **********************/
      Control.AddParamYear("param_ano", paramAno)
      Control.AddParamMonth("param_mes", paramMes)

      /*************** ABRE RAW DESDE DATA LAKE **********************/
      Control.NewStep("Abre DataLake")
      val dfRawDataF100 =  new raw_dataf100(huemulBigDataGov, Control)
      if (!dfRawDataF100.open("raw_dataf100", Control, paramAno, paramMes, 1, 0, 0, 0))
        Control.RaiseError(s"error encontrado, abortar: ${dfRawDataF100.Error.ControlError_Message}")


      val TipoTablaParam: String = huemulBigDataGov.arguments.GetValue("TipoTabla", null, "Debe especificar TipoTabla (ORC,PARQUET,HBASE,DELTA)")
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
      /********************************************************/
      /*************** LÓGICA DE NEGOCIO **********************/
      /********************************************************/

      //instancia de clase <paises>
      Control.NewStep("Asocia columnas de la tabla ")
      val tblDataF100Wex = new dataf100_WEX_TRX(huemulBigDataGov, Control,TipoTabla)
      tblDataF100Wex.DF_from_RAW(dfRawDataF100,"dataf100_WEX")
      tblDataF100Wex.PKCOL1.setMapping("PKCOL1")
      tblDataF100Wex.PKCOL2.setMapping("PKCOL2")
      tblDataF100Wex.UNIQUECOL.setMapping("UNIQUECOL")
      tblDataF100Wex.VACIOCOL.setMapping("NULLCOL")
      tblDataF100Wex.LENGTHCOL.setMapping("LENGTHCOL")
      tblDataF100Wex.DECIMALCOL.setMapping("DECIMALCOL")
      tblDataF100Wex.DATECOL.setMapping("DATECOL")
      tblDataF100Wex.REGEXCOL.setMapping("REGEXCOL")
      tblDataF100Wex.HIERARCHYTEST01_E.setMapping("HIERARCHYTEST01")
      tblDataF100Wex.HIERARCHYTEST01_W.setMapping("HIERARCHYTEST01")
      tblDataF100Wex.HIERARCHYTEST01_WEX.setMapping("HIERARCHYTEST01")
      //tblDataF100Wex.HIERARCHYTEST02_E.setMapping("HIERARCHYTEST02")
      tblDataF100Wex.HIERARCHYTEST02_W.setMapping("HIERARCHYTEST02")
      tblDataF100Wex.HIERARCHYTEST02_WEX.setMapping("HIERARCHYTEST02")

      tblDataF100Wex.setApplyDistinct(false) //deshabilitar si DF tiene datos únicos, por default está habilitado

      Control.NewStep("Ejecuta Proceso carga <paises>")
      if (!tblDataF100Wex.executeFull("FinalSaved"))
        Control.RaiseError(s"User: Error al intentar masterizar los datos (${tblDataF100Wex.Error_Code}): ${tblDataF100Wex.Error_Text}")

      Control.FinishProcessOK
    } catch {
      case e: Exception =>
        Control.Control_Error.GetError(e, this.getClass.getName, null)
        Control.FinishProcessError()
    }

    Control
  }
}
