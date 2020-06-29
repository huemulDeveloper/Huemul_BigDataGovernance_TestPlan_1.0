package com.huemulsolutions.bigdata.raw

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.datalake._
import org.apache.spark.sql.types.{DateType, DecimalType, IntegerType, StringType}

/**
 * Clase que abre archivo RAW nation
 *
 * @param huemulBigDataGov  Clase inicial de la librería Huemul Big Data Governance
 * @param Control           Clase que posibilita la integración del desarrollo con el gobierno de datos
 */
class raw_dataf100(huemulBigDataGov: huemul_BigDataGovernance, Control: huemul_Control)
  extends huemul_DataLake(huemulBigDataGov, Control) with Serializable  {

  this.Description = "Data F100"
  this.GroupName = "poc"
  this.setFrequency(huemulType_Frequency.MONTHLY)

  //Crea variable para configuración de lectura del archivo
  val CurrentSetting = new huemul_DataLakeSetting(huemulBigDataGov)
  //configurar la fecha de vigencia de esta configuración
  CurrentSetting.StartDate = huemulBigDataGov.setDateTime(2010,1,1,0,0,0)
  CurrentSetting.EndDate = huemulBigDataGov.setDateTime(2050,12,12,0,0,0)

  //Configuración de rutas globales
  CurrentSetting.GlobalPath = huemulBigDataGov.GlobalSettings.RAW_SmallFiles_Path
  //Configura ruta local, se pueden usar comodines
  CurrentSetting.LocalPath = "planPruebas/"
  //configura el nombre del archivo (se pueden usar comodines)
  CurrentSetting.FileName = "TestDataF100.csv"
  //especifica el tipo de archivo a leer
  CurrentSetting.FileType = huemulType_FileType.TEXT_FILE
  //especifica el nombre del contacto del archivo en TI
  CurrentSetting.ContactName = "Christian Sattler"

  //Indica como se lee el archivo
  CurrentSetting.DataSchemaConf.ColSeparatorType = huemulType_Separator.CHARACTER  //POSITION;CHARACTER
  //separador de columnas
  CurrentSetting.DataSchemaConf.ColSeparator = ","    //SET FOR CHARACTER

  CurrentSetting.DataSchemaConf.AddColumns("PKCOL1", "", IntegerType, "")
  CurrentSetting.DataSchemaConf.AddColumns("PKCOL2", "", IntegerType, "")
  CurrentSetting.DataSchemaConf.AddColumns("UNIQUECOL", "", IntegerType, "")
  CurrentSetting.DataSchemaConf.AddColumns("NULLCOL", "", IntegerType, "")
  CurrentSetting.DataSchemaConf.AddColumns("LENGTHCOL", "", StringType, "")
  CurrentSetting.DataSchemaConf.AddColumns("DECIMALCOL", "", IntegerType, "")
  CurrentSetting.DataSchemaConf.AddColumns("DATECOL", "", DateType, "")
  CurrentSetting.DataSchemaConf.AddColumns("REGEXCOL", "", StringType, "")
  CurrentSetting.DataSchemaConf.AddColumns("HIERARCHYTEST01", "", StringType, "")
  CurrentSetting.DataSchemaConf.AddColumns("HIERARCHYTEST02", "", StringType, "")

  //Configurar de lectura de información de Log (en caso de tener, si no tiene se configura huemulType_Separator.NONE)
  CurrentSetting.LogSchemaConf.ColSeparatorType = huemulType_Separator.CHARACTER
  CurrentSetting.LogSchemaConf.ColSeparator=","
  CurrentSetting.LogSchemaConf.setHeaderColumnsString("HEADER")
  CurrentSetting.LogNumRows_FieldName = null


  this.SettingByDate.append(CurrentSetting)

  /**
   * Método que retorna una estructura con un DF de detalle, y registros de control
   *
   * @param Alias         Alias del DataFrame SQL
   * @param ControlParent ControlParent
   * @param year          Año del archivo
   * @param month         Mes del archivo
   * @param day           Dia del archivo
   * @param hour          Hora del archivo
   * @param min           Minuto del archivo
   * @param seg           Segundo Hora del archivo
   * @return              Boolean: True si la apertura del archivo fue exitosa
   */
  def open(Alias: String, ControlParent: huemul_Control
           , year: Integer, month: Integer, day: Integer, hour: Integer, min: Integer, seg: Integer): Boolean = {
    //Crea registro de control de procesos
    val control = new huemul_Control(huemulBigDataGov, ControlParent, huemulType_Frequency.ANY_MOMENT)

    //Guarda los parámetros importantes en el control de procesos
    control.AddParamYear("Ano", year)
    control.AddParamMonth("Mes", month)

    try {
      //NewStep va registrando los pasos de este proceso, también sirve como documentación del mismo.
      control.NewStep("Abre archivo RDD y devuelve esquemas para transformar a DF")
      if (!this.OpenFile(year, month, day, hour, min, seg, null)){
        //Control también entrega mecanismos de envío de excepciones
        control.RaiseError(s"Error al abrir archivo: ${this.Error.ControlError_Message}")
      }

      //Si el archivo no tiene cabecera, comentar la línea de .filter
      control.NewStep("Aplicando Filtro")
      val rowRDD = this.DataRDD
        //filtro para dejar fuera la primera fila
        .filter { x => x != this.Log.DataFirstRow  }
        .map { x => this.ConvertSchema(x) }

      //Crea DataFrame en Data.DataDF
      control.NewStep("Transformando datos a DataFrame")
      this.DF_from_RAW(rowRDD, Alias)

      //************************
      //**** VALIDACIÓN DQ *****
      //************************
      control.NewStep("Valida que cantidad de registros esté entre 10 y 500")
      //validación cantidad de filas
      val validaNumFilas = this.DataFramehuemul.DQ_NumRowsInterval(this, 1, 100)
      if (validaNumFilas.isError) control.RaiseError(s"user: Numero de Filas fuera del rango. ${validaNumFilas.Description}")

      control.FinishProcessOK
    } catch {
      case e: Exception =>
        control.Control_Error.GetError(e, this.getClass.getName, null)
        control.FinishProcessError()
    }
    control.Control_Error.IsOK()
  }
}

//---------------------------------------------------------------------------------------------------------------------
/**
 * Este objeto se utiliza solo para probar la lectura del archivo RAW.
 * La clase que está definida más abajo se utiliza para la lectura.
 */
object raw_dataf100_test {

  /**
   * Proceso main que permite  probar la configuración del archivo RAW
   *
   * @param args   Parámetros de invocación
   */
  def main(args : Array[String]) {


    //Creación API
    val huemulBigDataGov  = new huemul_BigDataGovernance(s"Testing DataLake - ${this.getClass.getSimpleName}", args, com.yourcompany.settings.globalSettings.Global)

    //Creación del objeto control, por default no permite ejecuciones en paralelo del mismo objeto (corre en modo SINGLETON)
    val Control = new huemul_Control(huemulBigDataGov, null, huemulType_Frequency.MONTHLY )

    /*************** PARÁMETROS **********************/
    val param_year = huemulBigDataGov.arguments.GetValue("year", null
      , "Debe especificar el parámetro año, ej: year=2017").toInt
    val param_month = huemulBigDataGov.arguments.GetValue("month", null
      , "Debe especificar el parámetro mes, ej: month=12").toInt

    //Inicializa clase RAW
    val DF_RAW =  new raw_dataf100(huemulBigDataGov, Control)
    if (!DF_RAW.open("DF_RAW", null, param_year, param_month, 0, 0, 0, 0)) {
      huemulBigDataGov.log_info.error("************************************************************")
      huemulBigDataGov.log_info.error("**********  E  R R O R   E N   P R O C E S O   *************")
      huemulBigDataGov.log_info.error("************************************************************")

    } else
      DF_RAW.DataFramehuemul.DataFrame.show()

    Control.FinishProcessOK
    huemulBigDataGov.close()

  }
}
