package com.huemulsolutions.bigdata.raw

import com.huemulsolutions.bigdata.datalake._
import com.huemulsolutions.bigdata.datalake.huemulType_FileType;
import com.huemulsolutions.bigdata.datalake.huemulType_Separator;
import com.huemulsolutions.bigdata.datalake.huemul_DataLake;
import com.huemulsolutions.bigdata.datalake.huemul_DataLakeSetting;
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import org.apache.spark.sql.types._

import com.huemulsolutions.bigdata.control.huemulType_Frequency.huemulType_Frequency

class raw_DatosOldValue(huemulLib: huemul_BigDataGovernance, Control: huemul_Control) extends huemul_DataLake(huemulLib, Control) with Serializable  {
   this.Description = "datos para probar funcionalidades de Old VAlue Trace"
   this.GroupName = "HuemulPlanPruebas"
      
   val FormatSetting = new huemul_DataLakeSetting(huemulLib)
    FormatSetting.StartDate = huemulLib.setDateTime(2010,1,1,0,0,0)
    FormatSetting.EndDate = huemulLib.setDateTime(2050,12,12,0,0,0)

    //Path info
    FormatSetting.GlobalPath = huemulLib.GlobalSettings.RAW_BigFiles_Path
    FormatSetting.LocalPath = "planPruebas/"
    FormatSetting.FileName = "OldValueTrace_{{TipoArchivo}}.txt"
    FormatSetting.FileType = huemulType_FileType.TEXT_FILE
    FormatSetting.ContactName = "Sebastián Rodríguez"
    
    //Columns Info CHARACTER
    
    //PLAN EJECUCION 1:
    /*
    FormatSetting.DataSchemaConf.ColSeparatorType = huemulType_Separator.CHARACTER  //POSITION;CHARACTER
    FormatSetting.DataSchemaConf.ColSeparator = "\\|"    //SET FOR CARACTER
    FormatSetting.DataSchemaConf.setHeaderColumnsString("TipoValor;IntValue;BigIntValue;SmallIntValue;TinyIntValue;DecimalValue;RealValue;FloatValue;StringValue;charValue;timeStampValue") //siempre con ;
    *  
    */
    
    //PLAN EJECUCION 2:
    /*
    FormatSetting.DataSchemaConf.ColSeparatorType = huemulType_Separator.CHARACTER  //POSITION;CHARACTER
    FormatSetting.DataSchemaConf.ColSeparator = "\\|"    //SET FOR CARACTER
    
    FormatSetting.DataSchemaConf.AddColumns("TipoValor", "TipoValor_ti", StringType)
    FormatSetting.DataSchemaConf.AddColumns("IntValue", "IntValue_ti", IntegerType)
    FormatSetting.DataSchemaConf.AddColumns("BigIntValue", "BigIntValue_ti", LongType, "con descripción mia")
    FormatSetting.DataSchemaConf.AddColumns("SmallIntValue", "SmallIntValue_ti", ShortType)
    FormatSetting.DataSchemaConf.AddColumns("TinyIntValue", "TinyIntValue_ti", ShortType)
    FormatSetting.DataSchemaConf.AddColumns("DecimalValue", "DecimalValue_ti", DecimalType(10,4))
    FormatSetting.DataSchemaConf.AddColumns("RealValue", "RealValue_ti", DoubleType)
    FormatSetting.DataSchemaConf.AddColumns("FloatValue", "FloatValue_ti", FloatType)
    FormatSetting.DataSchemaConf.AddColumns("StringValue", "StringValue_ti", StringType)
    FormatSetting.DataSchemaConf.AddColumns("charValue", "charValue_ti", StringType)
    FormatSetting.DataSchemaConf.AddColumns("timeStampValue", "timeStampValue_ti", TimestampType)
    * 
    */
    
    //PLAN EJECUCION 3:
    FormatSetting.DataSchemaConf.ColSeparatorType = huemulType_Separator.CHARACTER  //POSITION;CHARACTER
    FormatSetting.DataSchemaConf.ColSeparator = "\\|"
    
    FormatSetting.DataSchemaConf.AddColumns("codigo", "codigo", StringType,"")
    FormatSetting.DataSchemaConf.AddColumns("descripcion", "descripcion_ti", IntegerType,"")
    FormatSetting.DataSchemaConf.AddColumns("fecha", "fecha_ti", TimestampType,"")
    FormatSetting.DataSchemaConf.AddColumns("monto", "monto_ti", IntegerType,"")
    
    
    //Log Info
    FormatSetting.LogSchemaConf.ColSeparatorType = huemulType_Separator.CHARACTER  //POSITION;CHARACTER;NONE
    FormatSetting.LogNumRows_FieldName = null
    //Fields Info for CHARACTER
    FormatSetting.LogSchemaConf.ColSeparator = "|"    //SET FOR CARACTER
    FormatSetting.LogSchemaConf.setHeaderColumnsString("VACIO") //Fielda;Fieldb;fieldc
    
    this.SettingByDate.append(FormatSetting)
  
    /***
   * open(ano: Int, mes: Int) <br>
   * método que retorna una estructura con un DF de detalle, y registros de control <br>
   * ano: año de los archivos recibidos <br>
   * mes: mes de los archivos recibidos <br>
   * dia: dia de los archivos recibidos <br>
   * Retorna: true si todo está OK, false si tuvo algún problema <br>
  */
  def open(Alias: String, ControlParent: huemul_Control, ano: Integer, mes: Integer, dia: Integer, hora: Integer, min: Integer, seg: Integer, TipoArchivo: String, AplicarTrim: Boolean = true): Boolean = {
    val control = new huemul_Control(huemulLib, ControlParent, huemulType_Frequency.MONTHLY, false)
    //Setea parámetros
    control.AddParamYear("Ano", ano)
    control.AddParamMonth("Mes", mes)
    
    control.NewStep("Abriendo raw")
       
    try { 
      //Abre archivo RDD y devuelve esquemas para transformar a DF
      if (!this.OpenFile(ano, mes, dia, hora, min, seg, s"{{TipoArchivo}}=${TipoArchivo}")){
        control.RaiseError(s"Error al abrir archivo: ${this.Error.ControlError_Message}")
      }
      
      import huemulLib.spark.implicits._
   
      control.NewStep("Aplicando Filtro")
      /**/    //Agregar filtros o cambiar forma de leer archivo en este lugar
      this.ApplyTrim = AplicarTrim
     // this.allColumnsAsString(false)
      val rowRDD = this.DataRDD     
            .filter { x => x != this.Log.DataFirstRow  }
            .map(  x => {this.ConvertSchema(x)} )
        
            
      control.NewStep("Transformando a dataframe")      
      //Crea DataFrame en Data.DataDF
      this.DF_from_RAW(rowRDD, Alias)
        
      //****VALIDACION DQ*****
      //**********************
      
      control.NewStep("Validando cantidad de filas")      
      //validacion cantidad de filas
      val validanumfilas = this.DataFramehuemul.DQ_NumRowsInterval(this, 6,6)  
      if (validanumfilas.isError) control.RaiseError(s"user: Numero de Filas fuera del rango. ${validanumfilas.Description}")
                        
      control.FinishProcessOK                      
    } catch {
      case e: Exception => {
        control.Control_Error.GetError(e, this.getClass.getName, this, null)
        control.FinishProcessError()   
      }
    }         
    return control.Control_Error.IsOK()
  }
}




object raw_DatosOldValue {
  def main(args : Array[String]) {
    
    //Creación API
    val huemulLib  = new huemul_BigDataGovernance(s"BigData Fabrics - ${this.getClass.getSimpleName}", args, com.yourcompany.settings.globalSettings.Global)
    val Control = new huemul_Control(huemulLib, null, huemulType_Frequency.MONTHLY)
    /*************** PARAMETROS **********************/
    
    //Inicializa clase RAW  
    val DF_RAW =  new raw_DatosOldValue(huemulLib, Control)
    if (!DF_RAW.open("DF_RAW", null, 2018, 12, 31, 0, 0, 0, "ini")) {
      println("************************************************************")
      println("**********  E  R R O R   E N   P R O C E S O   *************")
      println("************************************************************")
    } else
      DF_RAW.DataFramehuemul.DataFrame.show()
      
    
    val MyName: String = this.getClass.getSimpleName
    //Cambiar los parametros:             nombre tabla hive   ,   package base , package específico
    //DF_RAW.GenerateInitialCode(MyName, "sbif_institucion_mes","bigdata.fabrics","sbif.bancos")       
    
    Control.FinishProcessOK
  }  
}