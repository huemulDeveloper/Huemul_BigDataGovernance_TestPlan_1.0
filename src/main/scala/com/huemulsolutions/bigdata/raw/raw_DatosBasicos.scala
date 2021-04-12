package com.huemulsolutions.bigdata.raw

import com.huemulsolutions.bigdata.datalake.huemulType_FileType
import com.huemulsolutions.bigdata.datalake.huemulType_Separator
import com.huemulsolutions.bigdata.datalake.huemul_DataLake
import com.huemulsolutions.bigdata.datalake.huemul_DataLakeSetting
import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import org.apache.spark.sql.types._

class raw_DatosBasicos(huemulLib: huemul_BigDataGovernance, Control: huemul_Control) extends huemul_DataLake(huemulLib, Control) with Serializable  {
   this.Description = "Datos Básicos por cada tipo de dato, para plan de pruebas"
   this.GroupName = "HuemulPlanPruebas"
      
   val FormatSetting = new huemul_DataLakeSetting(huemulLib)
    FormatSetting.StartDate = huemulLib.setDateTime(2010,1,1,0,0,0)
    FormatSetting.EndDate = huemulLib.setDateTime(2050,12,12,0,0,0)

    //Path info
    FormatSetting.GlobalPath = huemulLib.GlobalSettings.RAW_BigFiles_Path
    FormatSetting.LocalPath = "planPruebas/"
    FormatSetting.FileName = "DatosBasicos{{TipoArchivo}}.txt"
    FormatSetting.FileType = huemulType_FileType.TEXT_FILE
    FormatSetting.ContactName = "Sebastián Rodríguez"
    
    val a = 1
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
    FormatSetting.DataSchemaConf.ColSeparatorType = huemulType_Separator.POSITION  //POSITION;CHARACTER
    
    FormatSetting.DataSchemaConf.AddColumns("TipoValor", "TipoValor_ti", StringType,"",0,15)
    FormatSetting.DataSchemaConf.AddColumns("IntValue", "IntValue_ti", IntegerType,"",16,24)
    FormatSetting.DataSchemaConf.AddColumns("BigIntValue", "BigIntValue_ti", LongType, "con descripción mia",25,36)
    FormatSetting.DataSchemaConf.AddColumns("SmallIntValue", "SmallIntValue_ti", ShortType,"",37,50)
    FormatSetting.DataSchemaConf.AddColumns("TinyIntValue", "TinyIntValue_ti", ShortType,"",51,63)
    FormatSetting.DataSchemaConf.AddColumns("DecimalValue", "DecimalValue_ti", DecimalType(10,4),"",64,76)
    FormatSetting.DataSchemaConf.AddColumns("RealValue", "RealValue_ti", DoubleType,"",77,86)
    FormatSetting.DataSchemaConf.AddColumns("FloatValue", "FloatValue_ti", FloatType,"",87,97)
    FormatSetting.DataSchemaConf.AddColumns("StringValue", "StringValue_ti", StringType,"",98,110)
    FormatSetting.DataSchemaConf.AddColumns("charValue", "charValue_ti", StringType,"",111,120)
    FormatSetting.DataSchemaConf.AddColumns("timeStampValue", "timeStampValue_ti", TimestampType,"",121,140)
    
    
    //Log Info
    FormatSetting.LogSchemaConf.ColSeparatorType = huemulType_Separator.CHARACTER  //POSITION;CHARACTER;NONE
    FormatSetting.LogNumRows_FieldName = null
    //Fields Info for CHARACTER
    FormatSetting.LogSchemaConf.ColSeparator = ";"    //SET FOR CARACTER
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
      val validanumfilas = this.DataFramehuemul.DQ_NumRowsInterval(this, 6,7)  
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




object raw_DatosBasicos {
  def main(args : Array[String]) {
    
    //Creación API
    val huemulLib  = new huemul_BigDataGovernance(s"BigData Fabrics - ${this.getClass.getSimpleName}", args, com.yourcompany.settings.globalSettings.Global)
    val Control = new huemul_Control(huemulLib, null, huemulType_Frequency.MONTHLY)
    /*************** PARAMETROS **********************/
    
    //Inicializa clase RAW  
    val DF_RAW =  new raw_DatosBasicos(huemulLib, Control)
    if (!DF_RAW.open("DF_RAW", null, 2018, 12, 31, 0, 0, 0, "")) {
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