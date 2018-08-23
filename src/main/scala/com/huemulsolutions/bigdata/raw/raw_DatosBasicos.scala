package com.huemulsolutions.bigdata.raw

import com.huemulsolutions.bigdata.datalake._
import com.huemulsolutions.bigdata.datalake.huemulType_FileType;
import com.huemulsolutions.bigdata.datalake.huemulType_Separator;
import com.huemulsolutions.bigdata.datalake.huemul_DataLake;
import com.huemulsolutions.bigdata.datalake.huemul_DataLakeSetting;
import com.huemulsolutions.bigdata.test.globalSettings;
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._

class raw_DatosBasicos(huemulLib: huemul_Library, Control: huemul_Control) extends huemul_DataLake(huemulLib, Control) with Serializable  {
   this.LogicalName = "DatosBasicos"
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
    
    //Columns Info CHARACTER
    FormatSetting.DataSchemaConf.ColSeparatorType = huemulType_Separator.CHARACTER  //POSITION;CHARACTER
    FormatSetting.DataSchemaConf.ColSeparator = "\\|"    //SET FOR CARACTER
    FormatSetting.DataSchemaConf.HeaderColumnsString = "TipoValor;IntValue;BigIntValue;SmallIntValue;TinyIntValue;DecimalValue;RealValue;FloatValue;StringValue;charValue;timeStampValue" //siempre con ; 
    
    //Log Info
    FormatSetting.LogSchemaConf.ColSeparatorType = huemulType_Separator.CHARACTER  //POSITION;CHARACTER;NONE
    FormatSetting.LogNumRows_FieldName = null
    //Fields Info for CHARACTER
    FormatSetting.LogSchemaConf.ColSeparator = ";"    //SET FOR CARACTER
    FormatSetting.LogSchemaConf.HeaderColumnsString = "VACIO" //Fielda;Fieldb;fieldc
    
    this.SettingByDate.append(FormatSetting)
  
    /***
   * open(ano: Int, mes: Int) <br>
   * método que retorna una estructura con un DF de detalle, y registros de control <br>
   * ano: año de los archivos recibidos <br>
   * mes: mes de los archivos recibidos <br>
   * dia: dia de los archivos recibidos <br>
   * Retorna: true si todo está OK, false si tuvo algún problema <br>
  */
  def open(Alias: String, ControlParent: huemul_Control, ano: Integer, mes: Integer, dia: Integer, hora: Integer, min: Integer, seg: Integer, TipoArchivo: String): Boolean = {
    val control = new huemul_Control(huemulLib, ControlParent, false)
    //Setea parámetros
    control.AddParamInfo("Ano", ano.toString())
    control.AddParamInfo("Mes", mes.toString())
    
    control.NewStep("Abriendo raw")
       
    try { 
      //Abre archivo RDD y devuelve esquemas para transformar a DF
      if (!this.OpenFile(ano, mes, dia, hora, min, seg, s"{{TipoArchivo}}=${TipoArchivo}")){
        control.RaiseError(s"Error al abrir archivo: ${this.Error.ControlError_Message}")
      }
   
      control.NewStep("Aplicando Filtro")
      /**/    //Agregar filtros o cambiar forma de leer archivo en este lugar
      val rowRDD = this.DataRDD
            .filter { x => x != this.Log.DataFirstRow  }
            .map { x => this.ConvertSchema(x) }
            
      control.NewStep("Transformando a dataframe")      
      //Crea DataFrame en Data.DataDF
      this.DF_from_RAW(rowRDD, Alias)
        
      //****VALIDACION DQ*****
      //**********************
      
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
    val huemulLib  = new huemul_Library(s"BigData Fabrics - ${this.getClass.getSimpleName}", args, globalSettings.Global)
    val Control = new huemul_Control(huemulLib, null)
    /*************** PARAMETROS **********************/
    
    //Inicializa clase RAW  
    val DF_RAW =  new raw_DatosBasicos(huemulLib, Control)
    if (!DF_RAW.open("DF_RAW", null, 2018, 12, 31, 0, 0, 0, "")) {
      println("************************************************************")
      println("**********  E  R R O R   E N   P R O C E S O   *************")
      println("************************************************************")
    }
      
    
    val MyName: String = this.getClass.getSimpleName
    //Cambiar los parametros:             nombre tabla hive   ,   package base , package específico
    DF_RAW.GenerateInitialCode(MyName, "sbif_institucion_mes","bigdata.fabrics","sbif.bancos")       
    
    Control.FinishProcessOK
  }  
}