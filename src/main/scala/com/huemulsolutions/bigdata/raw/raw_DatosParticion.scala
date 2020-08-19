package com.huemulsolutions.bigdata.raw

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.datalake.{huemulType_FileType, huemulType_Separator, huemul_DataLake, huemul_DataLakeSetting}
import org.apache.spark.sql.types._

class raw_DatosParticion(huemulLib: huemul_BigDataGovernance, Control: huemul_Control) extends huemul_DataLake(huemulLib, Control) with Serializable  {
   this.Description = "datos para probar funcionalidades de Old VAlue Trace"
   this.GroupName = "HuemulPlanPruebas"
      
   val dataLakeConfig: huemul_DataLakeSetting = new huemul_DataLakeSetting(huemulLib)
     .setStartDate(2010,1,1,0,0,0)
     .setEndDate(2050,12,12,0,0,0)
     //Path & names
     .setGlobalPath(huemulLib.GlobalSettings.RAW_BigFiles_Path)
     .setLocalPath("planPruebas/")
     .setFileName("DatosParticion_{{YYYY}}{{MM}}{{DD}}_{{EMPRESA}}.txt")
     .setFileType(huemulType_FileType.TEXT_FILE)
     .setContactName("Sebastián Rodríguez")
     //Data
     .setColumnDelimiterType(huemulType_Separator.CHARACTER)
     .setColumnDelimiter("\\|")
     .addColumn("periodo", "periodo", StringType,"periodo de los datos")
     .addColumn("empresa", "empresa", StringType,"Nombre de la empresa")
     .addColumn("app", "app", StringType,"Canal utilizado")
     .addColumn("producto", "producto", StringType,"nombre producto")
     .addColumn("cantidad", "cantidad", IntegerType,"Cantidad")
     .addColumn("precio", "precio", IntegerType,"Precio")
     .addColumn("idTx", "idTx", StringType,"codigo de la transacción")
     //Header
     .setLogNumRowsColumnName(null)
       .setHeaderColumnDelimiterType(huemulType_Separator.CHARACTER)
       .setHeaderColumnDelimiter("\\|")
       .setHeaderColumnsString("VACIO")

  this.SettingByDate.append(dataLakeConfig)


    /***
   * open(ano: Int, mes: Int) <br>
   * método que retorna una estructura con un DF de detalle, y registros de control <br>
   * ano: año de los archivos recibidos <br>
   * mes: mes de los archivos recibidos <br>
   * dia: dia de los archivos recibidos <br>
   * Retorna: true si todo está OK, false si tuvo algún problema <br>
  */
  def open(Alias: String, ControlParent: huemul_Control, ano: Integer, mes: Integer, dia: Integer, hora: Integer, min: Integer, seg: Integer, Empresa: String, AplicarTrim: Boolean = true): Boolean = {
    val control = new huemul_Control(huemulLib, ControlParent, huemulType_Frequency.MONTHLY, false)
    //Setea parámetros
    control.AddParamYear("Ano", ano)
    control.AddParamMonth("Mes", mes)
    control.AddParamInformation("Empresa", Empresa)

    
    control.NewStep("Abriendo raw")
       
    try { 
      //Abre archivo RDD y devuelve esquemas para transformar a DF
      if (!this.OpenFile(ano, mes, dia, hora, min, seg, s"{{EMPRESA}}=$Empresa")){
        control.RaiseError(s"Error al abrir archivo: ${this.Error.ControlError_Message}")
      }
   
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
      val validanumfilas = this.DataFramehuemul.DQ_NumRowsInterval(this, 1,6000)
      if (validanumfilas.isError) control.RaiseError(s"user: Numero de Filas fuera del rango. ${validanumfilas.Description}")
                        
      control.FinishProcessOK                      
    } catch {
      case e: Exception =>
        control.Control_Error.GetError(e, this.getClass.getName, this, null)
        control.FinishProcessError()
    }         

    control.Control_Error.IsOK()
  }
}




