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
import scala.util.Try
import com.huemulsolutions.bigdata.control.huemulType_Frequency.huemulType_Frequency
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.Row

class keyvalue(key: String, value: String, posIni: Integer, posFin: Integer) extends Serializable {
  def getKey(): String = {return key}
  def getValue(): String = {return value}
  def getPosIni(): Integer = {return posIni}
  def getPosFin(): Integer = {return posFin}
}

class raw_DatosPDF(huemulLib: huemul_BigDataGovernance, Control: huemul_Control) extends huemul_DataLake(huemulLib, Control) with Serializable  {
   this.Description = "Datos de prueba para validar con PDF"
   this.GroupName = "HuemulPlanPruebas"
      
   val FormatSetting = new huemul_DataLakeSetting(huemulLib)
    FormatSetting.StartDate = huemulLib.setDateTime(2010,1,1,0,0,0)
    FormatSetting.EndDate = huemulLib.setDateTime(2050,12,12,0,0,0)

    //Path info
    FormatSetting.GlobalPath = huemulLib.GlobalSettings.RAW_BigFiles_Path
    FormatSetting.LocalPath = "planPruebas/"
    FormatSetting.FileName = "tabla_de_aplimentos.pdf"
    FormatSetting.FileType = huemulType_FileType.PDF_FILE
    FormatSetting.ContactName = "Sebastián Rodríguez"
    
    
    val a = 1
    //Columns Info CHARACTER
    
    //PLAN EJECUCION 3:
    FormatSetting.DataSchemaConf.ColSeparatorType = huemulType_Separator.POSITION  //POSITION;CHARACTER
    FormatSetting.DataSchemaConf.ColSeparator = " "
    
    FormatSetting.DataSchemaConf.AddColumns("Codigo", "Codigo", StringType)
    FormatSetting.DataSchemaConf.AddColumns("Grupo", "Grupo", StringType)
    //FormatSetting.DataSchemaConf.AddCustomColumn("nota", "texto explicativo de la nota")
    
    
    //Log Info
    FormatSetting.LogSchemaConf.ColSeparatorType = huemulType_Separator.NONE  //POSITION;CHARACTER;NONE
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
  def open(Alias: String, ControlParent: huemul_Control, ano: Integer, mes: Integer, dia: Integer, hora: Integer, min: Integer, seg: Integer): Boolean = {
    val control = new huemul_Control(huemulLib, ControlParent, huemulType_Frequency.MONTHLY, false)
    //Setea parámetros
    control.AddParamYear("Ano", ano)
    control.AddParamMonth("Mes", mes)
    
    control.NewStep("Abriendo raw")
       
    try { 
      //Abre archivo RDD y devuelve esquemas para transformar a DF
      if (!this.OpenFile(ano, mes, dia, hora, min, seg, null)){
        control.RaiseError(s"Error al abrir archivo: ${this.Error.ControlError_Message}")
      }
      
      import huemulLib.spark.implicits._
   
      control.NewStep("Aplicando Filtro")
       
      //*******************************************************
      //obtiene posicion inicial
      //*******************************************************
      val reg_filaInicial = this.DataRDD_extended.filter { f => f._4.startsWith("Códigos y grupos de alimentos")}.collect()
      if (reg_filaInicial.length == 0)
        RaiseError_RAW("Texto inicial no encontrado", 1)  
      val filaInicial = reg_filaInicial(0)._1
      
      //*******************************************************
      //obtiene posicion final
      //*******************************************************
      val reg_filaFinal = this.DataRDD_extended.filter { f => f._4.startsWith("Componentes: definición y expresión de nutrientes")}.collect()
      if (reg_filaFinal.length == 0)
        RaiseError_RAW("Texto final no encontrado", 2)
      val filaFinal = reg_filaFinal(0)._1
      
      //*******************************************************
      //genera expresiones regulares
      //*******************************************************
      val regexCodigo = "[A-Z]".r
      val regexpNotas_traduce = "^(?i)nota [0-9]+".r
      
      //genera filtros de textos que no deben ser considerados
      val filtroCirculares = "Instituto Nacional de Salud".toUpperCase()
      
      //*******************************************************
      //obtiene RDD BASE con datos filtrados
      //*******************************************************
      val rowRDD_Base = this.DataRDD_extended
            .filter { x => x._1 >= filaInicial && x._1 < filaFinal} //caracteres iniciales
            .filter { x => x._3 > 0  } //largo de fila con trim > 0
            .filter { x => !(x._4.toUpperCase() == filtroCirculares) } //filtro de textos que no deben ser considerados
            .map ( x=> (x._1 //posicion
                      , x._2 //largo
                      , x._3 //largo con trim
                      , x._4 //texto
                      , x._4.split(" ",2) //_5 split
                      , regexCodigo.findFirstMatchIn(x._4.split(" ",2)(0)).mkString.trim().length() == 1 && x._4.split(" ",2)(0).length() == 1  //_6 marcador de existencia de codigo
                      )).collect()
    
      
      //******************************************
      //IMPRESION DE RESULTADOS POR CONSOLA
      //******************************************
      println(s"********************* RESULTADO rowRDD_Base, cantidad filas: ${rowRDD_Base.length}")
      rowRDD_Base.foreach(println)
      
      
      //*******************************************************
      //Genera resultado final de la tabla
      //*******************************************************
      val rowRDD_Consolidado = rowRDD_Base.filter{x => x._6 == true} .map( x => (x._4 //código y texto original 
                                                ,x._5(0)
                                                ,x._5(1).trim()
                                               )
                                         )
      //*******************************************************
      //muestra ejemplo de los datos del arreglo CONSOLIDADO
      //*******************************************************
      println("")
      println("")
      println("********************************** RESULTADO RDD CONSOLIDADO")
      rowRDD_Consolidado.foreach { x => println(x) }
      println("")
      println("")
      
      
      //*******************************************************
      //muestra ejemplo de los datos del RDD final
      //*******************************************************
       val rowRDD = rowRDD_Consolidado
            .map{ x=>
                  var DataArray_Dest : Array[Any] = new Array[Any](2)
                  DataArray_Dest(0) = x._2
                  DataArray_Dest(1) = x._3
                  Row.fromSeq(DataArray_Dest )
              }
     
      println("")
      println("")
      println("********************************** RESULTADO RDD FINAL")
      rowRDD.foreach { x => println(x) }      
      println("")
      println("")
      
      control.NewStep("Transformando a dataframe")      
      //Crea DataFrame en Data.DataDF
      this.DF_from_RAW(huemulLib.spark.sparkContext.parallelize(rowRDD), Alias)
        
      //****VALIDACION DQ*****
      //**********************
      
      control.NewStep("Validando cantidad de filas")      
      //validacion cantidad de filas
      val validanumfilas = this.DataFramehuemul.DQ_NumRowsInterval(this, 6,100)  
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




object raw_DatosPDF_test {
  def main(args : Array[String]) {
    
    //Creación API
    val huemulLib  = new huemul_BigDataGovernance(s"BigData Fabrics - ${this.getClass.getSimpleName}", args, com.yourcompany.settings.globalSettings.Global)
    val Control = new huemul_Control(huemulLib, null, huemulType_Frequency.MONTHLY)
    /*************** PARAMETROS **********************/
    
    val TestPlanGroup: String = huemulLib.arguments.GetValue("TestPlanGroup", null, "Debe especificar el Grupo de Planes de Prueba")
    //Inicializa clase RAW  
    val DF_RAW =  new raw_DatosPDF(huemulLib, Control)
    if (!DF_RAW.open("DF_RAW", null, 2018, 12, 31, 0, 0, 0)) {
      println("************************************************************")
      println("***********  E R R O R   E N   P R O C E S O   *************")
      println("************************************************************")
    } else 
      DF_RAW.DataFramehuemul.DataFrame.show()
      
    try { 
      val resultados = huemulLib.spark.sql("""select MAX(CASE WHEN Codigo = "A" and Grupo = "Cereales y derivados" then 1 else 0 end) as Cumple_A
                        ,MAX(CASE WHEN Codigo = "B" and Grupo = "Verduras, hortalizas y derivados" then 1 else 0 end) as Cumple_B
                        ,MAX(CASE WHEN Codigo = "C" and Grupo = "Frutas y derivados" then 1 else 0 end) as Cumple_C
                        ,MAX(CASE WHEN Codigo = "D" and Grupo = "Grasas, aceites y oleaginosas" then 1 else 0 end) as Cumple_D
                        ,MAX(CASE WHEN Codigo = "E" and Grupo = "Pescados y mariscos" then 1 else 0 end) as Cumple_E
                        ,MAX(CASE WHEN Codigo = "F" and Grupo = "Carnes y derivados" then 1 else 0 end) as Cumple_F
                        ,MAX(CASE WHEN Codigo = "G" and Grupo = "Leches y derivados" then 1 else 0 end) as Cumple_G
                        ,MAX(CASE WHEN Codigo = "H" and Grupo = "Bebidas (alcohólicas y analcohólicas)" then 1 else 0 end) as Cumple_H
                        ,MAX(CASE WHEN Codigo = "J" and Grupo = "Huevos y derivados" then 1 else 0 end) as Cumple_J
                        ,MAX(CASE WHEN Codigo = "K" and Grupo = "Productos azucarados" then 1 else 0 end) as Cumple_K
                        ,MAX(CASE WHEN Codigo = "L" and Grupo = "Misceláneos" then 1 else 0 end) as Cumple_L
                        ,MAX(CASE WHEN Codigo = "P" and Grupo = "Otros alimentos nativos" then 1 else 0 end) as Cumple_P
                        ,MAX(CASE WHEN Codigo = "Q" and Grupo = "Alimentos infantiles" then 1 else 0 end) as Cumple_Q
                        ,MAX(CASE WHEN Codigo = "T" and Grupo = "Leguminosas y derivados" then 1 else 0 end) as Cumple_T
                        ,MAX(CASE WHEN Codigo = "U" and Grupo = "Tubérculos, raíces y derivados" then 1 else 0 end) as Cumple_U
                        ,MAX(CASE WHEN Codigo = "V" and Grupo = "Tubérculos andinos" then 1 else 0 end) as Cumple_V
                        ,CAST(COUNT(1) AS INTEGER) AS Cantidad
          FROM DF_RAW """).first()
      
      var res_Cumple_A =  resultados.getAs[Int]("Cumple_A")
      var IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "existe A", "valida que exista registro A", "Valor = 1", s"Valor = ${res_Cumple_A}", res_Cumple_A == 1)
      Control.RegisterTestPlanFeature("Lectura PDF", IdTestPlan)
      
      var res_Cumple_B =  resultados.getAs[Int]("Cumple_B")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "existe B", "valida que exista registro B", "Valor = 1", s"Valor = ${res_Cumple_B}", res_Cumple_B == 1)
      Control.RegisterTestPlanFeature("Lectura PDF", IdTestPlan)
      
      var res_Cumple_C =  resultados.getAs[Int]("Cumple_C")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "existe C", "valida que exista registro C", "Valor = 1", s"Valor = ${res_Cumple_C}", res_Cumple_C == 1)
      Control.RegisterTestPlanFeature("Lectura PDF", IdTestPlan)
      
      var res_Cumple_D =  resultados.getAs[Int]("Cumple_D")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "existe D", "valida que exista registro D", "Valor = 1", s"Valor = ${res_Cumple_D}", res_Cumple_D == 1)
      Control.RegisterTestPlanFeature("Lectura PDF", IdTestPlan)
      
      var res_Cumple_E =  resultados.getAs[Int]("Cumple_E")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "existe E", "valida que exista registro E", "Valor = 1", s"Valor = ${res_Cumple_E}", res_Cumple_E == 1)
      Control.RegisterTestPlanFeature("Lectura PDF", IdTestPlan)
      
      var res_Cumple_F =  resultados.getAs[Int]("Cumple_F")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "existe F", "valida que exista registro F", "Valor = 1", s"Valor = ${res_Cumple_F}", res_Cumple_F == 1)
      Control.RegisterTestPlanFeature("Lectura PDF", IdTestPlan)
      
      var res_Cumple_G =  resultados.getAs[Int]("Cumple_G")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "existe G", "valida que exista registro G", "Valor = 1", s"Valor = ${res_Cumple_G}", res_Cumple_G == 1)
      Control.RegisterTestPlanFeature("Lectura PDF", IdTestPlan)
      
      var res_Cumple_H =  resultados.getAs[Int]("Cumple_H")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "existe H", "valida que exista registro H", "Valor = 1", s"Valor = ${res_Cumple_H}", res_Cumple_H == 1)
      Control.RegisterTestPlanFeature("Lectura PDF", IdTestPlan)
      
      var res_Cumple_J =  resultados.getAs[Int]("Cumple_J")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "existe J", "valida que exista registro J", "Valor = 1", s"Valor = ${res_Cumple_J}", res_Cumple_J == 1)
      Control.RegisterTestPlanFeature("Lectura PDF", IdTestPlan)
      
      var res_Cumple_K =  resultados.getAs[Int]("Cumple_K")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "existe K", "valida que exista registro K", "Valor = 1", s"Valor = ${res_Cumple_K}", res_Cumple_K == 1)
      Control.RegisterTestPlanFeature("Lectura PDF", IdTestPlan)
      
      var res_Cumple_L =  resultados.getAs[Int]("Cumple_L")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "existe L", "valida que exista registro L", "Valor = 1", s"Valor = ${res_Cumple_L}", res_Cumple_L == 1)
      Control.RegisterTestPlanFeature("Lectura PDF", IdTestPlan)
      
      var res_Cumple_P =  resultados.getAs[Int]("Cumple_P")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "existe P", "valida que exista registro P", "Valor = 1", s"Valor = ${res_Cumple_P}", res_Cumple_P == 1)
      Control.RegisterTestPlanFeature("Lectura PDF", IdTestPlan)
      
      var res_Cumple_Q =  resultados.getAs[Int]("Cumple_Q")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "existe Q", "valida que exista registro Q", "Valor = 1", s"Valor = ${res_Cumple_Q}", res_Cumple_Q == 1)
      Control.RegisterTestPlanFeature("Lectura PDF", IdTestPlan)
      
      var res_Cumple_T =  resultados.getAs[Int]("Cumple_T")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "existe T", "valida que exista registro T", "Valor = 1", s"Valor = ${res_Cumple_T}", res_Cumple_T == 1)
      Control.RegisterTestPlanFeature("Lectura PDF", IdTestPlan)
      
      var res_Cumple_U =  resultados.getAs[Int]("Cumple_U")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "existe U", "valida que exista registro U", "Valor = 1", s"Valor = ${res_Cumple_U}", res_Cumple_U == 1)
      Control.RegisterTestPlanFeature("Lectura PDF", IdTestPlan)
      
      var res_Cumple_V =  resultados.getAs[Int]("Cumple_V")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "existe V", "valida que exista registro V", "Valor = 1", s"Valor = ${res_Cumple_V}", res_Cumple_V == 1)
      Control.RegisterTestPlanFeature("Lectura PDF", IdTestPlan)
      
      var res_Cumple_cantidad =  resultados.getAs[Int]("Cantidad")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Cantidad de filas", "Valida que hayan 16 filas", "Valor = 16", s"Valor = ${res_Cumple_cantidad}", res_Cumple_cantidad == 16)
      Control.RegisterTestPlanFeature("Lectura PDF", IdTestPlan)
          
      
      
      val MyName: String = this.getClass.getSimpleName
      //Cambiar los parametros:             nombre tabla hive   ,   package base , package específico
      //DF_RAW.GenerateInitialCode(MyName, "sbif_institucion_mes","bigdata.fabrics","sbif.bancos")       
      
      Control.FinishProcessOK
    } catch {
      case e: Exception => {
        Control.Control_Error.GetError(e, this.getClass.getName, null, null)
        Control.FinishProcessError()   
      }
    }   
    
    if (Control.TestPlan_CurrentIsOK(17))
      println("Proceso OK")
      
   
    huemulLib.close()
    println("sesión cerrada")
  }  
}