package com.huemulsolutions.bigdata.test


import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
//import com.yourcompany.settings.globalSettings
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import java.io.{FileNotFoundException, IOException}

/*
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.pdf.PDFParser;
import org.apache.tika.sax.BodyContentHandler;
 */

//import org.xml.sax.SAXException;
//import org.apache.tika.parser.pdf.PDFParserConfig

//import org.apache.spark.streaming._
//import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql._

import twitter4j._
import twitter4j.TwitterFactory
//import twitter4j.Twitter
import twitter4j.conf.ConfigurationBuilder
import scala.collection.JavaConverters._

//import com.huemulsolutions.bigdata.test.Proc_PlanPruebas_PermisosUpdate

/**
 * @author ${user.name}
 */


class demo_twitter_seba extends Serializable {
  var user_name: String = null
  var retweet_count: Integer = 0
  var tweet_followers_count: Integer = 0
  var source: String = null
  var coordinates: String = null
  var tweet_mentioned_count: Integer = null
  var tweet_ID: String = null
  var tweet_text: String = null
}


object App {

  def getKeyFromFile(fileName: String): String = {
    var key: String = null
    
   try {
      val openFile = Source.fromFile(fileName)
      key = openFile.getLines.mkString
      openFile.close()
    } catch {
        case _: FileNotFoundException => println(s"Couldn't find that file: ${fileName}")
        case e: IOException => println(s"($fileName). Got an IOException! ${e.getLocalizedMessage}")
        case _: Exception => println(s"exception opening ${fileName}")
    }
    
    return key
  }

  
  def main_PDF(args : Array[String]) {
    
    
    /*
    val document = PDDocument.load(new File("CGP_16003127_1344086.pdf"))
    document.getClass();
    val stripper = new PDFTextStripperByArea()
    stripper.setSortByPosition(true);
    val tStripper = new PDFTextStripper();
    tStripper.setStartPage(1)
    tStripper.setEndPage(1)
    val pdfFileInText = tStripper.getText(document);
    val lines = pdfFileInText.split("\\r?\\n");
    println(lines.length)
    lines.foreach(x => println(x))
*/
    
   
  }
  
  def main_twitter(args : Array[String]) {
  
    val localPath: String = System.getProperty("user.dir").concat("/")
       println(s"path: $localPath")
   
      
   val consumerKey = getKeyFromFile(s"${localPath}twitter_consumerKey.set") 
   val consumerSecret = getKeyFromFile(s"${localPath}twitter_consumerSecret.set") 
   val accessToken = getKeyFromFile(s"${localPath}twitter_accessToken.set") 
   val accessTokenSecret = getKeyFromFile(s"${localPath}twitter_accessTokenSecret.set") 
  
   
   
    
    val huemulLib = new huemul_BigDataGovernance("Pruebas Inicialización de Clases",args,com.yourcompany.settings.globalSettings.Global)
    val Control = new huemul_Control(huemulLib,null, huemulType_Frequency.MONTHLY)
   
   
   
    val cb = new ConfigurationBuilder()
    cb.setDebugEnabled(true)
    cb.setOAuthConsumerKey(consumerKey )
    cb.setOAuthConsumerSecret(consumerSecret )
    cb.setOAuthAccessToken(accessToken )
    cb.setOAuthAccessTokenSecret(accessTokenSecret )

    val twitterFactory = new TwitterFactory(cb.build())
    val twitter = twitterFactory.getInstance()

    //val dataList: ArrayBuffer[demo_twitter_seba] = new ArrayBuffer[demo_twitter_seba]()
    //var nuevo 
    var data_list : Array[Row] = null
    
    
    var i: Integer = 0
    val query = new Query(" #Azure ")
          query.setCount(100)
          query.lang("en")
          var finished = false
          while (!finished) {
            val result = twitter.search(query)
            val statuses = result.getTweets
            var lowestStatusId = Long.MaxValue
            for (status <- statuses.asScala) {
              
              if(!status.isRetweet){
                i += 1
                val nuevo = new Array[Any](8)  
                
                nuevo(0) = status.getUser.getScreenName //user_name
                nuevo(1) = status.getRetweetCount //retweet_count
                nuevo(2) = status.getUser.getFollowersCount //tweet_followers_count
                nuevo(3) = status.getSource //source
                val geoloc = status.getGeoLocation
                nuevo(4) = if (geoloc == null) null else geoloc.toString //coordinates

                val mentioned = status.getUserMentionEntities
                nuevo(5) =  mentioned.length //tweet_mentioned_count
                nuevo(6) = status.getId.toString //tweet_ID
                nuevo(7) = status.getText //tweet_text
                nuevo(8) = status.getCreatedAt
                status.getPlace.toString
                status.getPlace.getCountry
                
                
                if (data_list == null)
                  data_list = Array(Row.fromSeq(nuevo)) 
                else 
                  data_list = data_list :+ Row.fromSeq(nuevo)
                println(data_list.length)
                //val a = Row.fromSeq(nuevo) 
                //val b = data_list.fill(a)(1)
                //println(status.getText())
              }
              lowestStatusId = Math.min(status.getId, lowestStatusId)
            }
            query.setMaxId(lowestStatusId - 1)
           
           if (i >= 50) {
             val final_rdd = huemulLib.spark.sparkContext.parallelize(data_list )
             val df_final = huemulLib.spark.createDataFrame(final_rdd, StructType(
                                                            List(
                                                          StructField("user_name", StringType, nullable = true),
                                                          StructField("retweet_count", IntegerType, nullable = true),
                                                          StructField("tweet_followers_count", IntegerType, nullable = true),
                                                          StructField("source", StringType, nullable = true),
                                                          StructField("coordinates", StringType, nullable = true),
                                                          StructField("tweet_mentioned_count", IntegerType, nullable = true),
                                                          StructField("tweet_ID", StringType, nullable = true),
                                                          StructField("tweet_text", StringType, nullable = true))))
             
                                                          
              df_final.write.mode(SaveMode.Append).format("parquet").save("/user/data/spark/twitter_search_01.parquet")
              
              data_list = null
              i = 0
           }
          }
   
   
  
   
    /*
    val cb = new ConfigurationBuilder
    cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerSecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessTokenSecret)
    val auth = new OAuthAuthorization(cb.build)
    
    
    val ssc = new StreamingContext(huemulLib.spark.sparkContext, Seconds(1))
    ssc.checkpoint("/user/data/spark/checkpoint3")
    
    //val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = Array("5YhrJlIJOd0VOfaHRvcKNZeHz","","","")
    val stream = TwitterUtils.createStream(ssc, twitterAuth, filters, storageLevel) (ssc, None)
    
    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))
    val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
                     .map{case (topic, count) => (count, topic)}
                     .transform(_.sortByKey(false))
    val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
                     .map{case (topic, count) => (count, topic)}
                     .transform(_.sortByKey(false))
    // Print popular hashtags
    topCounts60.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })
    topCounts10.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })
    ssc.start()
    ssc.awaitTermination()
    */
    
    huemulLib.close()
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
    
    val validacionTablas = new ArrayBuffer[String]()
    //validacionTablas.append("production_mdm_oldvalue.tbl_datosbasicos_oldvalue")
    validacionTablas.append("production_mdm_oldvalue.tbl_datosbasicosupdate_oldvalue")
    validacionTablas.append("production_mdm_oldvalue.tbl_oldvaluetrace_oldvalue")
    validacionTablas.append("production_dqerror.tbl_datosbasicos_dq")
    validacionTablas.append("production_dqerror.tbl_datosbasicos_errorfk_dq")
    validacionTablas.append("production_dqerror.tbl_datosbasicos_mes_exclude_dq")
    validacionTablas.append("production_dqerror.tbl_datosbasicoserrores_dq")
    validacionTablas.append("production_dqerror.tbl_datosbasicosinsert_dq")
    validacionTablas.append("production_dqerror.tbl_datosbasicosinsert_exclude_dq")
    validacionTablas.append("production_dqerror.dataf100_w_dq")
    validacionTablas.append("production_dqerror.dataf100_e_dq")
    validacionTablas.append("production_dqerror.dataf100_wex_dq")
    validacionTablas.append("production_dqerror.dataf100_wex_w_dq")
    validacionTablas.append("production_dqerror.dataf100_wex_trx_dq")
    validacionTablas.append("production_master.tbl_datosbasicos")
    validacionTablas.append("production_master.tbl_datosbasicos_mes")
    validacionTablas.append("production_master.tbl_datosbasicos_mes_exclude")
    validacionTablas.append("production_master.tbl_datosbasicosinsert")
    validacionTablas.append("production_master.tbl_datosbasicosinsert_exclude")
    validacionTablas.append("production_master.tbl_datosbasicosnuevos")
    validacionTablas.append("production_master.tbl_datosbasicosnuevosperc")
    validacionTablas.append("production_master.tbl_datosbasicosupdate")
    validacionTablas.append("production_master.tbl_oldvaluetrace")
    validacionTablas.append("production_master.tbl_DatosParticion")
    validacionTablas.append("production_master.tbl_DatosParticionAcum")
    validacionTablas.append("production_master.tbl_DatosParticionMaster")
    validacionTablas.append("production_master.tbl_DatosBasicosNombres")
    validacionTablas.append("production_master.tbl_DatosBasicosNombres_mes")
    validacionTablas.append("production_master.dataf100_wex_trx")
    //validacionTablas.append("production_master.dataf100_W")
    //validacionTablas.append("production_master.dataf100_WEX")
    //validacionTablas.append("production_master.dataf100_WEX_W")
    
    var metadata_hive_active: Boolean = false
    var metadata_spark_active: Boolean = false
    var metadata_hwc_active: Boolean = false
    
    val huemulLib_ini = new huemul_BigDataGovernance("Pruebas Inicialización de Clases",args,com.yourcompany.settings.globalSettings.Global)
    
    
    com.yourcompany.settings.globalSettings.Global.HIVE_HourToUpdateMetadata=6
    
    metadata_hive_active = huemulLib_ini.arguments.GetValue("metadata_hive_active", "false").toBoolean
    metadata_spark_active = huemulLib_ini.arguments.GetValue("metadata_spark_active", "false").toBoolean
    metadata_hwc_active = huemulLib_ini.arguments.GetValue("metadata_hwc_active", "false").toBoolean
    
        
    huemulLib_ini.close()
    
    com.yourcompany.settings.globalSettings.Global.externalBBDD_conf.Using_HWC.setActive(metadata_hwc_active).setActiveForHBASE(metadata_hwc_active)
    
    
    com.yourcompany.settings.globalSettings.Global.externalBBDD_conf.Using_HIVE.setActive(metadata_hive_active).setActiveForHBASE(metadata_hive_active)
    if (metadata_hive_active) {
      val HIVE_Setting = new ArrayBuffer[huemul_KeyValuePath]()
       val localPath: String = System.getProperty("user.dir").concat("/")
       println(s"path: $localPath")
      HIVE_Setting.append(new huemul_KeyValuePath("production",getKeyFromFile(s"${localPath}prod-demo-setting-hive-connection.set")))
      HIVE_Setting.append(new huemul_KeyValuePath("experimental",getKeyFromFile(s"${localPath}prod-demo-setting-hive-connection.set")))
   
      
      com.yourcompany.settings.globalSettings.Global.externalBBDD_conf.Using_HIVE.setConnectionStrings(HIVE_Setting)
      println(s"""num reg: ${com.yourcompany.settings.globalSettings.Global.externalBBDD_conf.Using_HIVE.getJDBC_connection(huemulLib_ini).ExecuteJDBC_WithResult("select 1 as uno").ResultSet.length}""")
    }
    com.yourcompany.settings.globalSettings.Global.externalBBDD_conf.Using_SPARK.setActive(metadata_spark_active).setActiveForHBASE(false)
    
    if (com.yourcompany.settings.globalSettings.Global.getBigDataProvider() == huemulType_bigDataProvider.databricks) {
      com.yourcompany.settings.globalSettings.Global.setAVRO_format("avro")
    }

    prc_f100_testplan.main(args)
    
    com.huemulsolutions.bigdata.raw.raw_DatosPDF_test.main(args)
    com.huemulsolutions.bigdata.raw.raw_LargoDinamico_test.main(args)
    Proc_PlanPruebas_CargaMaster_SelectiveUpdate.main(args)
    Proc_PlanPruebas_CargaMasterNombres_SelectiveUpdate.main(args)

    Proc_PlanPruebas_PermisosFull.main(args)
    Proc_PlanPruebas_PermisosInsert.main(args)
    Proc_PlanPruebas_PermisosUpdate.main(args)

    Proc_PlanPruebas_Particion_dia_dia1p1.main(args)
    Proc_PlanPruebas_Particion_dia_dia1p2.main(args)
    Proc_PlanPruebas_Particion_dia_dia2p1.main(args)
    Proc_PlanPruebas_Particion_dia_dia2p2.main(args)
    Proc_PlanPruebas_Particion_dia_dia3p1.main(args)

    //reprocesamiento de Proc_PlanPruebas_Particion_dia_dia2p1, resultados deben ser iguales
    Proc_PlanPruebas_Particion_dia_dia2p3.main(args)


    Proc_PlanPruebas_Particion_diaAcum_dia1p1.main(args)
    Proc_PlanPruebas_Particion_diaAcum_dia1p1_re.main(args)

    Proc_PlanPruebas_Particion_master_dia1p1.main(args)
    Proc_PlanPruebas_Particion_master_dia3p1.main(args)
    
    Proc_PlanPruebas_CargaMaster.main(args)
    Proc_PlanPruebas_CargaMasterNombres.main(args)

    Proc_PlanPruebas_fk.main(args)
    Proc_PlanPruebas_CargaMaster_mes.main(args)
    Proc_PlanPruebas_CargaMasterNombres_mes.main(args)
    Proc_PlanPruebas_CargaMaster_mes_2.main(args)
    Proc_PlanPruebas_CargaMaster_mes_paso_2_selective.main(args)
    
    Proc_PlanPruebas_InsertLimitErrorPorc.main(args)
    Proc_PlanPruebas_InsertLimitError.main(args)
    
    Proc_PlanPruebas_NoMapped.main(args)
    
    Proc_PlanPruebas_OnlyInsertNew_warning.main(args)
    Proc_PlanPruebas_OnlyInsertNew.main(args)
    com.huemulsolutions.bigdata.raw.raw_DatosFORMATO_test.main(args)
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
    
    if (!huemulLib.hdfsPath_exists("/user/data/production/te"))
      println("prueba 1 exitosa: no existe")
    else 
      println("prueba 1 error")
    
    if (huemulLib.hdfsPath_exists("/user/data/production/temp/"))
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
    if (metadata_hive_active || metadata_hwc_active) {
      if (metadata_hwc_active) {
        val HIVE_Setting = new ArrayBuffer[huemul_KeyValuePath]()
        val localPath: String = System.getProperty("user.dir").concat("/")
        println(s"path: $localPath")
        HIVE_Setting.append(new huemul_KeyValuePath("production",getKeyFromFile(s"${localPath}prod-demo-setting-hive-connection.set")))
        HIVE_Setting.append(new huemul_KeyValuePath("experimental",getKeyFromFile(s"${localPath}prod-demo-setting-hive-connection.set")))
             
        com.yourcompany.settings.globalSettings.Global.externalBBDD_conf.Using_HIVE.setConnectionStrings(HIVE_Setting)
      }
      
      val connectionHIVE = huemulLib.GlobalSettings.externalBBDD_conf.Using_HIVE.getJDBC_connection(huemulLib)
      println("validando existencia de tablas en hive jdbc")
      
      validacionTablas.foreach { x =>  
        println(s"valida en HIVE existencia tabla $x")
        val valida01 = connectionHIVE.ExecuteJDBC_WithResult(s"select count(1) as cantidad from $x")
        if (valida01.IsError) {
          error_existe_tablas_en_hive = true
          println(valida01.ErrorDescription)
          Control.RegisterTestPlan(TestPlanGroup, s"hive $x", s"detalle error: ${valida01.ErrorDescription}", "no raiserror", s"raiserror", p_testPlan_IsOK = false)
    
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
          val valida01 = huemulLib.spark.sql(s"select count(1) as cantidad from ${x}")
          valida01.show()
        } catch {
          case e: Exception =>
            error_existe_tablas_en_spark = true
            println(e.getMessage)
            Control.RegisterTestPlan(TestPlanGroup, s"hive ${x}", s"detalle error: ${e.getMessage}", "no raiserror", s"raiserror", p_testPlan_IsOK = false)
        }
      }
    }
    Control.RegisterTestPlan(TestPlanGroup, "error_existe_tablas_en_spark", "error_existe_tablas_en_spark", "error_existe_tablas_en_spark = false", s"error_existe_tablas_en_spark = ${error_existe_tablas_en_spark}", !error_existe_tablas_en_spark)
    Control.TestPlan_CurrentIsOK(2)
    
    if (Control.TestPlan_IsOkById(TestPlanGroup, 40))
      println ("TODO OK")
    else
      println ("ERRORES")
      
    huemulLib.close()
    
    
    
  }

}