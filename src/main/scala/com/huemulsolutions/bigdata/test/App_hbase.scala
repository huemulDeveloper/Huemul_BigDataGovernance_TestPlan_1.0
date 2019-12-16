package com.huemulsolutions.bigdata.test


import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.yourcompany.settings.globalSettings
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.spark.HBaseRDDFunctions._
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.spark.KeyFamilyQualifier
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles
import org.apache.hadoop.hbase.client.Delete

//import org.apache.hadoop.hbase.io.ImmutableBytesWritable

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.util._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.api.java.function.FlatMapFunction



object App_hbase {


  
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
  
     // For implicit conversions from RDDs to DataFrames
    
   
    //Validación que todo está OK
    val huemulLib = new huemul_BigDataGovernance("Pruebas Inicialización de Clases",args,com.yourcompany.settings.globalSettings.Global)
    val Control = new huemul_Control(huemulLib,null, huemulType_Frequency.MONTHLY)
    
      /*
    val __DF = huemulLib.spark.sql("select '1010' as codigo, null as nombre, null as atelefono union all select  '1020' as codigo, 'nombre 1020' as nombre, 2222226 as atelefono union all select  '1030' as codigo, 'nombre 1030' as nombre, 3333333 as atelefono ")
    
    //ordena, primera columna debe ser PK
    val __cols = __DF.columns.sortBy { x => (if (x=="codigo") "0" else "1").concat(x) } 
    val __colSortedDF = __DF.select(__cols.map( x => col(x)): _*)
    //excluir PK
    val valCols = __cols.filterNot(x => x.equals("codigo"))
    val __numCols: Int = valCols.length
   
    import huemulLib.spark.implicits._ 
    val __pdd_2 = __colSortedDF.flatMap(row => {
      val rowKey = row(0).toString() //Bytes.toBytes(x._1)
      
      for (i <- 0 until __numCols) yield {
          val colName = valCols(i).toString()
          val colValue = if (row(i+1) == null) null else row(i+1).toString()
          
          (rowKey, ("default", colName, colValue))
        }
      }
    ).rdd
    
    //inicializa HBase
    val hbaseConf = HBaseConfiguration.create()
    val hbaseContext = new HBaseContext(huemulLib.spark.sparkContext, hbaseConf)
    
    //ASignación de tabla
    val stagingFolder = s"/tmp/user/${Control.Control_Id}"
    println(stagingFolder)
    val tableNameString: String = "emp_4"
    val tableName = TableName.valueOf(tableNameString)
    
    //elimina los registros que tengan algún valor en null
    val __tdd_null = __pdd_2.filter(x=> x._2._3 == null).map(x=>x._1).distinct().map(x=> Bytes.toBytes(x))
    hbaseContext.bulkDelete[Array[Byte]](__tdd_null
            ,tableName
            ,putRecord => new Delete( putRecord)
    		     
            ,4)
    
     
   
  
    val __tdd_notnull = __pdd_2.filter(x=> x._2._3 != null)
    __tdd_notnull.hbaseBulkLoad(hbaseContext
                          , tableName
                          , t =>  {
                            val rowKey = Bytes.toBytes(t._1)
                            val family: Array[Byte] = Bytes.toBytes(t._2._1)
                            val qualifier = Bytes.toBytes(t._2._2)
                            val value = Bytes.toBytes(t._2._3)
                            
                            val keyFamilyQualifier = new KeyFamilyQualifier(rowKey,family, qualifier)
                            Seq((keyFamilyQualifier, value)).iterator
                            
                          }
                          , stagingFolder)
    
    
    val load = new LoadIncrementalHFiles(hbaseConf)
    load.run(Array(stagingFolder, tableNameString))
        
  */
   
    huemulLib.close()
    
    
    
  }

}
