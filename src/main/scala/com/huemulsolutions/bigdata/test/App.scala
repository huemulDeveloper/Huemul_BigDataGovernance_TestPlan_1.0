package com.huemulsolutions.bigdata.test

import org.apache.spark.sql.SparkSession

/**
 * @author ${user.name}
 */
object App {

  def main(args : Array[String]) {
    Proc_PlanPruebas_CargaMaster.main(args)
    Proc_PlanPruebas_AutoCastOff.main(args)
    Proc_PlanPruebas_CargaAVRO.main(args)
    
    
  }

}
