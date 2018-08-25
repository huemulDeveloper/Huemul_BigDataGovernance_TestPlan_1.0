package com.huemulsolutions.bigdata.test

import org.apache.spark.sql.SparkSession

/**
 * @author ${user.name}
 */
object App {

  def main(args : Array[String]) {
    Proc_PlanPruebas_CargaMaster.main(args)
    
    Proc_PlanPruebas_InsertLimitError.main(args)
    Proc_PlanPruebas_NoMapped.main(args)
    
    Proc_PlanPruebas_OnlyInsertNew.main(args)
    Proc_PlanPruebas_OnlyUpdate.main(args)
     
    Proc_PlanPruebas_AutoCastOff.main(args)
    Proc_PlanPruebas_CargaAVRO.main(args)
    
   
    Proc_PlanPruebas_Errores.main(args)
    Proc_PlanPruebas_Malos01.main(args)
    
    
  }

}
