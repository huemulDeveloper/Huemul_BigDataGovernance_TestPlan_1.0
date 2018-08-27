package com.huemulsolutions.bigdata.test


import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.test.Proc_PlanPruebas_PermisosUpdate

/**
 * @author ${user.name}
 */
object App {

  def main(args : Array[String]) {
    val huemulLib = new huemul_Library("Pruebas Inicialización de Clases",args,globalSettings.Global)
    val Control = new huemul_Control(huemulLib,null)
    
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
  
    Proc_PlanPruebas_PermisosFull.main(args)
    Proc_PlanPruebas_PermisosInsert.main(args)
    Proc_PlanPruebas_PermisosUpdate.main(args)
    
    Proc_PlanPruebas_InsertLimitErrorPorc.main(args)
    
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
