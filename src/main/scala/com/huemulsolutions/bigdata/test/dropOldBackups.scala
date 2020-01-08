package com.huemulsolutions.bigdata.test

import com.huemulsolutions.bigdata.control.huemul_Control
import com.huemulsolutions.bigdata.control.huemulType_Frequency
import com.huemulsolutions.bigdata.common.huemul_BigDataGovernance

object dropOldBackups {
  def main(args: Array[String]): Unit = {
 
    val huemulBigDataGov  = new huemul_BigDataGovernance(s"BigDataGovernance Util", args, com.yourcompany.settings.globalSettings.Global)
    
    //numBackupToMaintain
    val numBackupToMaintain = huemulBigDataGov.arguments.GetValue("numBackupToMaintain", null,"param missing: numBackupToMaintain. Example: numBackupToMaintain=2 to maintain last 2 backups")
    
    if (numBackupToMaintain != null) {
      val Control = new huemul_Control(huemulBigDataGov,null, huemulType_Frequency.ANY_MOMENT, false, true) 
      Control.control_getBackupToDelete(numBackupToMaintain.toInt)
    }
      
    huemulBigDataGov.close()
    //Control.Init_CreateTables()
  }
}
