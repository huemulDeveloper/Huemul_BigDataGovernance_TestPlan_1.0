package samples

import org.junit._
import Assert._
import com.huemulsolutions.bigdata.tables.master._
import com.huemulsolutions.bigdata.test._
import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.raw.raw_DatosBasicos
import com.huemulsolutions.bigdata.common.huemul_BigDataGovernance
import com.huemulsolutions.bigdata.tables._

@Test
class AppTest {
    val args: Array[String] = new Array[String](1)
    args(0) = "Environment=production,RegisterInControl=false,TestPlanMode=true"
      
    val huemulLib = new huemul_BigDataGovernance("Pruebas Inicialización de Clases",args,globalSettings.Global)
    val Control = new huemul_Control(huemulLib,null, huemulType_Frequency.MONTHLY)
      
    @Test
    def pruebita() = assertTrue(genera()
      
    )
    
      
    @Test
    def TEST_tbl_DatosBasicos_Mes() = assertTrue(TESTp_tbl_DatosBasicos_mes())
    
    @Test
    def TEST_tbl_DatosBasicos() = assertTrue(TESTp_tbl_DatosBasicos())
    
    @Test
    def TEST_tbl_DatosBasicosAVRO() = assertTrue(TESTp_tbl_DatosBasicosAVRO())
    
//    @Test
//    def TEST_tbl_DatosBasicosInsesrt() = assertTrue(TESTp_tbl_DatosBasicosInsert())
    
//    @Test
//    def TEST_tbl_DatosBasicosErrores() = assertTrue(TESTp_tbl_DatosBasicosErrores())
    
    @Test
    def TEST_tbl_DatosBasicosUpdate() = assertTrue(TESTp_tbl_DatosBasicosUpdate())
    
    def genera(): Boolean = {
      val a = new raw_DatosBasicos(huemulLib, Control)
      
      a.GenerateInitialCode("com.yourapplication", "objectName", "tbl_algo", "test/", huemulType_Tables.Reference, huemulType_Frequency.MONTHLY)
      return true
    }
    
    /**Revisión de clase tbl_DatosBasicos
     * 
     */
    def TESTp_tbl_DatosBasicos(): Boolean = {
      var SinError = true
      
      try {
        val Master = new tbl_DatosBasicos(huemulLib,Control)
        if (Master.Error_isError) {
          println(s"Codigo: ${Master.Error_Code}, Descripción: ${Master.Error_Text}")
          SinError = false
        }
      } catch {
        case e: Exception => 
          SinError = false
          println(e)
      }
        
      
      return SinError
    }
    
    /**Revisión de clase tbl_DatosBasicos
     * 
     */
    def TESTp_tbl_DatosBasicos_mes(): Boolean = {
      var SinError = true
      
      try {
        val Master = new tbl_DatosBasicos_mes(huemulLib,Control,huemulType_StorageType.HBASE)
        if (Master.Error_isError) {
          println(s"Codigo: ${Master.Error_Code}, Descripción: ${Master.Error_Text}")
          SinError = false
        }
      } catch {
        case e: Exception => 
          SinError = false
          println(e)
      }
        
      
      return SinError
    }
    
    /**Revisión de clase tbl_DatosBasicosAVRO
     * 
     */
    def TESTp_tbl_DatosBasicosAVRO(): Boolean = {
      var SinError = true
      
      try {
        val Master = new tbl_DatosBasicosAVRO(huemulLib,Control,huemulType_StorageType.ORC)
        if (Master.Error_isError) {
          println(s"Codigo: ${Master.Error_Code}, Descripción: ${Master.Error_Text}")
          SinError = false
        }
      } catch {
        case e: Exception => 
          SinError = false
          println(e)
      }
        
      
      return SinError
    }

    /**Revisión de clase tbl_DatosBasicosInsert
     * 
     */
    def TESTp_tbl_DatosBasicosInsert(): Boolean = {
      var SinError = true
      
      try {
        val Master = new tbl_DatosBasicosInsert(huemulLib,Control,huemulType_StorageType.HBASE)
        if (Master.Error_isError) {
          println(s"Codigo: ${Master.Error_Code}, Descripción: ${Master.Error_Text}")
          SinError = false
        }
      } catch {
        case e: Exception => 
          SinError = false
          println(e)
      }
        
      
      return SinError
    }

    /**Revisión de clase tbl_DatosBasicosErrores
     * 
     */
    def TESTp_tbl_DatosBasicosErrores(): Boolean = {
      var SinError = true
      
      try {
        val Master = new tbl_DatosBasicosErrores(huemulLib,Control,huemulType_StorageType.HBASE)
        if (Master.Error_isError) {
          println(s"Codigo: ${Master.Error_Code}, Descripción: ${Master.Error_Text}")
          SinError = false
        }
      } catch {
        case e: Exception => 
          SinError = false
          println(e)
      }
        
      
      return SinError
    }

    /**Revisión de clase tbl_DatosBasicosUpdate
     * 
     */
    def TESTp_tbl_DatosBasicosUpdate(): Boolean = {
      var SinError = true
      
      try {
        val Master = new tbl_DatosBasicosUpdate(huemulLib,Control,huemulType_StorageType.PARQUET)
        if (Master.Error_isError) {
          println(s"Codigo: ${Master.Error_Code}, Descripción: ${Master.Error_Text}")
          SinError = false
        }
      } catch {
        case e: Exception => 
          SinError = false
          println(e)
      }
        
      
      return SinError
    }



}


