package samples

import org.junit._
import Assert._
import com.huemulsolutions.bigdata.tables.master._
import com.huemulsolutions.bigdata.test._
import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._

@Test
class AppTest {
    val args: Array[String] = new Array[String](1)
    args(0) = "Environment=production,RegisterInControl=false,TestPlanMode=true"
      
    val huemulLib = new huemul_Library("Pruebas Inicialización de Clases",args,globalSettings.Global)
    val Control = new huemul_Control(huemulLib,null)
      
      
    @Test
    def TEST_tbl_DatosBasicos() = assertTrue(TESTp_tbl_DatosBasicos())
    
    @Test
    def TEST_tbl_DatosBasicosAVRO() = assertTrue(TESTp_tbl_DatosBasicosAVRO())
    
    @Test
    def TEST_tbl_DatosBasicosInsesrt() = assertTrue(TESTp_tbl_DatosBasicosInsert())
    
    @Test
    def TEST_tbl_DatosBasicosNull() = assertTrue(TESTp_tbl_DatosBasicosNull())
    
    @Test
    def TEST_tbl_DatosBasicosUpdate() = assertTrue(TESTp_tbl_DatosBasicosUpdate())
    
    
    
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
    
    /**Revisión de clase tbl_DatosBasicosAVRO
     * 
     */
    def TESTp_tbl_DatosBasicosAVRO(): Boolean = {
      var SinError = true
      
      try {
        val Master = new tbl_DatosBasicosAVRO(huemulLib,Control)
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
        val Master = new tbl_DatosBasicosInsert(huemulLib,Control)
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

    /**Revisión de clase tbl_DatosBasicosNull
     * 
     */
    def TESTp_tbl_DatosBasicosNull(): Boolean = {
      var SinError = true
      
      try {
        val Master = new tbl_DatosBasicosErrores(huemulLib,Control)
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
        val Master = new tbl_DatosBasicosUpdate(huemulLib,Control)
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


