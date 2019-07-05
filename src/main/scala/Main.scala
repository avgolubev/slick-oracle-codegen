import slick.codegen.SourceCodeGenerator
import slick.jdbc._
import slick.model.Model
import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.ExecutionContext.Implicits.global
import slick.dbio.DBIO
import slick.jdbc.meta.MTable

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.Duration

object OracleModel {
  
  import slick.model.{QualifiedName, Model, Table, PrimaryKey, ForeignKey, ForeignKeyAction}  
  import anorm._
  import anorm.SqlParser._
    
  def buildModel(jdbcUrl: String, userName: String, pass: String, schemaNames: Seq[String], tableNames: Seq[String]): Model = {
    
    val db = DB(jdbcUrl, userName, pass)
                
    db.withConnection { implicit connection =>      
      val modelBuildSteps = new ModelBuildSteps(schemaNames, tableNames)
      modelBuildSteps.buildModel                  
    }       
  }
  
}

case class DB(jdbcUrl: String, userName: String, pass: String) {
    
  import  java.sql.{Connection, DriverManager}
      
  def withConnection[A](block: Connection => A): A = {
    
    var connection: Connection = null
    
    try {
      connection = DriverManager.getConnection(jdbcUrl, userName, pass)
      val result = block(connection)
      connection.close()
      result
    } catch {
      case all: Throwable =>
        if(connection != null) connection.close() 
        throw all
    }           
  }
  
}

object Main extends App {
  // original builder -  slick.codegen.SourceCodeGenerator.main(Array("slick.jdbc.OracleProfile", "oracle.jdbc.OracleDriver", "jdbc:oracle:thin:@server:1521:prod", "sl", "test", "userName", "pass"))
  val outputDir = "slick" // place generated files in sbt's managed sources folder   
  val jdbcUrl = "jdbc:oracle:thin:@//server:1521/prod"
  val jdbcDriver = "oracle.jdbc.OracleDriver"
  val slickDriver = "slick.jdbc.OracleProfile"
  val pkg = "demo"  
  val userName = ""  
  val pass = ""
  
  val model = OracleModel.buildModel(jdbcUrl, userName, pass, Seq(), Seq())
    
  new SourceCodeGenerator(model).writeToFile(slickDriver, outputDir, pkg)
}
