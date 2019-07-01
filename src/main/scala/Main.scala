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
  //import scala.collection.mutable
  //import scala.collection.immutable
    
  def buildModel(jdbcUrl: String, userName: String, pass: String, schemaNames: Seq[String], tableNames: Seq[String]): Model = {
    
    val db = DB(jdbcUrl, userName, pass)
        
    var resultTables = Seq.empty[Table]    
        
    db.withConnection { implicit connection =>
      
      var qualifiedNames = ModelBuildSteps.buildSchemaTableNames(schemaNames, tableNames)  
      
      while(!qualifiedNames.isEmpty) {
        
        val intermediateResultTables = for {
          qualifiedName <- qualifiedNames
          (columns, primaryKey) = ModelBuildSteps.buildColumns(qualifiedName)
        } yield Table(qualifiedName, columns, primaryKey, Seq.empty, Seq.empty)
        
       qualifiedNames = Seq() 
       resultTables ++:= intermediateResultTables map {table =>
         val (foreignKeys, referencedQualifiedNames) =ModelBuildSteps.buildForeignKeys(resultTables ++ intermediateResultTables, table)
         qualifiedNames ++:= referencedQualifiedNames
         table.copy(foreignKeys = foreignKeys) 
       }
     }
                  
    }       
    val setClean = resultTables.toSet
    Model(setClean.toSeq)
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
  // original builder -  slick.codegen.SourceCodeGenerator.main(Array("slick.jdbc.OracleProfile", "oracle.jdbc.OracleDriver", "jdbc:oracle:thin:@msk-dev-02:1521:prod", "sl", "test", "test_dev", "test_dev"))
  val outputDir = "slick" // place generated files in sbt's managed sources folder   
  val jdbcUrl = "jdbc:oracle:thin:@//msk-dev-02:1521/prod"
  val jdbcDriver = "oracle.jdbc.OracleDriver"
  val slickDriver = "slick.jdbc.OracleProfile"
  val pkg = "demo"  
  val userName = "test_dev"  
  val pass = "test_dev"
  
  val model = OracleModel.buildModel(jdbcUrl, userName, pass, Seq("OAS"), Seq())
    
  new SourceCodeGenerator(model).writeToFile(slickDriver, outputDir, pkg)
}
