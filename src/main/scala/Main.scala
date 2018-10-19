import slick.codegen.SourceCodeGenerator
import slick.jdbc._
import slick.model.Model
import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.ExecutionContext.Implicits.global
import slick.dbio.DBIO
import slick.jdbc.meta.MTable

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.Duration
import oracle.jdbc.OracleDriver



trait JdbcProfileCust extends JdbcProfile {
     override def defaultTables(implicit ec: ExecutionContext): DBIO[Seq[MTable]] = {
       println("1111111111")
       
       MTable.getTables//("%")
     }
}

object NewSourceCodeGenerator {

  def run(slickDriver: String, jdbcDriver: String, url: String, outputDir: String, pkg: String, schema:String): Unit = {

    val driver: JdbcProfile =
      Class.forName(slickDriver + "$").getField("MODULE$").get(null).asInstanceOf[JdbcProfile]
    val dbFactory = driver.api.Database

    val db = dbFactory.forURL(url, driver = jdbcDriver, keepAliveConnection = true)

    try {
      val allSchemas = Await.result(db.run(
        driver.createModel(None, ignoreInvalidDefaults = false)(ExecutionContext.global).withPinnedSession), Duration.Inf)
      println( allSchemas)  
      val publicSchema = new Model(allSchemas.tables.filter(x=> x.name.schema.contains(schema)), allSchemas.options)
      
      new SourceCodeGenerator(publicSchema).writeToFile(slickDriver, outputDir, pkg)
    } finally db.close
  }
  
  def run(profile: String, jdbcDriver: String, url: String, outputDir: String, pkg: String, user: Option[String], password: Option[String]): Unit = {
    val profileInstance: JdbcProfile = Class.forName(profile + "$").getField("MODULE$").get(null).asInstanceOf[JdbcProfile]
    val dbFactory = profileInstance.api.Database
    val db = dbFactory.forURL(url, driver = jdbcDriver, user = user.getOrElse(null), password = password.getOrElse(null), keepAliveConnection = true)
    profileInstance.defaultTables
    try {
      val filteredTables = profileInstance.defaultTables.map(_.filter(t => t.name.name.contains("Z")))
      val modelAction = profileInstance.createModel(Option(filteredTables), true)
      //val fModel = db.run(profileInstance.createModel(None, true)(ExecutionContext.global).withPinnedSession)
      val fModel = db.run(modelAction)
      //.map(m => new Model(m.tables.filter(t => t.name.table == "MY_TEST"), m.options))
      val fRes = fModel.map { x => 
        new SourceCodeGenerator(x).writeToFile(profile,outputDir, pkg)
      }
      Await.result(fRes, Duration.Inf)
 /*     
      val m = Await.result(fModel, Duration.Inf)            
      val generatorInstance = new SourceCodeGenerator(m)
      generatorInstance.writeToFile(profile,outputDir, pkg)
*/      
    } finally db.close
  }  
  
}

object OracleModel {
  
  import slick.model.{QualifiedName, Model, Table, PrimaryKey, ForeignKey, ForeignKeyAction}
  
  import anorm._
  import anorm.SqlParser._
  import scala.collection.mutable
  
  def tpe(dataType: String, dataLength: Int) = dataType match {
    case "CHAR" | "VARCHAR" | "VARCHAR2" if dataLength == 1 => "Char"
    case "CHAR" | "VARCHAR" | "VARCHAR2" => "String"
    case "NUMBER" | "INTEGER" => "scala.math.BigDecimal"
    case "DATE" => "java.sql.Timestamp"
    case _ => "Any"  
  }
  
  def buildModel(jdbcUrl: String, userName: String, pass: String, schemaSeq: Seq[String], tableSeq: Seq[String]): Model = {
    
    val db = DB(jdbcUrl, userName, pass)
    val parser1 = str("owner") ~ str("table_name") map ( flatten ) *
    val parser2 = str("column_name") ~ str("data_type") ~ int("data_length") ~ str("nullable") map ( flatten ) *
    val parser3 = str("constraint_name") ~ str("column_name") map ( flatten ) *
    val parser4 = str("constraint_name") ~ str(".delete_rule") ~ str("column_name") map ( flatten ) *
    val parser5 = str("r_owner") ~ str("table_name") ~ str("constraint_name") ~ str("column_name") map ( flatten ) *

    var tables: Seq[Table] = Seq.empty

    db.withConnection { implicit connection =>
      
      val ownersTables: List[(String, String)] = (schemaSeq, tableSeq) match {
        case (Seq(), Seq()) =>
          SQL(""" SELECT owner, table_name FROM all_tables """).as(parser1)
          
        case (Seq(), _) =>
          SQL("""
              SELECT owner, table_name FROM all_tables 
                where table_name in ({tableNames})
          """).on("tableNames" -> tableSeq).as(parser1)
          
        case (_, Seq()) =>
          SQL("""
              SELECT owner, table_name FROM all_tables 
                where owner in ({owners})
          """).on("owners" -> schemaSeq).as(parser1)
          
        case (_, _) =>
          SQL("""
              SELECT owner, table_name FROM all_tables 
                where owner in ({owners}) and table_name in ({tableNames})
          """).on("owners" -> schemaSeq, "tableNames" -> tableSeq).as(parser1)          
      }  
      
        
      for(ownerTable <- ownersTables) {
        
        var qualifiedName = QualifiedName(ownerTable._2, Some(ownerTable._1))
        
        var columns = SQL("""
          select column_name, data_type, data_length, nullable 
            from all_tab_columns 
              where owner = {owner} and table_name = {tableName}""")
        .on("owner" -> ownerTable._1, "tableName" -> ownerTable._2).as(parser2)        
        .map { column =>          
          slick.model.Column(column._1, qualifiedName, tpe(column._2, column._3), column._4 == "Y") 
        }

        var sqlPrimaryKey = SQL("""
          select c1.constraint_name, c2.column_name 
            from  all_constraints c1 
              inner join all_cons_columns c2 on c1.constraint_name = c2.constraint_name 
              and c2.owner = c1.owner 
              and c2.table_name = c1.table_name 
              and c2.constraint_name = c1.constraint_name
            where c1.constraint_type = 'P' and c1.owner = {owner} and c1.table_name = {tableName}          
          """).on("owner" -> ownerTable._1, "tableName" -> ownerTable._2).as(parser3)
          
        var primaryKey = 
          if(sqlPrimaryKey.isEmpty) None 
          else {            
            val columnsPerKey = columns.filter(c => sqlPrimaryKey.exists(_._2 == c.name ) )
            if(columnsPerKey.isEmpty) None
            else Some( PrimaryKey(Some(sqlPrimaryKey(0)._1), qualifiedName, columnsPerKey) )
          }  
          
          tables.:+= (Table(qualifiedName, columns, primaryKey, Seq.empty, Seq.empty))
/*          
       
        case class Table(
          name: QualifiedName,
          columns: Seq[Column],
          primaryKey: Option[PrimaryKey],
          foreignKeys: Seq[ForeignKey],
          indices: Seq[Index],
          options: Set[TableOption[_]] = Set()
        )
        * /        
        */
      }
      
      tables map { table =>
        val foreignMap = SQL("""
            select c1.constraint_name, c1.delete_rule, c2.column_name
              from  all_constraints c1 
                inner join all_cons_columns c2 on c1.constraint_name = c2.constraint_name 
                and c2.owner = c1.owner 
                and c2.table_name = c1.table_name 
                and c2.constraint_name = c1.constraint_name
              where c1.constraint_type = 'R' and c1.owner = {owner} and c1.table_name = {tableName}
              order by c1.constraint_name
        """).on("owner" -> table.name.schema, "tableName" -> table.name.table).as(parser4).groupBy(_._1)
        
        val foreignKeys = mutable.Buffer.empty[ForeignKey]
        for(f <- foreignMap) {
          val r_foreignMap = SQL("""          
            select c1.r_owner, c2.table_name, c2.constraint_name, c2.column_name
            from  all_constraints c1 
              inner join all_cons_columns c2 on c2.owner = c1.r_owner 
              and c2.constraint_name = c1.r_constraint_name
            where c1.owner = {owner} and c1.constraint_name = {constraintName}  
          """).on("owner" -> table.name.schema, "constraintName" -> f._1).as(parser5).groupBy(_._1)            
                 
          val referencingTable = table.name
          val referencingColumns = table.columns.filter(c => f._2.exists(t => c.name == t._3))
          val referencedTable = tables.find(t => r_foreignMap.exists(r_f => r_f._1 == t.name.schema && r_f._2.exists(s => s._2 == t.name.table)) )
                                      .map(t => t.name).get // или выбрать в базе
          val referencedColumns = tables.find(t => r_foreignMap.exists(r_f => r_f._1 == t.name.schema && r_f._2.exists(s => s._2 == t.name.table)) )
                                      .map(t => t.columns).get // или выбрать в базе
                                      
          val onUpdate = ForeignKeyAction.NoAction
          val onDelete = f._2(0)._2 match {
            case "CASCADE " => ForeignKeyAction.Cascade
            case "SET NULL" => ForeignKeyAction.SetNull
            case _ => ForeignKeyAction.NoAction
          }
          
          foreignKeys += ForeignKey(Some(f._1), referencingTable, referencingColumns, referencedTable, referencedColumns, onUpdate, onDelete)  
                  
        }
        
        table.copy(foreignKeys = foreignKeys) 
        
      }
      
      
      
    }
    
    
    
    Model(tables)
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



object Example extends App {
  val outputDir = "slick" // place generated files in sbt's managed sources folder
  //val url = "jdbc:oracle:thin:oas/oas@//msk-test-01:1521/prod"
  val jdbcUrl = "jdbc:oracle:thin:@//msk-test-01:1521/prod"  
  val jdbcDriver = "oracle.jdbc.OracleDriver"
  val slickDriver = "slick.jdbc.OracleProfile"
  val pkg = "demo"  
  val schema = "OAS"
  val userName = "test_dev"  
  val pass = "test_dev"  
  
  import NewSourceCodeGenerator._
  
  //run(slickDriver, jdbcDriver, url, outputDir, pkg, schema)
  //run(slickDriver, jdbcDriver, url, outputDir, pkg, Option(name), Option(pass))
  val model = OracleModel.buildModel(jdbcUrl, userName, pass, Seq("OAS"), Seq.empty)
      
  new SourceCodeGenerator(model).writeToFile(slickDriver, outputDir, pkg)
}
