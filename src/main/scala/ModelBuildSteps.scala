
import java.sql.Connection
import scala.collection.mutable
import anorm.{SQL, ~}
import anorm.SqlParser._
import slick.model.{Column, PrimaryKey, QualifiedName, ForeignKey, Table, ForeignKeyAction}
import slick.sql.SqlProfile
import slick.relational.RelationalProfile
import slick.ast.ColumnOption

object ModelBuildSteps {
    
  val qualifiedNameParser = str("owner") ~ str("table_name") map { case owner ~ table_name => QualifiedName(table_name, Option(owner)) } *
  val columnsParser = str("column_name") ~ str("data_type") ~ int("data_length") ~ str("nullable") ~ get[Option[String]]("constraint_name") map ( flatten ) *
  val primaryKeyParser = str("constraint_name") ~ str("column_name") map ( flatten ) *
  val parser4 = str("constraint_name") ~ str(".delete_rule") ~ str("column_name") map ( flatten ) * 
  val parser6 = str("r_owner") ~ str("table_name") ~ str("constraint_name") ~ str("column_name") map { 
    case r_owner ~ table_name ~ constraint_name ~ column_name => 
      (QualifiedName(table_name, Option(r_owner)) -> column_name) 
  } *
  
  val parser444 = str("constraint_name") ~ str(".delete_rule") ~ str("column_name") map ( flatten ) *
      
  def buildSchemaTableNames(schemaNames: Seq[String], tableNames: Seq[String])(implicit connection: Connection): Seq[QualifiedName]  = SQL { 
      (schemaNames, tableNames) match {
        case (Seq(), Seq()) =>
          """ SELECT owner, table_name FROM all_tables """
          
        case (Seq(), _) =>
          """ SELECT owner, table_name FROM all_tables where table_name in ({tableNames}) """
          
        case (_, Seq()) =>
          """ SELECT owner, table_name FROM all_tables where owner in ({owners}) """
          
        case (_, _) =>
          """ SELECT owner, table_name FROM all_tables where owner in ({owners}) and table_name in ({tableNames}) """          
    }  
  }.on("owners" -> schemaNames, "tableNames" -> tableNames).as(qualifiedNameParser).toSeq

  private def tpe(dataType: String, dataLength: Int) = dataType match {
    case "CHAR" | "VARCHAR" | "VARCHAR2" if dataLength == 1 => "Char"
    case "CHAR" | "VARCHAR" | "VARCHAR2" => "String"
    case "NUMBER" | "INTEGER" => "scala.math.BigDecimal"
    case "DATE" => "java.sql.Timestamp"
    case _ => "Any"  
  }  
  
  private def varying(dataType: String): Boolean = {
      Seq("NVARCHAR", "VARCHAR", "VARCHAR2", "LONGVARCHAR", "LONGNVARCHAR") contains dataType
  }
  
  private def length(tpe: String, size: Int): Option[Int] = if(tpe == "String") Some(size.toInt) else None
  
  def buildColumns(qualifiedName: QualifiedName)(implicit connection: Connection): (Seq[Column], Option[PrimaryKey]) = {     
    val columnsProperties = 
      SQL("""
        select a.column_name, a.data_type, a.data_length, a.nullable, c2.constraint_name--, a.data_default
          from  all_tab_columns a 
            left join all_constraints c1 on  a.owner = c1.owner  and a.table_name = c1.table_name and c1.constraint_type = 'P'               
            left join all_cons_columns c2 on a.column_name = c2.column_name  and a.owner = c2.owner and a.table_name = c2.table_name 
              and c1.constraint_name = c2.constraint_name  
          where a.owner = {owner} and a.table_name = {tableName} order by a.column_id""")
      .on("owner" -> qualifiedName.schema, "tableName" -> qualifiedName.table).as(columnsParser)
      
      lazy val primaryKeyCount = columnsProperties.count(_._5.isDefined )
      
      val columnsPK = columnsProperties.map { column =>          
        val tpeVal = tpe(column._2, column._3)
        ( Column(column._1, qualifiedName, tpeVal, column._4 == "Y", 
            Set() ++          
            Some(column._2).map(SqlProfile.ColumnOption.SqlType) ++ // original - dbType.map(SqlProfile.ColumnOption.SqlType)
            length(tpeVal, column._3).map(RelationalProfile.ColumnOption.Length.apply(_,varying = varying(column._2))) ++
            (if(column._5.isDefined && primaryKeyCount == 1) Some(ColumnOption.PrimaryKey) else None) 
            // ? ++ (if(!autoInc) convenientDefault else None) )
        ), column._5)                   
      }
    
    (columnsPK.map(_._1), if(primaryKeyCount > 1) buildPrimaryKey(qualifiedName, columnsPK) else None )
    
  }
  
  def buildPrimaryKey(qualifiedName: QualifiedName, columnsPK: Seq[(Column, Option[String])])(implicit connection: Connection) = {                   
      val columnsPerKey = columnsPK.filter(_._2.isDefined)
      Some( PrimaryKey(Some(columnsPerKey(0)._2.get), qualifiedName, columnsPerKey.map(_._1)) )          
  }
  
  def buildForeignKeys(allTables: Seq[Table], currentTable: Table)(implicit connection: Connection): (Seq[ForeignKey], Seq[QualifiedName]) = {
    
    val foreignKeys = mutable.Buffer.empty[ForeignKey]
    
    val foreignMap = SQL("""
        select c1.constraint_name, c1.delete_rule, c2.column_name
          from  all_constraints c1 
            inner join all_cons_columns c2 on c1.constraint_name = c2.constraint_name 
            and c2.owner = c1.owner 
            and c2.table_name = c1.table_name 
            and c2.constraint_name = c1.constraint_name
          where c1.constraint_type = 'R' and c1.owner = {owner} and c1.table_name = {tableName}
          order by c1.constraint_name
    """).on("owner" -> currentTable.name.schema, "tableName" -> currentTable.name.table).as(parser4).groupBy(_._1)        
    
    val referencedQualifiedNames =  mutable.Set.empty[QualifiedName]
    
    for(f <- foreignMap) {
      
      val referencedTableConstraintColumns = SQL("""          
        select c1.r_owner, c2.table_name, c2.constraint_name, c2.column_name
        from  all_constraints c1 
          inner join all_cons_columns c2 on c2.owner = c1.r_owner 
          and c2.constraint_name = c1.r_constraint_name
        where c1.owner = {owner} and c1.constraint_name = {constraintName}  
      """).on("owner" -> currentTable.name.schema, "constraintName" -> f._1).as(parser6)      
                                         
      val referencingTable = currentTable.name
      val referencingColumns = currentTable.columns.filter(c => f._2.exists(_._3 == c.name))
                                
      val (referencedTable, referencedColumns) =   
        allTables.find {t =>
          referencedTableConstraintColumns.exists {_._1 == t.name} 
        }.fold {
          val referencedPrimaryKeyColumns = buildColumns(referencedTableConstraintColumns(0)._1)._1.filter(c => referencedTableConstraintColumns.exists(p => p._2 == c.name))
          referencedQualifiedNames += referencedTableConstraintColumns(0)._1
          (referencedTableConstraintColumns(0)._1, referencedPrimaryKeyColumns)
          
        }(t => (t.name, t.columns))
              
      val onUpdate = ForeignKeyAction.Cascade  
      val onDelete = f._2(0)._2 match {
        case "CASCADE" => ForeignKeyAction.Cascade
        case "SET NULL" => ForeignKeyAction.SetNull
        case _ => ForeignKeyAction.Restrict
      }
        
      foreignKeys += ForeignKey(Some(f._1), referencingTable, referencingColumns, referencedTable, referencedColumns, onUpdate, onDelete)  
              
    }
    (Seq(foreignKeys.toSeq: _*), Seq(referencedQualifiedNames.toSeq: _*))    
  }
      
}