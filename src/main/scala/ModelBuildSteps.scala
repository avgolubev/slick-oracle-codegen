
import java.sql.Connection
import scala.collection.mutable
import anorm.{SQL, ~}
import anorm.SqlParser._
import slick.model.{Column, PrimaryKey, QualifiedName, ForeignKey, Table, ForeignKeyAction}


object ModelBuildSteps {
    
  val qualifiedNameParser = str("owner") ~ str("table_name") map { case owner ~ table_name => QualifiedName(table_name, Some(owner)) } *
  val columnsParser = str("column_name") ~ str("data_type") ~ int("data_length") ~ str("nullable") map ( flatten ) *
  val primaryKeyParser = str("constraint_name") ~ str("column_name") map ( flatten ) *
  val parser4 = str("constraint_name") ~ str(".delete_rule") ~ str("column_name") map ( flatten ) *
  val parser5 = str("r_owner") ~ str("table_name") ~ str("constraint_name") ~ str("column_name") map ( flatten ) *  
  val parser6 = str("r_owner") ~ str("table_name") ~ str("constraint_name") ~ str("column_name") map { 
    case r_owner ~ table_name ~ constraint_name ~ column_name => 
      (QualifiedName(table_name, Some(r_owner)) -> column_name) 
  } *
  
  //QualifiedName(schemaTableName._2, Some(schemaTableName._1))
  
  def buildSchemaTableNames2(schemaNames: Seq[String], tableNames: Seq[String])(implicit connection: Connection) = (schemaNames, tableNames) match {
    case (Seq(), Seq()) =>
      SQL(""" SELECT owner, table_name FROM all_tables """).as(qualifiedNameParser)
      
    case (Seq(), _) =>
      SQL("""
          SELECT owner, table_name FROM all_tables 
            where table_name in ({tableNames})
      """).on("tableNames" -> tableNames).as(qualifiedNameParser)
      
    case (_, Seq()) =>
      SQL("""
          SELECT owner, table_name FROM all_tables 
            where owner in ({owners})
      """).on("owners" -> schemaNames).as(qualifiedNameParser)
      
    case (_, _) =>
      SQL("""
          SELECT owner, table_name FROM all_tables 
            where owner in ({owners}) and table_name in ({tableNames})
      """).on("owners" -> schemaNames, "tableNames" -> tableNames).as(qualifiedNameParser)          
  }
  
  
  def buildSchemaTableNames(schemaNames: Seq[String], tableNames: Seq[String])(implicit connection: Connection) = SQL { 
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

  def tpe(dataType: String, dataLength: Int) = dataType match {
    case "CHAR" | "VARCHAR" | "VARCHAR2" if dataLength == 1 => "Char"
    case "CHAR" | "VARCHAR" | "VARCHAR2" => "String"
    case "NUMBER" | "INTEGER" => "scala.math.BigDecimal"
    case "DATE" => "java.sql.Timestamp"
    case _ => "Any"  
  }  
  
  def buildColumns(qualifiedName: QualifiedName)(implicit connection: Connection): Seq[Column] = SQL("""
      select column_name, data_type, data_length, nullable 
        from all_tab_columns 
          where owner = {owner} and table_name = {tableName} order by column_id""")
    .on("owner" -> qualifiedName.schema, "tableName" -> qualifiedName.table).as(columnsParser)        
    .map { column =>          
      slick.model.Column(column._1, qualifiedName, tpe(column._2, column._3), column._4 == "Y") 
    }
  
  def buildPrimaryKey(qualifiedName: QualifiedName, columns: Seq[Column])(implicit connection: Connection) = {
    
    val sqlPrimaryKey = SQL("""
      select c1.constraint_name, c2.column_name 
        from  all_constraints c1 
          inner join all_cons_columns c2 on c1.constraint_name = c2.constraint_name 
          and c2.owner = c1.owner 
          and c2.table_name = c1.table_name 
          and c2.constraint_name = c1.constraint_name
        where c1.constraint_type = 'P' and c1.owner = {owner} and c1.table_name = {tableName}          
      """).on("owner" -> qualifiedName.schema, "tableName" -> qualifiedName.table).as(primaryKeyParser)   
      
    if(sqlPrimaryKey.isEmpty) None 
    else {            
      val columnsPerKey = columns.filter(c => sqlPrimaryKey.exists(_._2 == c.name ) )
      if(columnsPerKey.isEmpty) None
      else Some( PrimaryKey(Some(sqlPrimaryKey(0)._1), qualifiedName, columnsPerKey) )
    }      
    
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
          val referencedPrimaryKeyColumns = buildColumns(referencedTableConstraintColumns(0)._1).filter(c => referencedTableConstraintColumns.exists(p => p._2 == c.name))
          referencedQualifiedNames += referencedTableConstraintColumns(0)._1
          (referencedTableConstraintColumns(0)._1, referencedPrimaryKeyColumns)
          
        }(t => (t.name, t.columns))
                                                                   
      val onUpdate = ForeignKeyAction.NoAction
      val onDelete = f._2(0)._2 match {
        case "CASCADE " => ForeignKeyAction.Cascade
        case "SET NULL" => ForeignKeyAction.SetNull
        case _ => ForeignKeyAction.NoAction
      }
      
      foreignKeys += ForeignKey(Some(f._1), referencingTable, referencingColumns, referencedTable, referencedColumns, onUpdate, onDelete)  
              
    }
    (collection.immutable.Seq(foreignKeys.toSeq: _*), collection.immutable.Seq(referencedQualifiedNames.toSeq: _*))    
  }
  
  
  
}