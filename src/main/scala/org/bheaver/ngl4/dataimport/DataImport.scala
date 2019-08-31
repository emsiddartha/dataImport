package org.bheaver.ngl4.dataimport

import java.sql.{Connection, DriverManager}

import org.mongodb.scala.{Completed, MongoClient, Observer}
import org.mongodb.scala.bson.collection.immutable.Document
import Helpers._

import scala.collection.mutable.ListBuffer



object DataImport extends App {
  val mongoDatabaseName = "lib3"

  Class.forName("org.postgresql.Driver")
  val url = "jdbc:postgresql://localhost/eflu"
  val connection = DriverManager.getConnection(url,"newgenlib","newgenlib")
  val tableNames = listTableNames(connection)
  val mongoClient: MongoClient = MongoClient("mongodb://localhost")
  val database = mongoClient.getDatabase(mongoDatabaseName)


  tableNames.foreach(tableName => {
    val statement = connection.createStatement()
    val resultSet = statement.executeQuery(s"select row_to_json(t) from ${tableName} t")
    val collection = database.getCollection(tableName)
    while(resultSet.next()){
      val json = resultSet.getString(1)
      collection.insertOne(Document(json)).results
    }
    resultSet.close()
    statement.close()
  })

  connection.close()


  def listTableNames(con: Connection): Seq[String] = {
    var tableNames = new ListBuffer[String]
    val statement = connection.createStatement()
    val resultSet = statement.executeQuery("SELECT table_name  FROM information_schema.tables WHERE table_schema='public'   AND table_type='BASE TABLE'")
    while(resultSet.next()){
      tableNames += resultSet.getString(1)
    }
    resultSet.close()
    statement.close()
    val list = tableNames.toList
    println(list)
    list
  }
}
