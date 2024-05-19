package com.mikelionis.lukas.kafka2postgres

import org.testcontainers.containers.PostgreSQLContainer

import java.sql.{Connection, DriverManager, Timestamp}
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Using

trait PostgresUtils {
  case class User(id: String, name: String, email: String, createdAt: Timestamp, updateAt: Timestamp, status: String)

  protected val postgres: PostgreSQLContainer[Nothing]
  protected var postgresCon: Connection

  protected val usersTableDef: String = getSQLQuery("src/main/sql/users.sql")
  protected val forgottenUsersTableDef: String = getSQLQuery("src/main/sql/forgotten_users.sql")

  def newPostgresConnection(): Connection = {
    DriverManager.getConnection(
      postgres.getJdbcUrl,
      postgres.getUsername,
      postgres.getPassword
    )
  }

  def createPostgresTables(usersTable: String, forgottenUsersTable: String): Unit = {
    Using(postgresCon.createStatement())(_.execute(usersTableDef.replace("users", usersTable)))
    Using(postgresCon.createStatement())(_.execute(forgottenUsersTableDef.replace("forgotten_users", forgottenUsersTable)))

    // ensure tables exists before doing anything else in tests
    Using(postgresCon.createStatement())(_.execute(s"SELECT 1 FROM $usersTable;"))
    Using(postgresCon.createStatement())(_.execute(s"SELECT 1 FROM $forgottenUsersTable"))
  }

  def selectUsers(table: String): List[User] = {
    val buffer = ListBuffer.empty[User]
    Using(postgresCon.createStatement()) { stmt =>
      Using(stmt.executeQuery(s"SELECT id, name, email, created_at, updated_at, status FROM $table;")) { rs =>
        while (rs.next()) {
          buffer.addOne(
            User(
              rs.getString("id"),
              rs.getString("name"),
              rs.getString("email"),
              rs.getTimestamp("created_at"),
              rs.getTimestamp("updated_at"),
              rs.getString("status")
            )
          )
        }
      }
    }
    buffer.toList
  }

  def getSQLQuery(path: String): String = Using.resource(Source.fromFile(path))(_.getLines().mkString("\n"))
}
