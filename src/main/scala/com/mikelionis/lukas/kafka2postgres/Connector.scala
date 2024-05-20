package com.mikelionis.lukas.kafka2postgres

import com.messages.events.user.{UserCreated, UserDeleted, UserEmailUpdated, UserForgotten}
import com.mikelionis.lukas.kafka2postgres.util.{Logging, PostgresErrorCodes}
import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.serialization.{ByteBufferDeserializer, StringDeserializer}

import java.nio.ByteBuffer
import java.sql.{Connection, DriverManager, PreparedStatement, SQLException}
import java.time.Duration
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean
import scala.jdk.CollectionConverters._
import scala.util.Using

object Connector {
  private def newKafkaConsumer(kafkaConfig: Config): KafkaConsumer[String, ByteBuffer] = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getString(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG))
    props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaConfig.getString(ConsumerConfig.GROUP_ID_CONFIG))
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaConfig.getString(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG))
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteBufferDeserializer].getName)
    new KafkaConsumer[String, ByteBuffer](props)
  }

  private def newPostgresConnection(postgresConfig: Config): Connection = {
    DriverManager.getConnection(
      postgresConfig.getString("url"),
      postgresConfig.getString("user"),
      postgresConfig.getString("password")
    )
  }
}

class Connector(config: Config) extends Logging {
  import Connector._

  private val running = new AtomicBoolean(false)

  private val kafkaConfig = config.getConfig("kafka")
  private val postgresConfig = config.getConfig("postgres")
  private val connectorConfig = config.getConfig("connector")

  private val srcTopic = connectorConfig.getString("src.topic")
  private val srcSchemaHeaderName = connectorConfig.getString("src.schema.header")
  private val usersTable = connectorConfig.getString("trg.table.users")
  private val forgottenUsersTable = connectorConfig.getString("trg.table.forgotten")

  def run(): Boolean = {
    if (running.compareAndSet(false, true)) {
      Using.resources(newKafkaConsumer(kafkaConfig), newPostgresConnection(postgresConfig)) { (consumer, con) =>
        log.info(s"Initialized kafka consumer '${kafkaConfig.getString(ConsumerConfig.GROUP_ID_CONFIG)}'")
        log.info(s"Initialized Postgres connection to ${postgresConfig.getString("url")}")

        consumer.subscribe(List(srcTopic).asJava)
        log.info(s"Subscribed to topic '$srcTopic'")

        log.info(s"Running the connector...")
        while (running.get()) {
          val records = consumer.poll(Duration.ofMillis(500)).records(srcTopic).asScala.toList
          log.info(s"Connector read ${records.size}")

          val handledRecords = records.map(handleRecord(con)).count(identity)
          log.info(s"Connector handled $handledRecords/${records.size} records")

          consumer.commitSync()
        }

        log.info("Kafka and Postgres connection have been closed")
        true
      }
    } else {
      log.warn("Connector has already been started, ignoring duplicate 'run' call")
      false
    }
  }

  def stop(): Boolean = {
    if (running.compareAndSet(true, false)) {
      log.info("Stopping the connector...")
      true
    } else {
      log.warn("Connector is not started, ignoring 'close' call")
      false
    }
  }

  private def handleRecord(con: Connection)(record: ConsumerRecord[String, ByteBuffer]): Boolean = {
    val headers = record.headers().headers(srcSchemaHeaderName).iterator()
    if (headers.hasNext) {
      val headerValue = new String(headers.next().value())
      UserEventSchema(headerValue) match {
        case Some(UserEventSchema.UserCreated) =>
          insertUser(con)(Decoders.userCreated.decode(record.value())); true
        case Some(UserEventSchema.UserEmailUpdated) =>
          updateUserEmail(con)(Decoders.userEmailUpdated.decode(record.value())); true
        case Some(UserEventSchema.UserDeleted) =>
          updateUserStatus(con)(Decoders.userDeleted.decode(record.value())); true
        case Some(UserEventSchema.UserForgotten) =>
          forgetUser(con)(Decoders.userForgotten.decode(record.value())); true
        case None => log.warn(s"Unsupported schema '$headerValue'"); false
      }
    } else {
      log.warn(s"Missing header '$srcSchemaHeaderName'")
      false
    }
  }

  private def insertUser(con: Connection)(event: UserCreated): Unit = {
    val query = s"INSERT INTO $usersTable (id, name, email) VALUES(?, ?, ?);"
    Using.resource(con.prepareStatement(query)) { stmt =>
      stmt.setString(1, event.id.toString)
      stmt.setString(2, event.name.toString)
      stmt.setString(3, event.email.toString)

      try stmt.execute()
      catch {
        case ex: SQLException if ex.getErrorCode == PostgresErrorCodes.UniqueViolation =>
          log.warn(s"Postgres exception while inserting user (event=$event) in $usersTable", ex)
      }
    }
  }

  private def updateUserEmail(con: Connection)(event: UserEmailUpdated): Unit = {
    val query = s"UPDATE $usersTable SET email = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?;"
    Using.resource(con.prepareStatement(query)) { stmt =>
      stmt.setString(1, event.email.toString)
      stmt.setString(2, event.id.toString)

      try {
        val updatedRows = stmt.executeUpdate()
        if (updatedRows == 0) log.warn(s"Attempted to update email of a non-existing user under id '${event.id}' in $usersTable")
      } catch {
        case ex: SQLException if ex.getErrorCode == PostgresErrorCodes.UniqueViolation =>
          log.warn(s"Postgres exception while updating user email (event=$event)", ex)
      }
    }
  }

  private def updateUserStatus(con: Connection)(event: UserDeleted): Unit = {
    val query = s"UPDATE $usersTable SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?;"
    Using.resource(con.prepareStatement(query)) { stmt =>
      stmt.setString(1, UserStatus.toString(UserStatus.Deleted))
      stmt.setString(2, event.id.toString)
      val updatedRows = stmt.executeUpdate()
      if (updatedRows == 0) log.warn(s"Attempted to mark deleted a non-existing user under id '${event.id}' in $usersTable")
    }
  }

  private def forgetUser(con: Connection)(event: UserForgotten): Unit = {
    val deleteQuery = s"DELETE FROM $usersTable WHERE id = ?;"
    val insertQuery = s"INSERT INTO $forgottenUsersTable (id) VALUES(?);"

    con.setAutoCommit(false)

    var deleteStmt: PreparedStatement = null
    var insertStmt: PreparedStatement = null
    try {
      deleteStmt = con.prepareStatement(deleteQuery)
      deleteStmt.setString(1, event.id.toString)
      val deletedRows = deleteStmt.executeUpdate()
      if (deletedRows == 0) log.warn(s"Attempted to delete a non-existing user under id '${event.id}' in $usersTable")

      insertStmt = con.prepareStatement(insertQuery)
      insertStmt.setString(1, event.id.toString)
      try insertStmt.executeUpdate()
      catch {
        case ex: SQLException if ex.getErrorCode == PostgresErrorCodes.UniqueViolation =>
          log.warn(s"Postgres exception while inserting user under id '${event.id}' in $forgottenUsersTable", ex)
      }

      con.commit()
    } catch {
      case ex: Throwable =>
        con.rollback()
        throw ex
    } finally {
      if (deleteStmt != null) deleteStmt.close()
      if (insertStmt != null) insertStmt.close()
    }

    con.setAutoCommit(true)
  }
}
