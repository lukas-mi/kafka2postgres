package com.mikelionis.lukas.kafka2postgres

import com.messages.events.user.UserCreated
import com.mikelionis.lukas.kafka2postgres.util.Logging
import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.serialization.{ByteBufferDeserializer, StringDeserializer}

import java.nio.ByteBuffer
import java.sql.{Connection, DriverManager, SQLException}
import java.time.Duration
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean
import scala.jdk.CollectionConverters._
import scala.util.Using

class Connector(config: Config) extends Logging {
  private val running = new AtomicBoolean(false)

  private val kafkaConfig = config.getConfig("kafka")
  private val postgresConfig = config.getConfig("postgres")
  private val connectorConfig = config.getConfig("connector")

  private val srcTopic = connectorConfig.getString("src.topic")
  private val srcSchemaHeaderName = connectorConfig.getString("src.schema.header")
  private val trgTable = connectorConfig.getString("trg.table")

  // TODO: logging
  // TODO: multi-threaded use case

  def run(): Boolean = {
    if (running.compareAndSet(false, true)) {
      // TODO: add retry logic
      Using.resources(newKafkaConsumer(), newPostgresConnection()) { (consumer, con) =>
        log.info(s"Initialized kafka consumer '${kafkaConfig.getString(ConsumerConfig.GROUP_ID_CONFIG)}'")
        log.info(s"Initialized Postgres connection to ${postgresConfig.getString("url")}")

        consumer.subscribe(List(srcTopic).asJava)
        log.info(s"Subscribed to topic '$srcTopic'")

        // TODO: manual consumer offset commit!
        log.info(s"Running the connector...")
        while (running.get()) {
          val records = consumer.poll(Duration.ofMillis(500)).records(srcTopic).asScala.toList
          log.info(s"Connector read ${records.size}")
          records.foreach(handleRecord(con))
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

  private def handleRecord(con: Connection)(record: ConsumerRecord[String, ByteBuffer]): Unit = {
    // TODO: handle without header and unsupported headers
    val headers = record.headers().headers(srcSchemaHeaderName).iterator()
    if (headers.hasNext) {
      val headerValue = new String(headers.next().value())
      UserEventSchema(headerValue) match {
        case Some(UserEventSchema.UserCreated) =>
          val userCreatedEvent = Decoders.userCreated.decode(record.value())
          val query = s"INSERT INTO $trgTable (id, name, email) VALUES(?, ?, ?);"
          Using.resource(con.prepareStatement(query)) { stmt =>
            stmt.setString(1, userCreatedEvent.id.toString)
            stmt.setString(2, userCreatedEvent.name.toString)
            stmt.setString(3, userCreatedEvent.email.toString)

            try stmt.execute()
            catch {
              case ex: SQLException if ex.getErrorCode == PostgresErrorCodes.UniqueViolation =>
                log.warn(s"Postgres exception while inserting $userCreatedEvent", ex)
            }
          }
        case Some(UserEventSchema.UserDeleted) => ???
        case Some(UserEventSchema.UserEmailUpdated) => ???
        case Some(UserEventSchema.UserForgotten) => ???
        case None => log.warn(s"Unsupported schema '$headerValue'")
      }
    } else {
      log.warn(s"Missing header '$srcSchemaHeaderName'")
    }
  }

  private def newKafkaConsumer(): KafkaConsumer[String, ByteBuffer] = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getString(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG))
    props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaConfig.getString(ConsumerConfig.GROUP_ID_CONFIG))
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaConfig.getString(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG))
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteBufferDeserializer].getName)
    new KafkaConsumer[String, ByteBuffer](props)
  }

  private def newPostgresConnection(): Connection = {
    DriverManager.getConnection(
      postgresConfig.getString("url"),
      postgresConfig.getString("user"),
      postgresConfig.getString("password")
    )
  }
}
