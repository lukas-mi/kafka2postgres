package com.mikelionis.lukas.kafka2postgres

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.admin.AdminClient
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.time.{Milliseconds, Seconds, Span}
import org.testcontainers.containers.{KafkaContainer, Network, PostgreSQLContainer}
import org.testcontainers.utility.DockerImageName

import java.sql.Connection
import java.time.Duration

abstract class ConnectorSpecWrapper extends AnyFlatSpec
  with BeforeAndAfterAll
  with Eventually
  with KafkaUtils
  with PostgresUtils
  with EventGenHelper {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(
    timeout = Span.apply(5, Seconds),
    interval = Span.apply(200, Milliseconds)
  )

  private val network = Network.newNetwork()

  override val kafka: KafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))
    .withNetwork(network)
    .withStartupTimeout(Duration.ofSeconds(60))
  override var kafkaAdmin: AdminClient = _

  override val postgres: PostgreSQLContainer[Nothing] = new PostgreSQLContainer(DockerImageName.parse("postgres:13.15"))
  override var postgresCon: Connection = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    kafka.start()
    postgres.start()
    kafkaAdmin = newAdmin()
    postgresCon = newPostgresConnection()
  }

  override def afterAll(): Unit = {
    if (kafkaAdmin != null) {
      kafkaAdmin.close()
    }
    if (postgresCon != null) {
      postgresCon.close()
    }
    kafka.stop()
    postgres.stop()
    network.close()
    super.afterAll()
  }

  def newConnector(srcTopic: String, trgTable: String): Connector = {
    if (kafka.isRunning && postgres.isRunning) {
      val config = ConfigFactory.parseString(
        s"""
          |kafka {
          |  bootstrap.servers = "${kafka.getBootstrapServers}"
          |  group.id = "kafka2postgres-user-events-test"
          |  auto.offset.reset = "earliest"
          |}
          |
          |postgres {
          |  url = "${postgres.getJdbcUrl}"
          |  user = "${postgres.getUsername}"
          |  password = "${postgres.getPassword}"
          |}
          |
          |connector {
          |  src.topic = $srcTopic
          |  src.schema.header = $srcSchemaHeaderName
          |  trg.table = $trgTable
          |}
          |""".stripMargin
      )
      new Connector(config)
    } else {
      throw new Exception("Cannot create a connector, kafka and/or postgres is/are not running")
    }
  }
}
