package com.mikelionis.lukas.kafka2postgres

import com.mikelionis.lukas.kafka2postgres.util._
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.producer.KafkaProducer
import org.scalatest.{Assertion, BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import org.scalatest.time.{Milliseconds, Seconds, Span}
import org.testcontainers.containers.{KafkaContainer, Network, PostgreSQLContainer}

import java.nio.ByteBuffer
import java.sql.Connection
import java.time.Duration

abstract class ConnectorSpecWrapper extends AnyFlatSpec
  with should.Matchers
  with BeforeAndAfterAll
  with BeforeAndAfterEach
  with Eventually
  with KafkaUtils
  with PostgresUtils
  with EventGenHelper {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(
    timeout = Span.apply(5, Seconds),
    interval = Span.apply(200, Milliseconds)
  )

  private val network = Network.newNetwork()

  override val kafka: KafkaContainer = new KafkaContainer(kafkaImage)
    .withNetwork(network)
    .withStartupTimeout(Duration.ofSeconds(60))
  override var kafkaAdmin: AdminClient = _
  override var kafkaProducer: KafkaProducer[String, ByteBuffer] = _

  override val postgres: PostgreSQLContainer[Nothing] = new PostgreSQLContainer(postgresImage)
  override var postgresCon: Connection = _

  protected val srcTopic = s"user-events-test"
  protected val usersTable = s"users_test"
  protected val forgottenUsersTable = s"forgotten_test"

  override def beforeAll(): Unit = {
    super.beforeAll()
    kafka.start()
    postgres.start()
    kafkaAdmin = newAdmin()
    kafkaProducer = newProducer()
    postgresCon = newPostgresConnection()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    createTopic(srcTopic)
    createPostgresTables(usersTable, forgottenUsersTable)
    Thread.sleep(200) // TODO: inspect if necessary
  }

  override def afterEach(): Unit = {
    super.beforeEach()
    deleteTopic(srcTopic)
    dropPostgresTables(usersTable, forgottenUsersTable)
    Thread.sleep(200) // TODO: inspect if necessary
  }

  override def afterAll(): Unit = {
    if (kafkaAdmin != null) kafkaAdmin.close()
    if (kafkaProducer != null) kafkaProducer.close()
    if (postgresCon != null) postgresCon.close()
    kafka.stop()
    postgres.stop()
    network.close()
    super.afterAll()
  }

  def newConnector(srcTopic: String, usersTable: String, forgottenUsersTable: String): Connector = {
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
          |  src.schema.header = $schemaHeaderName
          |  trg.table.users = $usersTable
          |  trg.table.forgotten = $forgottenUsersTable
          |}
          |""".stripMargin
      )
      new Connector(config)
    } else {
      throw new Exception("Cannot create a connector, kafka and/or postgres is/are not running")
    }
  }

  def withConnector(code: => Assertion): Assertion = {
    val connector = newConnector(srcTopic, usersTable, forgottenUsersTable)
    new Thread(
      () => connector.run(),
      "connector-starter-in-spec"
    ).start()

    try code
    finally connector.stop()
  }

  def assertUser(
      user: User, beforeCreationMillis: Long, checkUpdatedAt: Boolean,
      expectedId: String, expectedName: String, expectedEmail: String, expectedStatus: UserStatus
  ): Assertion = {
    user.id shouldBe expectedId
    user.name shouldBe expectedName
    user.email shouldBe expectedEmail
    user.status shouldBe UserStatus.toString(expectedStatus)
    user.createdAt.getTime should be > beforeCreationMillis
    if (checkUpdatedAt) {
      user.createdAt.getTime should be < user.updateAt.getTime
    } else {
      user.updateAt shouldBe null
    }
  }

  def assertForgottenUser(
      user: ForgottenUser, beforeForgottenMillis: Long,
      expectedId: String
  ): Assertion = {
    user.id shouldBe expectedId
    user.forgottenAt.getTime should be > beforeForgottenMillis
  }
}
