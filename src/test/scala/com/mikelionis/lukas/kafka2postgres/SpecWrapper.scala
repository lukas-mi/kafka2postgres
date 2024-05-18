package com.mikelionis.lukas.kafka2postgres

import org.apache.kafka.clients.admin.AdminClient
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.time.{Milliseconds, Seconds, Span}
import org.testcontainers.containers.{KafkaContainer, Network, PostgreSQLContainer}
import org.testcontainers.utility.DockerImageName

import java.time.Duration

abstract class SpecWrapper extends AnyFlatSpec with BeforeAndAfterAll with Eventually with KafkaUtils {
  override implicit val patienceConfig: PatienceConfig = PatienceConfig(
    timeout = Span.apply(5, Seconds),
    interval = Span.apply(200, Milliseconds)
  )

  private val network = Network.newNetwork()

  private val postgres = new PostgreSQLContainer(DockerImageName.parse("postgres:13.15"))

  override val kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))
    .withNetwork(network)
    .withStartupTimeout(Duration.ofSeconds(60))

  override var kafkaAdmin: AdminClient = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    kafka.start()
    postgres.start()
    kafkaAdmin = newAdmin()
  }

  override def afterAll(): Unit = {
    if (kafkaAdmin != null) {
      kafkaAdmin.close()
    }
    kafka.stop()
    postgres.stop()
    network.close()
    super.afterAll()
  }
}
