package com.mikelionis.lukas.kafka2postgres

import com.messages.events.user.UserCreated
import org.scalatest.matchers.should

import java.time.Duration
import scala.jdk.CollectionConverters._
import scala.util.Using

class ConnectorSpec extends ConnectorSpecWrapper with should.Matchers {
  classOf[Connector].getSimpleName should "insert a user data after consuming UserCreated event" in {
    val srcTopic = "user-events-test"
    val trgTable = "users_test"

    createTopic(srcTopic)
    createUsersTable(trgTable)
    Thread.sleep(500) // ensure table exists

    val connector = newConnector(srcTopic, trgTable)

    new Thread(
      () => connector.run(),
      "connector-starter-in-spec"
    ).start()

    val userId = "user1"
    val userName = "username1"
    val userEmail = "user1@mail.com"
    Using.resource(newProducer()) { producer =>
      producer.send(newUserCreatedRecord(srcTopic, userId, userName, userEmail))
    }

    eventually {
      val users = selectUsers(trgTable)

      users.size shouldBe 1
    }

    connector.stop()
  }

  it should "run the test" in {
    val topic = "test-topic"

    createTopic(topic)

    val userId = "user1"
    val userName = "username1"
    val userEmail = "user1@mail.com"
    val producerRecord = newUserCreatedRecord(topic, userId, userName, userEmail)

    Using.resources(newProducer(), newConsumer()) { (producer, consumer) =>
      consumer.subscribe(List(topic).asJava)
      producer.send(producerRecord)


      eventually {
        println("polling")
        val consumedRecords = consumer.poll(Duration.ofMillis(100)).records(topic).asScala.toList
        consumedRecords.size shouldBe 1

        println(consumedRecords.head.key())
        println(UserCreated.getDecoder.decode(consumedRecords.head.value()))
        val headers = consumedRecords.head.headers().headers("name").iterator().asScala.toList
        headers.foreach(header => println(s"${header.key()}=${new String(header.value())}"))
      }
    }

    Some(1) shouldBe Some(1)
  }
}
