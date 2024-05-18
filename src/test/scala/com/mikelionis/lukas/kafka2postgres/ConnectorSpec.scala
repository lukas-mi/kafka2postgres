package com.mikelionis.lukas.kafka2postgres

import com.messages.events.user.UserCreated
import org.scalatest.matchers.should

import java.time.Duration
import scala.jdk.CollectionConverters._
import scala.util.Using

class ConnectorSpec extends SpecWrapper with should.Matchers {
  it should "run the test" in {
    val topic = "test-topic"
    val gen = new EventGenerator(topic)

    createTopic(topic)

    val userId = "user1"
    val userName = "username1"
    val userEmail = "user1@mail.com"
    val producerRecord = gen.newUserCreatedRecord(userId, userName, userEmail)

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


    // TODO: create topic
    // TODO: write/read from topic
    // TODO: query to postgres


    Some(1) shouldBe Some(1)
  }
}
