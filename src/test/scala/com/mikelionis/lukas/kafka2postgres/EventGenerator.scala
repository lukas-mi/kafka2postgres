package com.mikelionis.lukas.kafka2postgres

import com.messages.events.user.UserCreated
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader

import java.nio.ByteBuffer
import java.util
import scala.jdk.CollectionConverters._

class EventGenerator(topic: String) {

  def newUserCreatedRecord(id: String, name: String, email: String): ProducerRecord[String, ByteBuffer] = {
    new ProducerRecord[String, ByteBuffer](
      topic,
      null,
      id,
      UserCreated.getEncoder.encode(
        UserCreated
          .newBuilder()
          .setId(id)
          .setName(name)
          .setEmail(email)
          .build()
      ),
      List[Header](new RecordHeader("name", classOf[UserCreated].getSimpleName.getBytes)).asJava
    )
  }


}
