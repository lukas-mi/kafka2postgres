package com.mikelionis.lukas.kafka2postgres.util

import com.messages.events.user._
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader

import java.nio.ByteBuffer
import scala.jdk.CollectionConverters._

trait EventGenHelper {
  val schemaHeaderName = "name"

  def newUserCreatedRecord(topic: String, id: String, name: String, email: String): ProducerRecord[String, ByteBuffer] = {
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
      List[Header](new RecordHeader(schemaHeaderName, classOf[UserCreated].getSimpleName.getBytes)).asJava
    )
  }

  def newUserEmailUpdatedRecord(topic: String, id: String, email: String): ProducerRecord[String, ByteBuffer] = {
    new ProducerRecord[String, ByteBuffer](
      topic,
      null,
      id,
      UserEmailUpdated.getEncoder.encode(
        UserEmailUpdated
          .newBuilder()
          .setId(id)
          .setEmail(email)
          .build()
      ),
      List[Header](new RecordHeader(schemaHeaderName, classOf[UserEmailUpdated].getSimpleName.getBytes)).asJava
    )
  }

  def newUserDeletedRecord(topic: String, id: String): ProducerRecord[String, ByteBuffer] = {
    new ProducerRecord[String, ByteBuffer](
      topic,
      null,
      id,
      UserDeleted.getEncoder.encode(
        UserDeleted
          .newBuilder()
          .setId(id)
          .build()
      ),
      List[Header](new RecordHeader(schemaHeaderName, classOf[UserDeleted].getSimpleName.getBytes)).asJava
    )
  }

  def newUserForgottenRecord(topic: String, id: String): ProducerRecord[String, ByteBuffer] = {
    new ProducerRecord[String, ByteBuffer](
      topic,
      null,
      id,
      UserForgotten.getEncoder.encode(
        UserForgotten
          .newBuilder()
          .setId(id)
          .build()
      ),
      List[Header](new RecordHeader(schemaHeaderName, classOf[UserForgotten].getSimpleName.getBytes)).asJava
    )
  }

  def newUnsupportedEvent(topic: String, schema: String, key: String, content: Array[Byte]): ProducerRecord[String, ByteBuffer] = {
    new ProducerRecord[String, ByteBuffer](
      topic,
      null,
      key,
      ByteBuffer.wrap(content),
      List[Header](new RecordHeader(schemaHeaderName, schema.getBytes)).asJava
    )
  }
}
