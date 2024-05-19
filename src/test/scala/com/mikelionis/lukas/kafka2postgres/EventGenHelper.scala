package com.mikelionis.lukas.kafka2postgres

import com.messages.events.user.UserCreated
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader

import java.nio.ByteBuffer
import scala.jdk.CollectionConverters._

trait EventHelper {
  val srcSchemaHeaderName = "name"

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
      List[Header](new RecordHeader(srcSchemaHeaderName, classOf[UserCreated].getSimpleName.getBytes)).asJava
    )
  }


}
