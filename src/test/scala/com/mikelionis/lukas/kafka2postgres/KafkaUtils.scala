package com.mikelionis.lukas.kafka2postgres

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.{ByteBufferDeserializer, ByteBufferSerializer, StringDeserializer, StringSerializer}
import org.testcontainers.containers.KafkaContainer

import java.nio.ByteBuffer
import java.util.Properties
import scala.jdk.CollectionConverters._

trait KafkaUtils {
  protected val kafka: KafkaContainer
  protected var kafkaAdmin: AdminClient

  def createTopic(topic: String): Unit = {
    kafkaAdmin.createTopics(List(new NewTopic(topic, 1, 1.asInstanceOf[Short])).asJava)
  }

  def newAdmin(): AdminClient = {
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers)
    AdminClient.create(props)
  }

  def newConsumer(): KafkaConsumer[String, ByteBuffer] = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteBufferDeserializer].getName)
    new KafkaConsumer[String, ByteBuffer](props)
  }

  def newProducer(): KafkaProducer[String, ByteBuffer] = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteBufferSerializer].getName)
    new KafkaProducer[String, ByteBuffer](props)
  }

}
