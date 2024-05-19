package com.mikelionis.lukas.kafka2postgres

import com.messages.events.user._
import org.apache.avro.message.BinaryMessageDecoder

object Decoders {
  val userCreated: BinaryMessageDecoder[UserCreated] = UserCreated.getDecoder
  val userDeleted: BinaryMessageDecoder[UserDeleted] = UserDeleted.getDecoder
  val userEmailUpdated: BinaryMessageDecoder[UserEmailUpdated] = UserEmailUpdated.getDecoder
  val userForgotten: BinaryMessageDecoder[UserForgotten] = UserForgotten.getDecoder
}
