package com.mikelionis.lukas.kafka2postgres

sealed trait UserEventSchema

object UserEventSchema {
  case object UserCreated extends UserEventSchema
  case object UserDeleted extends UserEventSchema
  case object UserEmailUpdated extends UserEventSchema
  case object UserForgotten extends UserEventSchema

  def apply(schema: String): Option[UserEventSchema] =
    schema match {
      case "UserCreated" => Some(UserCreated)
      case "UserDeleted" => Some(UserDeleted)
      case "UserEmailUpdated" => Some(UserEmailUpdated)
      case "UserForgotten" => Some(UserForgotten)
      case _ => None
    }
}
