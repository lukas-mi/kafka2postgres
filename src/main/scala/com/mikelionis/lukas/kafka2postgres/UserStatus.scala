package com.mikelionis.lukas.kafka2postgres

sealed trait UserStatus

object UserStatus {
  case object Active extends UserStatus
  case object Deleted extends UserStatus

  def apply(status: String): Option[UserStatus] =
    status match {
      case "ACTIVE" => Some(Active)
      case "DELETED" => Some(Deleted)
      case _ => None
    }

  def toString(status: UserStatus): String = status.toString.toUpperCase
}
