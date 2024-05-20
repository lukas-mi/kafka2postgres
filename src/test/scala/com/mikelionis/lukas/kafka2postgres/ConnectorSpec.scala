package com.mikelionis.lukas.kafka2postgres

class ConnectorSpec extends ConnectorSpecWrapper {
  classOf[Connector].getSimpleName should "consume user events from Kafka and update user record correctly in DB" in {
    val srcTopic = "user-events-test"
    val usersTable = "users_test"
    val forgottenUsersTable = "forgotten_test"

    createTopic(srcTopic)
    createPostgresTables(usersTable, forgottenUsersTable)

    withConnector(srcTopic, usersTable, forgottenUsersTable) {
      val userId = "user1"
      val userName = "username1"
      val userEmail = "user1@mail.com"
      val beforeCreatedMillis = System.currentTimeMillis()
      kafkaProducer.send(newUserCreatedRecord(srcTopic, userId, userName, userEmail))

      // check that user is created
      eventually {
        val users = selectUsers(usersTable)

        users.size shouldBe 1
        assertUser(users.head, beforeCreatedMillis, checkUpdatedAt = false, userId, userName, userEmail, UserStatus.Active)

        selectForgottenUsers(forgottenUsersTable).size shouldBe 0
      }

      val updatedUserEmail = "user1.updated@mail.com"
      kafkaProducer.send(newUserEmailUpdatedRecord(srcTopic, userId, updatedUserEmail))

      // check that user email is updated
      eventually {
        val users = selectUsers(usersTable)

        users.size shouldBe 1
        assertUser(users.head, beforeCreatedMillis, checkUpdatedAt = true, userId, userName, updatedUserEmail, UserStatus.Active)

        selectForgottenUsers(forgottenUsersTable).size shouldBe 0
      }

      kafkaProducer.send(newUserDeletedRecord(srcTopic, userId))

      // check that user status is DELETED
      eventually {
        val users = selectUsers(usersTable)

        users.size shouldBe 1
        assertUser(users.head, beforeCreatedMillis, checkUpdatedAt = true, userId, userName, updatedUserEmail, UserStatus.Deleted)

        selectForgottenUsers(forgottenUsersTable).size shouldBe 0
      }

      val beforeForgottenMillis = System.currentTimeMillis()
      kafkaProducer.send(newUserForgottenRecord(srcTopic, userId))

      // check that user status is DELETED
      eventually {
        selectUsers(usersTable).size shouldBe 0

        val forgottenUsers = selectForgottenUsers(forgottenUsersTable)
        forgottenUsers.size shouldBe 1

        assertForgottenUser(forgottenUsers.head, beforeForgottenMillis, userId)
      }
    }
  }
}
