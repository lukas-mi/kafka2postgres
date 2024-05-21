package com.mikelionis.lukas.kafka2postgres

class ConnectorSpec extends ConnectorSpecWrapper {
  classOf[Connector].getSimpleName should "aggregate all event types for users correctly" in {
    withConnector {
      val (userId1, userName1, userEmail1) = ("user1", "username1", "user1@mail.com")
      val (userId2, userName2, userEmail2) = ("user2", "username2", "user2@mail.com")
      val beforeCreatedMillis = System.currentTimeMillis()
      kafkaProducer.send(newUserCreatedRecord(srcTopic, userId1, userName1, userEmail1))
      kafkaProducer.send(newUserCreatedRecord(srcTopic, userId2, userName2, userEmail2))

      // check that user is created
      eventually {
        val users = selectUsers(usersTable)

        users.size shouldBe 2
        val user1 = users.find(_.id == userId1).get
        val user2 = users.find(_.id == userId2).get
        assertUser(user1, beforeCreatedMillis, checkUpdatedAt = false, userId1, userName1, userEmail1, UserStatus.Active)
        assertUser(user2, beforeCreatedMillis, checkUpdatedAt = false, userId2, userName2, userEmail2, UserStatus.Active)

        selectForgottenUsers(forgottenUsersTable).size shouldBe 0
      }

      val updatedUserEmail1 = "user1.updated@mail.com"
      val updatedUserEmail2 = "user2.updated@mail.com"
      kafkaProducer.send(newUserEmailUpdatedRecord(srcTopic, userId1, updatedUserEmail1))
      kafkaProducer.send(newUserEmailUpdatedRecord(srcTopic, userId2, updatedUserEmail2))

      // check that user email is updated
      eventually {
        val users = selectUsers(usersTable)

        users.size shouldBe 2
        val user1 = users.find(_.id == userId1).get
        val user2 = users.find(_.id == userId2).get
        assertUser(user1, beforeCreatedMillis, checkUpdatedAt = true, userId1, userName1, updatedUserEmail1, UserStatus.Active)
        assertUser(user2, beforeCreatedMillis, checkUpdatedAt = true, userId2, userName2, updatedUserEmail2, UserStatus.Active)

        selectForgottenUsers(forgottenUsersTable).size shouldBe 0
      }

      kafkaProducer.send(newUserDeletedRecord(srcTopic, userId1))
      kafkaProducer.send(newUserDeletedRecord(srcTopic, userId2))

      // check that user status is DELETED
      eventually {
        val users = selectUsers(usersTable)

        users.size shouldBe 2
        val user1 = users.find(_.id == userId1).get
        val user2 = users.find(_.id == userId2).get
        assertUser(user1, beforeCreatedMillis, checkUpdatedAt = true, userId1, userName1, updatedUserEmail1, UserStatus.Deleted)
        assertUser(user2, beforeCreatedMillis, checkUpdatedAt = true, userId2, userName2, updatedUserEmail2, UserStatus.Deleted)

        selectForgottenUsers(forgottenUsersTable).size shouldBe 0
      }

      val beforeForgottenMillis = System.currentTimeMillis()
      kafkaProducer.send(newUserForgottenRecord(srcTopic, userId1))
      kafkaProducer.send(newUserForgottenRecord(srcTopic, userId2))

      // check that user status is DELETED
      eventually {
        selectUsers(usersTable).size shouldBe 0

        val forgottenUsers = selectForgottenUsers(forgottenUsersTable)
        forgottenUsers.size shouldBe 2
        val user1 = forgottenUsers.find(_.id == userId1).get
        val user2 = forgottenUsers.find(_.id == userId2).get

        assertForgottenUser(user1, beforeForgottenMillis, userId1)
        assertForgottenUser(user2, beforeForgottenMillis, userId2)
      }
    }
  }

  // TODO: is this expected behavior?
  it should "forget non-existing users" in {
    val (userId1, userName1, userEmail1) = ("user1", "username1", "user1@mail.com")
    val (userId2, userName2, userEmail2) = ("user2", "username2", "user2@mail.com")
    val userIdX = "userX"
    val beforeCreatedMillis = System.currentTimeMillis()

    withConnector {
      kafkaProducer.send(newUserCreatedRecord(srcTopic, userId1, userName1, userEmail1))
      kafkaProducer.send(newUserForgottenRecord(srcTopic, userIdX)) // non-existing id
      kafkaProducer.send(newUserCreatedRecord(srcTopic, userId2, userName2, userEmail2))

      eventually {
        val users = selectUsers(usersTable)

        users.size shouldBe 2
        val user1 = users.find(_.id == userId1).get
        val user2 = users.find(_.id == userId2).get
        assertUser(user1, beforeCreatedMillis, checkUpdatedAt = false, userId1, userName1, userEmail1, UserStatus.Active)
        assertUser(user2, beforeCreatedMillis, checkUpdatedAt = false, userId2, userName2, userEmail2, UserStatus.Active)

        val forgottenUsers = selectForgottenUsers(forgottenUsersTable)
        forgottenUsers.size shouldBe 1
        val userX = forgottenUsers.find(_.id == userIdX).get

        assertForgottenUser(userX, beforeCreatedMillis, userIdX)
      }
    }
  }

  it should "not create users with existing ids or emails" in {
    val (userId1, userName1, userEmail1) = ("user1", "username1", "user1@mail.com")
    val (userId2, userName2, userEmail2) = (userId1, "username2", "user2@mail.com") // duplicate id
    val (userId3, userName3, userEmail3) = ("user3", "username3", userEmail1) // duplicate email
    val (userId4, userName4, userEmail4) = ("user4", "username4", "user4@mail.com")
    val beforeCreatedMillis = System.currentTimeMillis()

    withConnector {
      kafkaProducer.send(newUserCreatedRecord(srcTopic, userId1, userName1, userEmail1))
      kafkaProducer.send(newUserCreatedRecord(srcTopic, userId2, userName2, userEmail2))
      kafkaProducer.send(newUserCreatedRecord(srcTopic, userId3, userName3, userEmail3))
      kafkaProducer.send(newUserCreatedRecord(srcTopic, userId4, userName4, userEmail4))

      eventually {
        val users = selectUsers(usersTable)

        users.size shouldBe 2
        val user1 = users.find(_.id == userId1).get
        val user4 = users.find(_.id == userId4).get
        assertUser(user1, beforeCreatedMillis, checkUpdatedAt = false, userId1, userName1, userEmail1, UserStatus.Active)
        assertUser(user4, beforeCreatedMillis, checkUpdatedAt = false, userId4, userName4, userEmail4, UserStatus.Active)

        selectForgottenUsers(forgottenUsersTable).size shouldBe 0
      }
    }
  }

  it should "not update user email if such email already exists" in {
    val (userId1, userName1, userEmail1) = ("user1", "username1", "user1@mail.com")
    val (userId2, userName2, userEmail2) = ("user2", "username2", "user2@mail.com") // duplicate email
    val (userId3, userName3, userEmail3) = ("user3", "username3", "user3@mail.com")
    val userEmailUpdated3 = "user3.updated@mail.com"
    val beforeCreatedMillis = System.currentTimeMillis()

    withConnector {
      kafkaProducer.send(newUserCreatedRecord(srcTopic, userId1, userName1, userEmail1))
      kafkaProducer.send(newUserCreatedRecord(srcTopic, userId2, userName2, userEmail2))
      kafkaProducer.send(newUserCreatedRecord(srcTopic, userId3, userName3, userEmail3))

      kafkaProducer.send(newUserEmailUpdatedRecord(srcTopic, userId2, userEmail1)) // invalid update
      kafkaProducer.send(newUserEmailUpdatedRecord(srcTopic, userId3, userEmailUpdated3)) // valid update

      eventually {
        val users = selectUsers(usersTable)

        users.size shouldBe 3
        val user1 = users.find(_.id == userId1).get
        val user2 = users.find(_.id == userId2).get
        val user3 = users.find(_.id == userId3).get
        assertUser(user1, beforeCreatedMillis, checkUpdatedAt = false, userId1, userName1, userEmail1, UserStatus.Active)
        assertUser(user2, beforeCreatedMillis, checkUpdatedAt = false, userId2, userName2, userEmail2, UserStatus.Active)
        assertUser(user3, beforeCreatedMillis, checkUpdatedAt = true, userId3, userName3, userEmailUpdated3, UserStatus.Active)

        selectForgottenUsers(forgottenUsersTable).size shouldBe 0
      }
    }
  }

  it should "ignore delete/update events for non-existing users" in {
    val (userId1, userName1, userEmail1) = ("user1", "username1", "user1@mail.com")
    val (userId2, userName2, userEmail2) = ("user2", "username2", "user2@mail.com")
    val beforeCreatedMillis = System.currentTimeMillis()

    withConnector {
      kafkaProducer.send(newUserCreatedRecord(srcTopic, userId1, userName1, userEmail1))
      kafkaProducer.send(newUserEmailUpdatedRecord(srcTopic, "userX", "userX@email.com")) // non-existing id
      kafkaProducer.send(newUserDeletedRecord(srcTopic, "userX")) // non-existing id
      kafkaProducer.send(newUserCreatedRecord(srcTopic, userId2, userName2, userEmail2))

      eventually {
        val users = selectUsers(usersTable)

        users.size shouldBe 2
        val user1 = users.find(_.id == userId1).get
        val user2 = users.find(_.id == userId2).get
        assertUser(user1, beforeCreatedMillis, checkUpdatedAt = false, userId1, userName1, userEmail1, UserStatus.Active)
        assertUser(user2, beforeCreatedMillis, checkUpdatedAt = false, userId2, userName2, userEmail2, UserStatus.Active)

        selectForgottenUsers(forgottenUsersTable).size shouldBe 0
      }
    }
  }

  it should "ignore events with unsupported schema" in {
    val (userId1, userName1, userEmail1) = ("user1", "username1", "user1@mail.com")
    val (userId2, userName2, userEmail2) = ("user2", "username2", "user2@mail.com")
    val beforeCreatedMillis = System.currentTimeMillis()

    withConnector {
      kafkaProducer.send(newUserCreatedRecord(srcTopic, userId1, userName1, userEmail1))
      kafkaProducer.send(newUnsupportedEvent(srcTopic, "TestSchema", "test-key", "test-value".getBytes)) // unsupported
      kafkaProducer.send(newUserCreatedRecord(srcTopic, userId2, userName2, userEmail2))

      eventually {
        val users = selectUsers(usersTable)

        users.size shouldBe 2
        val user1 = users.find(_.id == userId1).get
        val user2 = users.find(_.id == userId2).get
        assertUser(user1, beforeCreatedMillis, checkUpdatedAt = false, userId1, userName1, userEmail1, UserStatus.Active)
        assertUser(user2, beforeCreatedMillis, checkUpdatedAt = false, userId2, userName2, userEmail2, UserStatus.Active)

        selectForgottenUsers(forgottenUsersTable).size shouldBe 0
      }
    }
  }

  // TODO: add tests for unparsable events
}
