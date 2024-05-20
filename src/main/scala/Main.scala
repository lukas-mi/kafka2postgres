import com.mikelionis.lukas.kafka2postgres.Connector
import com.typesafe.config.ConfigFactory

object Main {
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load()
    val connector = new Connector(config)

    connector.run()

    Runtime
      .getRuntime
      .addShutdownHook(new Thread(() => connector.stop(), "shutdown-hook-thread"))
  }
}
