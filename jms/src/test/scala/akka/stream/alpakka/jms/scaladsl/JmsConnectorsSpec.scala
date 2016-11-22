package akka.stream.alpakka.jms.scaladsl

import javax.jms.JMSException

import akka.NotUsed
import akka.stream.alpakka.jms.{JmsSinkSettings, JmsSourceSettings, JmsSpec}
import akka.stream.scaladsl.{Sink, Source}
import org.apache.activemq.ActiveMQConnectionFactory

import scala.concurrent.Future
import scala.concurrent.duration._

class JmsConnectorsSpec extends JmsSpec {

  override implicit val patienceConfig = PatienceConfig(1.minute)

  "The JMS Connectors" should {
    "publish and consume elements through a queue" in {

      //#connection-factory
      val connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616")
      //#connection-factory

      //#create-sink
      val jmsSink: Sink[String, NotUsed] = JmsSink(
        JmsSinkSettings(connectionFactory).withQueue("test")
      )
      //#create-sink

      //#run-sink
      val in = List("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k")
      Source(in).runWith(jmsSink)
      //#run-sink

      //#create-source
      val jmsSource: Source[String, NotUsed] = JmsSource.textSource(
        JmsSourceSettings(connectionFactory, bufferSize = 10).withQueue("test")
      )
      //#create-source

      //#run-source
      val result = jmsSource.take(in.size).runWith(Sink.seq)
      //#run-source

      result.futureValue shouldEqual in
    }

    "applying backpressure when the consumer is slower than the producer" in {

      import system.dispatcher

      val connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616")
      val in = List("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k")
      Source(in).runWith(JmsSink(JmsSinkSettings(connectionFactory).withQueue("test")))

      val result = JmsSource.textSource(JmsSourceSettings(connectionFactory, bufferSize = 1).withQueue("test")).mapAsync(1)(e => akka.pattern.after(1.seconds, system.scheduler)(Future.successful(e)) ).take(in.size).runWith(Sink.seq)

      result.futureValue shouldEqual in
    }

    "deconnection should fail the stage" in {
      val connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616")
      val result = JmsSource(JmsSourceSettings(connectionFactory).withQueue("test")).runWith(Sink.seq)
      broker.stop()
      result.failed.futureValue shouldBe an[JMSException]
    }

  }

}
