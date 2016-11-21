package akka.stream.alpakka.jms

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.apache.activemq.broker.BrokerService
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}
import org.scalatest.concurrent.ScalaFutures

abstract class JmsSpec extends WordSpec with Matchers with BeforeAndAfterAll with ScalaFutures {

  implicit val system = ActorSystem(this.getClass.getSimpleName)
  implicit val materializer = ActorMaterializer()
  val broker = new BrokerService()

  override protected def beforeAll(): Unit = {
    broker.setPersistent(false)
    broker.setBrokerName("localhost")
    broker.setUseJmx(false)
    broker.addConnector("tcp://localhost:61616")
    broker.start()
  }

  override protected def afterAll(): Unit ={
    broker.stop()
    system.terminate()
  }

}
