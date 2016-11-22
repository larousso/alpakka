package akka.stream.alpakka.jms

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.apache.activemq.broker.BrokerService
import org.scalactic.source.Position
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

abstract class JmsSpec extends WordSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach with ScalaFutures {

  implicit val system = ActorSystem(this.getClass.getSimpleName)
  implicit val materializer = ActorMaterializer()
  val broker = new BrokerService()



  override protected def beforeEach(): Unit = {
    broker.setPersistent(false)
    broker.setBrokerName("localhost")
    broker.setUseJmx(false)
    broker.addConnector("tcp://localhost:61616")
    broker.start()
  }

  override protected def afterEach(): Unit = {
    if(broker.isStarted) {
      broker.stop()
    }
  }

  override protected def afterAll(): Unit ={
    system.terminate()
  }

}
