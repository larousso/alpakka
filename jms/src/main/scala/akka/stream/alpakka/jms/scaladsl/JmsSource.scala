package akka.stream.alpakka.jms.scaladsl

import javax.jms.ConnectionFactory

import akka.NotUsed
import akka.stream.alpakka.jms.{JmsSourceSettings, JmsSourceStage}
import akka.stream.scaladsl.Source

object JmsSource {

  /**
    * Scala API: Creates an [[JmsSource]]
    * @param jmsSettings the connection settings
    * @return
    */
  def apply(jmsSettings: JmsSourceSettings): Source[String, NotUsed] =
    Source.fromGraph(new JmsSourceStage(jmsSettings))

}
