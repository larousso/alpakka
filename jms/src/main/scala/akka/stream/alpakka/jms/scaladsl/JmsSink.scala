package akka.stream.alpakka.jms.scaladsl

import akka.NotUsed
import akka.stream.alpakka.jms.{JmsSinkSettings, JmsSinkStage}
import akka.stream.scaladsl.Sink

object JmsSink {

  /**
    * Scala API: Creates an [[JmsSink]]
    * @param jmsSettings the connexion settings
    * @return
    */
  def apply(jmsSettings: JmsSinkSettings): Sink[String, NotUsed] =
    Sink.fromGraph(new JmsSinkStage(jmsSettings))

}
