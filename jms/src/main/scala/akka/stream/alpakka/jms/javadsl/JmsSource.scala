package akka.stream.alpakka.jms.javadsl

import akka.NotUsed
import akka.stream.alpakka.jms.{JmsSourceSettings, JmsSourceStage}

object JmsSource {

  /**
    * Java API: Creates an [[JmsSource]]
    *
    * @param jmsSourceSettings the jms connexion settings
    * @return a [[akka.stream.javadsl.Source]]
    */
  def create(jmsSourceSettings: JmsSourceSettings): akka.stream.javadsl.Source[String, NotUsed] =
    akka.stream.javadsl.Source.fromGraph(new JmsSourceStage(jmsSourceSettings))

}
