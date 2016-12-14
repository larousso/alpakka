package akka.stream.alpakka.elasticsearch

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.marshalling.{Marshal, ToEntityMarshaller}
import akka.http.scaladsl.model.Uri.{Path, Query}
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.{FromResponseUnmarshaller, Unmarshal}
import akka.http.scaladsl.util.FastFuture
import akka.http.scaladsl.{Http, HttpExt}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source

import scala.concurrent.{ExecutionContext, Future}

private[elasticsearch] class ElasticsearchClient(val host: String, val port: Int, scheme: String = "http", val credentials: Option[Credentials], val system: ActorSystem) {

  implicit private val sys = system
  implicit private val mat = ActorMaterializer()

  private val http: HttpExt = Http(sys)

  private val baseUri = Uri().withScheme(scheme).withHost(host).withPort(port)


  private def request[A, B](path: Path, method: HttpMethod, body: Option[B] = None, query: Option[Query] = None)(implicit unmarshaller: FromResponseUnmarshaller[A], marshaller: ToEntityMarshaller[B], ec: ExecutionContext): Future[A] = {
    buildRequest(path, method, body, query)
      .flatMap { request =>
        http.singleRequest(request).flatMap {
          case HttpResponse(code, _, entity, _) if code == StatusCodes.OK || code == StatusCodes.Created =>
            Unmarshal(entity).to[A]
          case HttpResponse(code, _, entity, _) =>
            entity.dataBytes
              .map(_.utf8String)
              .runFold("")((str, acc) => str + acc)
              .flatMap(cause =>
                FastFuture.failed(EsException(code.intValue(), cause))
              )
        }
      }

  }

  private def buildRequest[B](path: Path, method: HttpMethod, body: Option[B] = None, query: Option[Query] = None)(implicit marshaller: ToEntityMarshaller[B]): Future[HttpRequest] = {
    val uri: Uri = query.fold(baseUri.withPath(path))(baseUri.withPath(path).withQuery)
    body.fold(FastFuture.successful[RequestEntity](HttpEntity.Empty))(b => Marshal(b).to[RequestEntity])
        .map(entity => HttpRequest(method, uri, entity = entity))

  }

  def scrollSearch[Json](index: Seq[String], `type`: Seq[String], query: String, scroll: String = "1m", size: Option[Int] = None)(implicit unmarshaller: FromResponseUnmarshaller[SearchResponse[Json]], ec: ExecutionContext): Source[SearchResponse[Json], NotUsed] = {
    val path = indexPath(index, `type`)
    val querys: Seq[(String, String)] = Seq(
      Some(scroll).map(_ => "scroll" -> scroll),
      size.map(s => "size" -> s.toString)
    ).flatten

    Source
      .fromFuture(request(path / "_search", HttpMethods.GET, Some(query), Some(Query(querys: _*))))
      .flatMapConcat { resp =>
        val Some(scroll_id) = resp.scroll_id
        Source.single(resp).merge(nextScroll(scroll_id, scroll)(unmarshaller, ec))
      }
  }

  private def nextScroll[Json](scroll_id: String, scroll: String)(implicit unmarshaller: FromResponseUnmarshaller[SearchResponse[Json]], ec: ExecutionContext): Source[SearchResponse[Json], NotUsed] = {
    val scrollRequest = Scroll(scroll, scroll_id)
    Source.fromFuture(request(Path.Empty / "_search" / "scroll", HttpMethods.POST, Some(scrollRequest)))
      .flatMapConcat { resp =>
        val single: Source[SearchResponse[Json], NotUsed] = Source.single(resp)
        if (resp.hits.hits.isEmpty) {
          single
        } else {
          single.merge(nextScroll(scroll_id, scroll))
        }
      }
  }

  private def indexPath(names: Seq[String], types: Seq[String]) = names match {
    case Nil | Seq() => Path.Empty / "*" ++ toPath(types)
    case _ => Path.Empty / names.mkString(",") ++ toPath(types)
  }

  private def toPath(values: Seq[String]) = values match {
    case Nil | Seq() => Path.Empty
    case _ => Path.Empty / values.mkString(",")
  }

}

private final case class Scroll(scroll: String, scroll_id: String)

sealed trait ESResponse

final case class Shards[Json](total: Int, failed: Int, successful: Int, failures: Seq[Json]) extends ESResponse

case class Hit[Json](_index: String, _type: String, _id: String, _score: Float, _source: Json) extends ESResponse

case class Hits[Json](total: Int, max_score: Option[Float], hits: Seq[Hit[Json]]) extends ESResponse

final case class SearchResponse[Json](took: Int, _shards: Shards[Json], timed_out: Boolean, hits: Hits[Json], scroll_id: Option[String], aggregations: Option[Json] = None) extends ESResponse

final case class Credentials(login: String, password: String)

final case class EsException(code: Int, cause: String) extends Throwable