package akka.stream.alpakka.jms.javadsl;

import static akka.pattern.PatternsCS.after;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

import javax.jms.JMSException;

import akka.NotUsed;
import akka.stream.alpakka.jms.JmsSinkSettings;
import akka.stream.alpakka.jms.JmsSourceSettings;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.junit.*;

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.JavaTestKit;
import scala.concurrent.duration.FiniteDuration;

public class JmsConnectorsTest {

    static ActorSystem system;
    static Materializer materializer;

    BrokerService broker;

    @BeforeClass
    public static void setup() throws Exception {
        system = ActorSystem.create();
        materializer = ActorMaterializer.create(system);
    }

    @AfterClass
    public static void teardown() throws Exception {
        JavaTestKit.shutdownActorSystem(system);
    }

    @Before
    public void startServer() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setBrokerName("localhost");
        broker.setUseJmx(false);
        broker.addConnector("tcp://localhost:61617");
        broker.start();
    }

    @After
    public void stopServer() throws Exception {
        broker.stop();
    }

    @Test
    public void publishAndConsume() throws ExecutionException, InterruptedException, TimeoutException {
        //#connection-factory
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61617");
        //#connection-factory

        //#create-sink
        Sink<String, NotUsed> jmsSink = JmsSink.create(
            JmsSinkSettings
                    .create(connectionFactory)
                    .withQueue("test")
        );
        //#create-sink

        //#run-sink
        List<String> in = Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k");
        Source.from(in).runWith(jmsSink, materializer);
        //#run-sink

        //#create-source
        Source<String, NotUsed> jmsSource = JmsSource
                .textSource(JmsSourceSettings
                        .create(connectionFactory)
                        .withQueue("test")
                        .withBufferSize(10)
                );
        //#create-source

        //#run-source
        CompletionStage<List<String>> result = jmsSource
                .take(in.size())
                .runWith(Sink.seq(), materializer);
        //#run-source

        assertEquals(in, result.toCompletableFuture().get(3, TimeUnit.SECONDS));
    }

    @Test
    public void applyingBackpressure() throws ExecutionException, InterruptedException, TimeoutException {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61617");
        List<String> in = Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k");
        Source.from(in).runWith(JmsSink.create(JmsSinkSettings
                .create(connectionFactory)
                .withQueue("test")
        ), materializer);

        CompletionStage<List<String>> result = JmsSource
                .textSource(JmsSourceSettings
                        .create(connectionFactory)
                        .withQueue("test")
                        .withBufferSize(1)
                )
                .mapAsync(1, e ->
                        after(FiniteDuration.create(1, TimeUnit.SECONDS), system.scheduler(), system.dispatcher(), CompletableFuture.completedFuture(e))
                )
                .take(in.size()).runWith(Sink.seq(), materializer);

        assertEquals(in, result.toCompletableFuture().get(15, TimeUnit.SECONDS));
    }

    @Test
    public void deconnexionShouldFail() throws Exception {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61617");

        CompletionStage<List<String>> result = JmsSource.textSource(JmsSourceSettings
                .create(connectionFactory)
                .withQueue("test")
                .withBufferSize(1)
        ).runWith(Sink.seq(), materializer);
        broker.stop();
        try {
            result.toCompletableFuture().get();
            fail();
        } catch (ExecutionException e) {
            assertEquals(JMSException.class, e.getCause().getClass());
        }
    }
}
