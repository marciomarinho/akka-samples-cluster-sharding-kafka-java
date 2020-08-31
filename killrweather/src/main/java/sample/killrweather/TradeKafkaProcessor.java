package sample.killrweather;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.PostStop;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Adapter;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.cluster.sharding.external.ExternalShardAllocationStrategy;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.Entity;
import akka.cluster.sharding.typed.javadsl.EntityRef;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import akka.kafka.AutoSubscription;
import akka.kafka.ConsumerRebalanceEvent;
import akka.kafka.ConsumerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.cluster.sharding.KafkaClusterSharding;
import akka.kafka.javadsl.Consumer;
import akka.stream.javadsl.Sink;
import akka.util.Timeout;
import com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * A sharded `WeatherStation` has a set of recorded datapoints
 * For each weather station common cumulative computations can be run:
 * aggregate, averages, high/low, topK (e.g. the top N highest temperatures).
 * <p>
 * Note that since this station is not storing its state anywhere else than in JVM memory, if Akka Cluster Sharding
 * rebalances it - moves it to another node because of cluster nodes added removed etc - it will lose all its state.
 * For a sharded entity to have state that survives being stopped and started again it needs to be persistent,
 * for example by being an EventSourcedBehavior.
 */
final class TradeKafkaProcessor extends AbstractBehavior<TradeKafkaProcessor.Command> {

    // setup for using WeatherStations through Akka Cluster Sharding
    // these could also live elsewhere and the WeatherStation class be completely
    // oblivious to being used in sharding

    public static final String groupId = "register-trade-topic-group-id";

    public static final EntityTypeKey<TradeKafkaProcessor.Command> TypeKey =
            EntityTypeKey.create(TradeKafkaProcessor.Command.class, groupId); //"WeatherStation");

    public static final String REGISTER_TRADE_TOPIC = "register-trade-topic";

    public static void initSharding(ActorSystem<?> system) {

        CompletionStage<KafkaClusterSharding.KafkaShardingNoEnvelopeExtractor<TradeKafkaProcessor.Command>> messageExtractor =
                KafkaClusterSharding.get(system)
                        .messageExtractorNoEnvelope(
                                REGISTER_TRADE_TOPIC,
                                Duration.ofSeconds(10),
                                (TradeKafkaProcessor.Command msg) -> msg.getId(),
                                ConsumerSettings.create(
                                        Adapter.toClassic(system), new StringDeserializer(), new StringDeserializer())
                                        .withBootstrapServers("localhost:9092")
                                        .withGroupId(
                                                TypeKey
                                                        .name()));

        messageExtractor.thenAccept(
                extractor ->
                        ClusterSharding.get(system)
                                .init(
                                        Entity.of(TypeKey, ctx -> create(ctx.getEntityId()))
                                                .withAllocationStrategy(
                                                        new ExternalShardAllocationStrategy(
                                                                system, TypeKey.name(), Timeout.create(Duration.ofSeconds(5))))
                                                .withMessageExtractor(extractor)));

        akka.actor.typed.ActorRef<ConsumerRebalanceEvent> rebalanceListener =
                KafkaClusterSharding.get(system).rebalanceListener(TypeKey);

        ConsumerSettings<String, byte[]> consumerSettings =
                ConsumerSettings.create(
                        Adapter.toClassic(system), new StringDeserializer(), new ByteArrayDeserializer())
                        .withBootstrapServers("localhost:9092")
                        .withGroupId(
                                TypeKey
                                        .name()); // use the same group id as we used in the `EntityTypeKey` for `User`

        // pass the rebalance listener to the topic subscription
        AutoSubscription subscription =
                Subscriptions.topics(REGISTER_TRADE_TOPIC)
                        .withRebalanceListener(Adapter.toClassic(rebalanceListener));

        Consumer.plainSource(consumerSettings, subscription)
//                .via(userBusiness(system, 1)) // put your business logic, or omit to just try starting the stream
                .map(e -> {
                            String key = e.key();
                            String value = new String(e.value());
                            System.out.println(key);
                            System.out.println(value);

                            userBusiness(system, new Data(key, value));

                            return value;
                        }
                )
                .runWith(Sink.ignore(), system);

    }

    private static CompletionStage<TradeKafkaProcessor.DataRecorded> userBusiness(ActorSystem<?> system, TradeKafkaProcessor.Data data) {
        ClusterSharding sharding = ClusterSharding.get(system);
        EntityRef<Command> ref = sharding.entityRefFor(TradeKafkaProcessor.TypeKey, data.id);
        return ref.ask(replyTo -> new TradeKafkaProcessor.Record(data, System.currentTimeMillis(), replyTo), Duration.ofSeconds(10));
    }

    // actor commands and responses
    interface Command extends CborSerializable {
        public String getId();
    }

    public static final class Record implements Command {
        public final Data data;
        public final long processingTimestamp;
        public final ActorRef<DataRecorded> replyTo;


        public Record(Data data, long processingTimestamp, ActorRef<DataRecorded> replyTo) {
            this.data = data;
            this.processingTimestamp = processingTimestamp;
            this.replyTo = replyTo;
        }

        @Override
        public String getId() {
            return data.id;
        }
    }

    public static final class DataRecorded implements CborSerializable {

        public final String id;

        @JsonCreator
        public DataRecorded(final String id) {
            this.id = id;
        }

        @Override
        public String toString() {
            return "DataRecorded{" +
                    "wsid='" + id + '\'' +
                    '}';
        }

    }

    public static class Data {

        public final String id;
        public final String value;

        @JsonCreator
        public Data(final String id, final String value) {
            this.id = id;
            this.value = value;
        }

    }

    public static Behavior<Command> create(String wsid) {
        return Behaviors.setup(context ->
                new TradeKafkaProcessor(context, wsid)
        );
    }

    private static double average(List<Double> values) {
        return values.stream().mapToDouble(i -> i).average().getAsDouble();
    }

    private final String wsid;
    private final List<Data> values = new ArrayList<>();

    public TradeKafkaProcessor(ActorContext<Command> context, String wsid) {
        super(context);
        this.wsid = wsid;
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(Record.class, this::onRecord)
                .onSignalEquals(PostStop.instance(), this::postStop)
                .build();
    }

    private Behavior<Command> onRecord(Record record) {
        values.add(record.data);
        record.replyTo.tell(new DataRecorded(wsid));
        return this;
    }

    private Behavior<Command> postStop() {
        getContext().getLog().info("Stopping, losing all recorded state for station {}", wsid);
        return this;
    }

}
