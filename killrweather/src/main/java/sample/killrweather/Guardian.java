package sample.killrweather;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Adapter;
import akka.actor.typed.javadsl.Behaviors;
import akka.kafka.ConsumerSettings;
import akka.kafka.cluster.sharding.KafkaClusterSharding;
import com.fasterxml.jackson.databind.deser.std.StringDeserializer;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

/**
 * Root actor bootstrapping the application
 */
final class Guardian {

  public static Behavior<Void> create(int httpPort) {
    return Behaviors.setup(context -> {
      WeatherStation.initSharding(context.getSystem());
      WeatherRoutes routes = new WeatherRoutes(context.getSystem());
      WeatherHttpServer.start(routes.weather(), httpPort, context.getSystem());

      return Behaviors.empty();
    });
  }
}
