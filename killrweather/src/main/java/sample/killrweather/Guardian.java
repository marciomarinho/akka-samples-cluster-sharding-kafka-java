package sample.killrweather;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;

/**
 * Root actor bootstrapping the application
 */
final class Guardian {

    public static Behavior<Void> create(int httpPort) {
        return Behaviors.setup(context -> {
            TradeKafkaProcessor.initSharding(context.getSystem());
            return Behaviors.empty();
        });
    }
}
