package sample.killrweather;

import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Receive;

public class User extends AbstractBehavior<User> {

    @Override
    public Receive<User> createReceive() {
        System.out.println("*************************");
        System.out.println("****** PASSED HERE *******");
        System.out.println("*************************");
        return null;
    }

    public interface Command {
        String getId();
    }


    public final String id;

    public User(ActorContext<User> context, String entityId) {
        super(context);
        this.id = entityId;
        System.out.println("*************************");
        System.out.println("****** PASSED HERE 2 *******");
        System.out.println("*************************");
    }

    public String getId() {
        System.out.println("*************************");
        System.out.println("****** PASSED HERE 3*******");
        System.out.println("*************************");

        return this.id;
    }

}