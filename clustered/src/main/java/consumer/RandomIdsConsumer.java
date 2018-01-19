package consumer;

import java.util.concurrent.atomic.AtomicBoolean;

import io.vertx.core.Future;
import io.vertx.core.VertxOptions;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.Vertx;

public class RandomIdsConsumer extends AbstractVerticle {

   private AtomicBoolean reboot = new AtomicBoolean(false);

   @Override
   public void start(Future<Void> startFuture) throws Exception {

      vertx.eventBus().consumer("ids", message -> {
         int id = ((Integer) message.body()).intValue();
         System.out.println(">> " + id);

         if(id == 0 && !reboot.getAndSet(true)) {
            launchReboot();
         }
      });

      startFuture.complete();
   }

   private void launchReboot() {
      System.out.println(">> Start system reboot ... ");

      vertx.setTimer(3000, h -> {
         System.out.println("<< Reboot Over");
         reboot.set(false);
      });
   }

   public static void main(String[] args) {
      VertxOptions vertxOptions = new VertxOptions().setClustered(true);
      Vertx.clusteredVertx(vertxOptions, ar -> {
         if (ar.failed()) {
            System.err.println("Cannot create vert.x instance : " + ar.cause());
         } else {
            Vertx vertx = ar.result();
            vertx.deployVerticle(RandomIdsConsumer.class.getName());
         }
      });
   }
}
