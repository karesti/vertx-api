package consumer;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import io.vertx.core.Future;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.shareddata.Lock;

public class RebootConsumer extends AbstractVerticle {

   public static final String REBOOT_ADDRESS = "reboot";

   private boolean reboot = false;
   private String id = "ID-" + UUID.randomUUID().toString().substring(0, 4).toUpperCase();

   @Override
   public void start(Future<Void> startFuture) throws Exception {
      System.out.println(id + " verticle started");
      vertx.eventBus().consumer("ids", message -> {
         int id = ((Integer) message.body()).intValue();

         if (id == 0) {
            launchRebootWithLock();
         }
      });

      startFuture.complete();
   }

   private void launchRebootWithLock() {
      if (!reboot) {
         reboot = true;
         vertx.sharedData().getLock("lock", ar -> {
            vertx.eventBus().send(REBOOT_ADDRESS, startRebootMessage());
            System.out.println(">> Start system reboot ... ");
            Lock lock = ar.result();
            vertx.setTimer(3000, h -> {
               vertx.eventBus().send(REBOOT_ADDRESS, endRebootMessage());
               System.out.println("<< Reboot Over");
               reboot = false;
               lock.release();
            });
         });
      }
   }

   private void launchReboot() {
      if (!reboot) {
         reboot = true;
         vertx.eventBus().send(REBOOT_ADDRESS, startRebootMessage());
         System.out.println(">> Start system reboot ... ");

         vertx.setTimer(3000, h -> {
            vertx.eventBus().send(REBOOT_ADDRESS, endRebootMessage());
            System.out.println("<< Reboot Over");
            reboot = false;
         });
      }
   }

   private JsonObject startRebootMessage() {
      JsonObject message = new JsonObject();
      message.put("status", "STARTED");
      message.put("by", id);
      return message;
   }

   private JsonObject endRebootMessage() {
      JsonObject message = new JsonObject();
      message.put("status", "FINISHED");
      message.put("by", id);
      return message;
   }

   public static void main(String[] args) {
      VertxOptions vertxOptions = new VertxOptions().setClustered(true);
      Vertx.clusteredVertx(vertxOptions, ar -> {
         if (ar.failed()) {
            System.err.println("Cannot create vert.x instance : " + ar.cause());
         } else {
            Vertx vertx = ar.result();
            vertx.deployVerticle(RebootConsumer.class.getName());
         }
      });
   }
}
