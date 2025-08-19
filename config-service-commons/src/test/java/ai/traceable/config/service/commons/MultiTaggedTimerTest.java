package ai.traceable.config.service.commons;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.traceable.config.service.commons.metrics.MultiTaggedTimer;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** From <a href="https://github.com/firedome/dynamic-actuator-metrics">...</a> MIT LICENSE */
public class MultiTaggedTimerTest {

  @Test
  public void multiTaggedTimerTest() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MultiTaggedTimer multiTaggedTimer =
        MultiTaggedTimer.create("some-timer", registry, "who", "action");
    multiTaggedTimer
        .getTimer("Eric", "walk-the-dog")
        .record(
            new Runnable() {
              @Override
              public void run() {
                try {
                  Thread.sleep(500);
                } catch (InterruptedException e) {
                  e.printStackTrace();
                }
              }
            });

    multiTaggedTimer.getTimer("Eric", "make-dinner").record(30, TimeUnit.MINUTES);
    multiTaggedTimer.getTimer("Benz", "make-dinner").record(30, TimeUnit.MINUTES);
    multiTaggedTimer.getTimer("Benz", "homework").record(2, TimeUnit.HOURS);
    multiTaggedTimer.getTimer("Eric", "walk-the-dog").record(10, TimeUnit.MINUTES);
    multiTaggedTimer.getTimer("Benz", "walk-the-dog").record(15, TimeUnit.MINUTES);
    List<Meter> meters = registry.getMeters();
    assertEquals(5, meters.size());
  }

  @Test
  public void multiTaggedTimerIllegalArgsTest() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MultiTaggedTimer multiTaggedTimer =
        MultiTaggedTimer.create("some-timer", registry, "who", "action");
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          multiTaggedTimer.getTimer("walk-the-dog").record(800, TimeUnit.MINUTES);
        });
  }
}
