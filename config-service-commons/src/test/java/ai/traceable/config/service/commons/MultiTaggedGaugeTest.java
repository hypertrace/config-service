package ai.traceable.config.service.commons;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.traceable.config.service.commons.metrics.MultiTaggedGauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

/** From <a href="https://github.com/firedome/dynamic-actuator-metrics">...</a> MIT LICENSE */
public class MultiTaggedGaugeTest {
  @Test
  public void multiTaggedGaugeTest() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MultiTaggedGauge multiTaggedGauge =
        MultiTaggedGauge.create("weekly-high-tide", registry, "city", "day");
    multiTaggedGauge.set(1.75, "Halifax", "Sunday");
    multiTaggedGauge.set(1.3, "Portland", "Sunday");
    multiTaggedGauge.set(2, "Portland", "Monday");
    multiTaggedGauge.set(0.81, "Venice", "Monday");
    multiTaggedGauge.set(1.8, "Halifax", "Monday");
    multiTaggedGauge.set(1.98, "Halifax", "Monday");
    multiTaggedGauge.set(0.43, "Venice", "Tuesday");
    multiTaggedGauge.set(1.86, "Halifax", "Tuesday");
    multiTaggedGauge.set(2.4, "Portland", "Monday");
    multiTaggedGauge.set(0.56, "Venice", "Tuesday");

    assertEquals(7, registry.getMeters().size());
  }

  @Test
  public void multiTaggedGaugeIllegalArgsTest() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MultiTaggedGauge multiTaggedGauge =
        MultiTaggedGauge.create("weekly-high-tide", registry, "city", "day");
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          multiTaggedGauge.set(1.75, "Halifax");
        });
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          multiTaggedGauge.set(1.75, "Halifax", "Sunday", "boo");
        });
  }
}
