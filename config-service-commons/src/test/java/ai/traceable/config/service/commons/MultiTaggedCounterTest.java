package ai.traceable.config.service.commons;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.config.service.commons.metrics.MultiTaggedCounter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** From <a href="https://github.com/firedome/dynamic-actuator-metrics">...</a> MIT LICENSE */
public class MultiTaggedCounterTest {
  @Test
  public void multiTaggedCounterTest() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MultiTaggedCounter multiTaggedCounter =
        MultiTaggedCounter.create("sheep", registry, "color", "age-group");
    multiTaggedCounter.increment("black", "young");
    multiTaggedCounter.increment("black", "young");
    multiTaggedCounter.increment("white", "young");
    multiTaggedCounter.increment("black", "old");
    multiTaggedCounter.increment("black", "toddler");
    multiTaggedCounter.increment("black", "old");
    multiTaggedCounter.increment("white", "toddler");
    multiTaggedCounter.increment("brown", "adult");

    assertEquals(6, registry.getMeters().size());
  }

  @Test
  public void multiTaggedCounterIllegalArgTest() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          SimpleMeterRegistry registry = new SimpleMeterRegistry();
          MultiTaggedCounter multiTaggedCounter =
              MultiTaggedCounter.create("sheep", registry, "color", "age-group");
          multiTaggedCounter.increment("black");
        });
  }
}
