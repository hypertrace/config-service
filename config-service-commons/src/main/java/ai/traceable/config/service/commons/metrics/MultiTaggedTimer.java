package ai.traceable.config.service.commons.metrics;

import io.micrometer.core.instrument.ImmutableTag;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.noop.NoopTimer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** From <a href="https://github.com/firedome/dynamic-actuator-metrics">...</a> MIT LICENSE */
public class MultiTaggedTimer {
  private final String name;
  private final String[] tagNames;
  private final MeterRegistry registry;
  private final boolean noop;
  private final Map<String, Timer> timers = new HashMap<>();

  private MultiTaggedTimer(String name, MeterRegistry registry, String... tags) {
    this.name = name;
    this.tagNames = tags;
    this.registry = registry;
    this.noop = this.registry == null;
  }

  public static MultiTaggedTimer create(String name, MeterRegistry registry, String... tags) {
    return new MultiTaggedTimer(name, registry, tags);
  }

  public static MultiTaggedTimer noop() {
    return new MultiTaggedTimer("", null);
  }

  private boolean isNoop() {
    return this.noop;
  }

  public Timer getTimer(String... tagValues) {
    if (isNoop()) {
      return new NoopTimer(new Meter.Id("", Tags.empty(), null, "", Meter.Type.TIMER));
    }
    String valuesString = Arrays.toString(tagValues);
    if (tagValues.length != tagNames.length) {
      throw new IllegalArgumentException(
          "Timer tags mismatch! Expected args are "
              + Arrays.toString(tagNames)
              + ", provided tags are "
              + valuesString);
    }
    Timer timer = timers.get(valuesString);
    if (timer == null) {
      List<Tag> tags = new ArrayList<>(tagNames.length);
      for (int ii = 0; ii < tagNames.length; ii++) {
        tags.add(new ImmutableTag(tagNames[ii], tagValues[ii]));
      }
      timer = Timer.builder(name).tags(tags).register(registry);
      timers.put(valuesString, timer);
    }
    return timer;
  }
}
