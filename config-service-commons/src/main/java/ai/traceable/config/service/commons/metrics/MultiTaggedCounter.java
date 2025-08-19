package ai.traceable.config.service.commons.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.ImmutableTag;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** From <a href="https://github.com/firedome/dynamic-actuator-metrics">...</a> MIT LICENSE */
public class MultiTaggedCounter {
  private final String name;
  private final String[] tagNames;
  private final MeterRegistry registry;
  private final boolean noop;
  private final Map<String, Counter> counters = new HashMap<>();

  private MultiTaggedCounter(String name, MeterRegistry registry, String... tags) {
    this.name = name;
    this.tagNames = tags;
    this.registry = registry;
    this.noop = this.registry == null;
  }

  public static MultiTaggedCounter create(String name, MeterRegistry registry, String... tags) {
    return new MultiTaggedCounter(name, registry, tags);
  }

  public static MultiTaggedCounter noop() {
    return new MultiTaggedCounter("", null);
  }

  private boolean isNoop() {
    return this.noop;
  }

  public void increment(String... tagValues) {
    increment(1, tagValues);
  }

  public void increment(double amount, String... tagValues) {
    if (isNoop()) {
      return;
    }
    String valuesString = Arrays.toString(tagValues);
    if (tagValues.length != tagNames.length) {
      throw new IllegalArgumentException(
          "Counter tags mismatch! Expected args are "
              + Arrays.toString(tagNames)
              + ", provided tags are "
              + valuesString);
    }
    Counter counter = counters.get(valuesString);
    if (counter == null) {
      List<Tag> tags = new ArrayList<>(tagNames.length);
      for (int ii = 0; ii < tagNames.length; ii++) {
        tags.add(new ImmutableTag(tagNames[ii], tagValues[ii]));
      }
      counter = Counter.builder(name).tags(tags).register(registry);
      counters.put(valuesString, counter);
    }
    counter.increment(amount);
  }
}
