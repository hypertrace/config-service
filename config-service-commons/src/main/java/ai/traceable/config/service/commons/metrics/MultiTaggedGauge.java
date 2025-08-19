package ai.traceable.config.service.commons.metrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.ImmutableTag;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** From <a href="https://github.com/firedome/dynamic-actuator-metrics">...</a> MIT LICENSE */
public class MultiTaggedGauge {

  private final String name;
  private final String[] tagNames;
  private final MeterRegistry registry;
  private final boolean noop;
  private final Map<String, DoubleWrapper> gaugeValues = new HashMap<>();

  private MultiTaggedGauge(String name, MeterRegistry registry, String... tags) {
    this.name = name;
    this.tagNames = tags;
    this.registry = registry;
    this.noop = this.registry == null;
  }

  public static MultiTaggedGauge create(String name, MeterRegistry registry, String... tags) {
    return new MultiTaggedGauge(name, registry, tags);
  }

  public static MultiTaggedGauge noop() {
    return new MultiTaggedGauge("", null);
  }

  private boolean isNoop() {
    return this.noop;
  }

  public void set(double value, String... tagValues) {
    String valuesString = Arrays.toString(tagValues);
    if (tagValues.length != tagNames.length) {
      throw new IllegalArgumentException(
          "Gauge tags mismatch! Expected args are "
              + Arrays.toString(tagNames)
              + ", provided tags are "
              + valuesString);
    }

    DoubleWrapper number = gaugeValues.get(valuesString);
    if (number == null) {
      List<Tag> tags = new ArrayList<>(tagNames.length);
      for (int ii = 0; ii < tagNames.length; ii++) {
        tags.add(new ImmutableTag(tagNames[ii], tagValues[ii]));
      }
      DoubleWrapper valueHolder = new DoubleWrapper(value);
      Gauge.builder(name, valueHolder, DoubleWrapper::getValue).tags(tags).register(registry);
      gaugeValues.put(valuesString, valueHolder);
    } else {
      number.setValue(value);
    }
  }
}
