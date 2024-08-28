package org.hypertrace.config.service.metric;

import java.util.Collections;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.serviceframework.docstore.metrics.DocStoreMetricsRegistry;
import org.hypertrace.core.serviceframework.spi.PlatformServiceLifecycle;

public class ConfigMetricsReporter {
  private final DocStoreMetricsRegistry metricsRegistry;

  public ConfigMetricsReporter(
      final Datastore datastore, final PlatformServiceLifecycle lifecycle) {
    metricsRegistry =
        new DocStoreMetricsRegistry(datastore)
            .withPlatformLifecycle(lifecycle)
            .withCustomMetrics(Collections.emptyList());
  }

  public void monitor() {
    metricsRegistry.monitor();
  }
}
