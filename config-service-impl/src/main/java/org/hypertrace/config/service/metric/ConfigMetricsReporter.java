package org.hypertrace.config.service.metric;

import java.util.List;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.serviceframework.docstore.metrics.DocStoreCustomMetricReportingConfig;
import org.hypertrace.core.serviceframework.docstore.metrics.DocStoreMetricsRegistry;
import org.hypertrace.core.serviceframework.spi.PlatformServiceLifecycle;

public class ConfigMetricsReporter {
  private final DocStoreMetricsRegistry metricsRegistry;

  public ConfigMetricsReporter(
      final Datastore datastore,
      final PlatformServiceLifecycle lifecycle,
      List<DocStoreCustomMetricReportingConfig> configurationCounterConfig) {
    metricsRegistry =
        new DocStoreMetricsRegistry(datastore)
            .withPlatformLifecycle(lifecycle)
            .withCustomMetrics(configurationCounterConfig);
  }

  public void monitor() {
    metricsRegistry.monitor();
  }
}
