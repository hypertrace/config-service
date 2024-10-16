package org.hypertrace.partitioner.config.service;

import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.serviceframework.docstore.metrics.DocStoreMetricsRegistry;
import org.hypertrace.core.serviceframework.spi.PlatformServiceLifecycle;

public class DBMetricsReporter {
  private final DocStoreMetricsRegistry metricsRegistry;

  public DBMetricsReporter(
      final Datastore datastore, final PlatformServiceLifecycle platformServiceLifecycle) {
    metricsRegistry =
        new DocStoreMetricsRegistry(datastore).withPlatformLifecycle(platformServiceLifecycle);
  }

  public void monitor() {
    metricsRegistry.monitor();
  }
}
