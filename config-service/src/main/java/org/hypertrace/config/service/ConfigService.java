package org.hypertrace.config.service;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.core.serviceframework.grpc.GrpcPlatformServiceFactory;
import org.hypertrace.core.serviceframework.grpc.StandAloneGrpcPlatformServiceContainer;

public class ConfigService extends StandAloneGrpcPlatformServiceContainer {

  private final ConfigServiceFactory configServiceFactory = new ConfigServiceFactory();
  private ScheduledFuture<?> storeReportingFuture;

  public ConfigService(ConfigClient configClient) {
    super(configClient);
  }

  @Override
  protected GrpcPlatformServiceFactory getServiceFactory() {
    return this.configServiceFactory;
  }

  @Override
  protected void doStart() {
    this.storeReportingFuture = this.startReportingStoreHealth();
    super.doStart();
  }

  private ScheduledFuture<?> startReportingStoreHealth() {
    return Executors.newSingleThreadScheduledExecutor()
        .scheduleAtFixedRate(
            this.configServiceFactory::checkAndReportStoreHealth, 10, 60, TimeUnit.SECONDS);
  }

  @Override
  protected void doStop() {
    this.storeReportingFuture.cancel(false);
    super.doStop();
  }
}
