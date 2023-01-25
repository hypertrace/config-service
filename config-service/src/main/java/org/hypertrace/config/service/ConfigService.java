package org.hypertrace.config.service;

import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;

import java.util.List;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.core.serviceframework.grpc.GrpcPlatformServerDefinition;
import org.hypertrace.core.serviceframework.grpc.PlatformPeriodicTaskDefinition;
import org.hypertrace.core.serviceframework.grpc.StandAloneGrpcPlatformServiceContainer;

public class ConfigService extends StandAloneGrpcPlatformServiceContainer {

  private final ConfigServiceFactory configServiceFactory = new ConfigServiceFactory();
  private final GlobalConfigServiceFactory globalConfigServiceFactory =
      new GlobalConfigServiceFactory();

  public ConfigService(ConfigClient configClient) {
    super(configClient);
    this.registerManagedPeriodicTask(
        PlatformPeriodicTaskDefinition.builder()
            .name("Check and report config store health")
            .runnable(this.configServiceFactory::checkAndReportStoreHealth)
            .initialDelay(ofSeconds(10))
            .period(ofMinutes(1))
            .build());
  }

  @Override
  protected List<GrpcPlatformServerDefinition> getServerDefinitions() {
    return List.of(
        GrpcPlatformServerDefinition.builder()
            .name(this.getServiceName())
            .port(this.getServicePort())
            .serviceFactory(this.configServiceFactory)
            .build(),
        GrpcPlatformServerDefinition.builder()
            .name("networked-internal-global-config-service")
            .port(
                getAppConfig().getInt(GlobalConfigServiceFactory.GLOBAL_CONFIG_SERVICE_PORT_CONFIG))
            .serviceFactory(globalConfigServiceFactory)
            .build());
  }
}
