package ai.traceable.config.service;

import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;

import java.util.List;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.core.serviceframework.grpc.GrpcPlatformServerDefinition;
import org.hypertrace.core.serviceframework.grpc.PlatformPeriodicTaskDefinition;
import org.hypertrace.core.serviceframework.grpc.StandAloneGrpcPlatformServiceContainer;

public class TraceableConfigService extends StandAloneGrpcPlatformServiceContainer {
  private static final String INTERNAL_PORT_CONFIG = "service.port.internal";
  private static final String EXTERNAL_PORT_CONFIG = "service.port.external";
  private final SharedConfigServiceProvidersFactory providersFactory =
      new SharedConfigServiceProvidersFactory();
  private final TraceableInternalConfigServiceFactory internalServiceFactory =
      new TraceableInternalConfigServiceFactory(this.providersFactory);

  public TraceableConfigService(ConfigClient configClient) {
    super(configClient);
    this.registerManagedPeriodicTask(
        PlatformPeriodicTaskDefinition.builder()
            .name("Check and report config store health")
            .runnable(this.internalServiceFactory::checkAndReportStoreHealth)
            .initialDelay(ofSeconds(10))
            .period(ofMinutes(1))
            .build());
  }

  @Override
  protected List<GrpcPlatformServerDefinition> getServerDefinitions() {
    return List.of(
        GrpcPlatformServerDefinition.builder()
            .name("networked-internal-traceable-config-service")
            .port(getAppConfig().getInt(INTERNAL_PORT_CONFIG))
            .serviceFactory(this.internalServiceFactory)
            .build(),
        GrpcPlatformServerDefinition.builder()
            .name("networked-external-traceable-config-service")
            .port(getAppConfig().getInt(EXTERNAL_PORT_CONFIG))
            .serviceFactory(new TraceableExternalConfigServiceFactory(this.providersFactory))
            .build());
  }
}
