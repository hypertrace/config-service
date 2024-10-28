package ai.traceable.config.service;

import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;

import io.grpc.protobuf.services.HealthStatusManager;
import java.util.List;
import org.hypertrace.core.grpcutils.client.InProcessGrpcChannelRegistry;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.core.serviceframework.grpc.GrpcPlatformServerDefinition;
import org.hypertrace.core.serviceframework.grpc.GrpcServiceContainerEnvironment;
import org.hypertrace.core.serviceframework.grpc.PlatformPeriodicTaskDefinition;
import org.hypertrace.core.serviceframework.grpc.StandAloneGrpcPlatformServiceContainer;
import org.hypertrace.core.serviceframework.http.HttpContainer;
import org.hypertrace.core.serviceframework.http.jetty.JettyHttpServerBuilder;

public class TraceableConfigService extends StandAloneGrpcPlatformServiceContainer {
  private HttpContainer httpContainer;
  private static final String INTERNAL_PORT_CONFIG = "service.port.internal";
  private static final String EXTERNAL_PORT_CONFIG = "service.port.external";
  private static final String GLOBAL_SERVICE_INTERNAL_PORT_CONFIG = "global.service.port.internal";
  private final SharedConfigServiceProvidersFactory providersFactory =
      new SharedConfigServiceProvidersFactory();
  private final TraceableInternalConfigServiceFactory internalServiceFactory =
      new TraceableInternalConfigServiceFactory(this.providersFactory);
  private final TraceableInternalGlobalConfigServiceFactory internalGlobalConfigServiceFactory =
      new TraceableInternalGlobalConfigServiceFactory(this.providersFactory);
  private final TraceableRestConfigServiceFactory restConfigServiceFactory =
      new TraceableRestConfigServiceFactory(this.providersFactory);

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
  protected void doStart() {
    this.httpContainer.start();
    super.doStart();
  }

  @Override
  protected void doStop() {
    this.httpContainer.stop();
    super.doStop();
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
            .build(),
        GrpcPlatformServerDefinition.builder()
            .name("networked-internal-traceable-global-config-service")
            .port(getAppConfig().getInt(GLOBAL_SERVICE_INTERNAL_PORT_CONFIG))
            .serviceFactory(internalGlobalConfigServiceFactory)
            .build());
  }

  @Override
  protected GrpcServiceContainerEnvironment buildContainerEnvironment(
      InProcessGrpcChannelRegistry channelRegistry, HealthStatusManager healthStatusManager) {
    TraceableConfigServiceContainerEnvironment environment =
        new TraceableConfigServiceContainerEnvironment(
            super.buildContainerEnvironment(channelRegistry, healthStatusManager));
    this.initializeHttpContainer(environment);
    return environment;
  }

  private void initializeHttpContainer(TraceableConfigServiceContainerEnvironment environment) {
    this.httpContainer =
        new JettyHttpServerBuilder()
            .addHandlers(this.restConfigServiceFactory.buildHandlers(environment))
            .build();
  }
}
