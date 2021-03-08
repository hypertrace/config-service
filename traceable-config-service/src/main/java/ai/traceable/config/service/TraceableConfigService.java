package ai.traceable.config.service;

import ai.traceable.localprocessing.config.service.LocalProcessingConfigServiceImpl;
import ai.traceable.localprocessing.config.service.LocalProcessingRulesServiceImpl;
import ai.traceable.ratelimiting.service.RateLimitingConfigServiceImpl;
import ai.traceable.region.config.service.RegionConfigServiceFactory;
import ai.traceable.sensitivedata.config.service.PiiFilterConfigServiceImpl;
import ai.traceable.sensitivedata.config.service.SensitiveDataConfigServiceImpl;
import com.typesafe.config.Config;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import org.hypertrace.config.service.ConfigServicesFactory;
import org.hypertrace.config.service.store.ConfigStore;
import org.hypertrace.core.grpcutils.server.InterceptorUtil;
import org.hypertrace.core.serviceframework.PlatformService;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TraceableConfigService extends PlatformService {

  private static final String SERVICE_NAME_CONFIG = "service.name";
  private static final String INTERNAL_PORT_CONFIG = "service.port.internal";
  private static final String EXTERNAL_PORT_CONFIG = "service.port.external";
  private static final Logger LOG = LoggerFactory.getLogger(TraceableConfigService.class);
  private String serviceName;
  private int internalServerPort;
  private int externalServerPort;
  private ConfigStore configStore;
  private Server internalTraceableConfigServer;
  private Server externalTraceableConfigServer;

  public TraceableConfigService(ConfigClient configClient) {
    super(configClient);
  }

  @Override
  protected void doInit() {
    Config config = getAppConfig();
    serviceName = config.getString(SERVICE_NAME_CONFIG);
    internalServerPort = config.getInt(INTERNAL_PORT_CONFIG);
    externalServerPort = config.getInt(EXTERNAL_PORT_CONFIG);
    LOG.info(
        "Creating {} on ports - {}(internal) and {}(external)",
        serviceName,
        internalServerPort,
        externalServerPort);

    ManagedChannel managedChannel =
        ManagedChannelBuilder.forAddress("localhost", internalServerPort).usePlaintext().build();
    this.getLifecycle().shutdownComplete().thenRun(managedChannel::shutdown);

    ServerBuilder<?> internalServerBuilder = ServerBuilder.forPort(internalServerPort);
    configStore = ConfigServicesFactory.buildConfigStore(getAppConfig());

    ConfigServicesFactory.buildAllConfigServices(configStore, internalServerPort, getLifecycle())
        .stream()
        .map(InterceptorUtil::wrapInterceptors)
        .forEach(internalServerBuilder::addService);

    SensitiveDataConfigServiceImpl sensitiveDataConfigService =
        new SensitiveDataConfigServiceImpl(managedChannel, config);
    RateLimitingConfigServiceImpl rateLimitingConfigService =
        new RateLimitingConfigServiceImpl(managedChannel, config);
    LocalProcessingRulesServiceImpl localProcessingRulesService =
        new LocalProcessingRulesServiceImpl(managedChannel);
    internalServerBuilder
        .addService(InterceptorUtil.wrapInterceptors(sensitiveDataConfigService))
        .addService(InterceptorUtil.wrapInterceptors(rateLimitingConfigService))
        .addService(InterceptorUtil.wrapInterceptors(localProcessingRulesService))
        .addService(InterceptorUtil.wrapInterceptors(RegionConfigServiceFactory.build(config)));
    internalTraceableConfigServer = internalServerBuilder.build();

    ServerBuilder<?> externalServerBuilder = ServerBuilder.forPort(externalServerPort);
    PiiFilterConfigServiceImpl piiFilterConfigService =
        new PiiFilterConfigServiceImpl(managedChannel, config);
    LocalProcessingConfigServiceImpl localProcessingConfigService =
        new LocalProcessingConfigServiceImpl(managedChannel, config);
    externalServerBuilder
        .addService(InterceptorUtil.wrapInterceptors(piiFilterConfigService))
        .addService(InterceptorUtil.wrapInterceptors(localProcessingConfigService));
    externalTraceableConfigServer = externalServerBuilder.build();
  }

  @Override
  protected void doStart() {
    LOG.info(
        "Attempting to start {} on ports - {}(internal) and {}(external)",
        serviceName,
        internalServerPort,
        externalServerPort);
    try {
      internalTraceableConfigServer.start();
      externalTraceableConfigServer.start();
      LOG.info(
          "Started {} on ports - {}(internal) and {}(external)",
          serviceName,
          internalServerPort,
          externalServerPort);
    } catch (IOException e) {
      LOG.error("Unable to start {}", serviceName);
      throw new RuntimeException(e);
    }

    try {
      internalTraceableConfigServer.awaitTermination();
      externalTraceableConfigServer.awaitTermination();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void doStop() {
    LOG.info("Shutting down service: {}", serviceName);
    while (!internalTraceableConfigServer.isShutdown()) {
      internalTraceableConfigServer.shutdown();
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        LOG.warn("Interrupted!", e);
        Thread.currentThread().interrupt();
      }
    }
    while (!externalTraceableConfigServer.isShutdown()) {
      externalTraceableConfigServer.shutdown();
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        LOG.warn("Interrupted!", e);
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public boolean healthCheck() {
    return configStore.healthCheck();
  }
}
