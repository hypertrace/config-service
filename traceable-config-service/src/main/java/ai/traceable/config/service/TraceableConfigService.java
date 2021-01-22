package ai.traceable.config.service;

import ai.traceable.ratelimiting.service.RateLimitingConfigServiceImpl;
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
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.server.InterceptorUtil;
import org.hypertrace.core.serviceframework.PlatformService;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TraceableConfigService extends PlatformService {

  private static final String SERVICE_NAME_CONFIG = "service.name";
  private static final String SERVICE_PORT_CONFIG = "service.port";
  private static final String RATE_LIMITING_CONFIG_SERVICE_CONFIG =
      "rate.limiting.config.service";
  private static final String SENSITIVE_DATA_CONFIG_SERVICE_CONFIG =
      "sensitive.data.config.service";
  private static final Logger LOG = LoggerFactory.getLogger(TraceableConfigService.class);
  private String serviceName;
  private int serverPort;
  private ConfigStore configStore;
  private Server traceableConfigServer;

  public TraceableConfigService(ConfigClient configClient) {
    super(configClient);
  }

  @Override
  protected void doInit() {
    Config config = getAppConfig();
    serviceName = config.getString(SERVICE_NAME_CONFIG);
    serverPort = config.getInt(SERVICE_PORT_CONFIG);
    LOG.info("Creating {} on port {}", serviceName, serverPort);

    ManagedChannel managedChannel =
        ManagedChannelBuilder.forAddress("localhost", serverPort).usePlaintext().build();
    this.getLifecycle().shutdownComplete().thenRun(managedChannel::shutdown);

    ServerBuilder<?> serverBuilder = ServerBuilder.forPort(serverPort);
    configStore = ConfigServicesFactory.buildConfigStore(getAppConfig());

    ConfigServicesFactory.buildAllConfigServices(configStore, serverPort, getLifecycle())
                         .stream()
                         .map(InterceptorUtil::wrapInterceptors)
                         .forEach(serverBuilder::addService);

    Config sensitiveDataConfigServiceConfig =
        config.getConfig(SENSITIVE_DATA_CONFIG_SERVICE_CONFIG);
    ConfigServiceBlockingStub configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(managedChannel)
                         .withCallCredentials(
                             RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
    SensitiveDataConfigServiceImpl sensitiveDataConfigService =
        new SensitiveDataConfigServiceImpl(
            configServiceBlockingStub, sensitiveDataConfigServiceConfig);
    PiiFilterConfigServiceImpl piiFilterConfigService =
        new PiiFilterConfigServiceImpl(configServiceBlockingStub, sensitiveDataConfigServiceConfig);
        Config rateLimitingConfigServiceConfig = config.getConfig(RATE_LIMITING_CONFIG_SERVICE_CONFIG);
    RateLimitingConfigServiceImpl rateLimitingConfigService =
        new RateLimitingConfigServiceImpl(configServiceBlockingStub, config);
    serverBuilder
        .addService(InterceptorUtil.wrapInterceptors(sensitiveDataConfigService))
        .addService(InterceptorUtil.wrapInterceptors(piiFilterConfigService))
        .addService(InterceptorUtil.wrapInterceptors(rateLimitingConfigService));

    traceableConfigServer = serverBuilder.build();
  }

  @Override
  protected void doStart() {
    LOG.info("Attempting to start {} on port {}", serviceName, serverPort);
    try {
      traceableConfigServer.start();
      LOG.info("Started Traceable Config Service on port {}", serverPort);
    } catch (IOException e) {
      LOG.error("Unable to start Traceable Config Service");
      throw new RuntimeException(e);
    }

    try {
      traceableConfigServer.awaitTermination();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void doStop() {
    LOG.info("Shutting down service: {}", serviceName);
    while (!traceableConfigServer.isShutdown()) {
      traceableConfigServer.shutdown();
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
