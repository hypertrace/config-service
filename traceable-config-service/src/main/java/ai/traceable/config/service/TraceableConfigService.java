package ai.traceable.config.service;

import ai.traceable.sensitivedata.config.service.PiiFilterConfigServiceImpl;
import ai.traceable.sensitivedata.config.service.SensitiveDataConfigServiceImpl;
import com.typesafe.config.Config;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import org.hypertrace.config.service.ConfigServiceGrpcImpl;
import org.hypertrace.config.service.store.ConfigStore;
import org.hypertrace.config.service.store.DocumentConfigStore;
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
  private static final String GENERIC_CONFIG_SERVICE_CONFIG = "generic.config.service";
  private static final String PII_FILTER_CONFIG_SERVICE_CONFIG = "pii.filter.config.service";
  private static final Logger LOG = LoggerFactory.getLogger(TraceableConfigService.class);
  private String serviceName;
  private int serverPort;
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
        ManagedChannelBuilder.forAddress("localhost", serverPort)
            .usePlaintext()
            .build();
    this.getLifecycle().shutdownComplete().thenRun(managedChannel::shutdown);
    ConfigServiceBlockingStub configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(managedChannel)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());

    Config genericConfigServiceConfig = config.getConfig(GENERIC_CONFIG_SERVICE_CONFIG);
    ConfigServiceGrpcImpl genericConfigServiceGrpcImpl =
        new ConfigServiceGrpcImpl(getConfigStore(genericConfigServiceConfig));
    SensitiveDataConfigServiceImpl sensitiveDataConfigService =
        new SensitiveDataConfigServiceImpl(configServiceBlockingStub);
    Config piiFilterConfigServiceConfig = config.getConfig(PII_FILTER_CONFIG_SERVICE_CONFIG);
    PiiFilterConfigServiceImpl piiFilterConfigService =
        new PiiFilterConfigServiceImpl(configServiceBlockingStub, piiFilterConfigServiceConfig);
    traceableConfigServer =
        ServerBuilder.forPort(serverPort)
            .addService(InterceptorUtil.wrapInterceptors(genericConfigServiceGrpcImpl))
            .addService(InterceptorUtil.wrapInterceptors(sensitiveDataConfigService))
            .addService(InterceptorUtil.wrapInterceptors(piiFilterConfigService))
            .build();
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
    return true;
  }

  private ConfigStore getConfigStore(Config config) {
    try {
      ConfigStore configStore = new DocumentConfigStore();
      configStore.init(config);
      return configStore;
    } catch (Exception e) {
      throw new RuntimeException("Error in getting or initializing config store", e);
    }
  }
}
