package ai.traceable.licensestatus.config.service;

import ai.traceable.license.metering.service.api.v1.LicenseMeteringServiceGrpc;
import ai.traceable.license.metering.service.api.v1.LicenseMeteringServiceGrpc.LicenseMeteringServiceBlockingStub;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class LicenseStatusConfigServiceModule extends AbstractModule {
  private static final String LICENSE_METERING_SERVICE_CONFIG_PATH = "license.metering.service";
  private static final String HOST_CONFIG_NAME = "host";
  private static final String PORT_CONFIG_NAME = "port";
  private final Config config;
  private final GrpcChannelRegistry channelRegistry;
  private final Channel channel;

  LicenseStatusConfigServiceModule(
      GrpcChannelRegistry channelRegistry, Channel channel, Config config) {
    this.channel = channel;
    this.channelRegistry = channelRegistry;
    this.config = config;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(LicenseStatusConfigServiceImpl.class);
    bind(ConfigServiceCoordinator.class).to(ConfigServiceCoordinatorImpl.class);
  }

  @Provides
  ConfigServiceGrpc.ConfigServiceBlockingStub providesConfigService() {
    return ConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  LicenseMeteringServiceGrpc.LicenseMeteringServiceBlockingStub
      providesLicenseMeteringServiceService() {
    Config licenseMeteringServiceConfig =
        this.config.getConfig(LICENSE_METERING_SERVICE_CONFIG_PATH);
    Channel licenseMeteringChannel =
        channelRegistry.forPlaintextAddress(
            licenseMeteringServiceConfig.getString(HOST_CONFIG_NAME),
            licenseMeteringServiceConfig.getInt(PORT_CONFIG_NAME));
    return LicenseMeteringServiceGrpc.newBlockingStub(licenseMeteringChannel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  @Singleton
  LicenseProvider providesLicenseProvider(
      LicenseMeteringServiceBlockingStub licenseMeteringServiceBlockingStub) {
    Config licenseMeteringServiceConfig =
        this.config.getConfig(LICENSE_METERING_SERVICE_CONFIG_PATH);
    return new LicenseProvider(licenseMeteringServiceConfig, licenseMeteringServiceBlockingStub);
  }
}
