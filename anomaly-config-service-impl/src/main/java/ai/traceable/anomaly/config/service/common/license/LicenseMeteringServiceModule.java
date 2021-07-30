package ai.traceable.anomaly.config.service.common.license;

import ai.traceable.license.metering.service.api.v1.LicenseMeteringServiceGrpc;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.typesafe.config.Config;
import io.grpc.ManagedChannel;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class LicenseMeteringServiceModule extends AbstractModule {

  private final LicenseMeteringServiceConfig config;
  private final GrpcChannelRegistry channelRegistry;

  public LicenseMeteringServiceModule(Config config, GrpcChannelRegistry channelRegistry) {
    this.config = new LicenseMeteringServiceConfig(config);
    this.channelRegistry = channelRegistry;
  }

  @Provides
  @Singleton
  LicenseMeteringServiceConfig providesLicenseMeteringServiceConfig() {
    return config;
  }

  @Provides
  @Singleton
  LicenseMeteringServiceGrpc.LicenseMeteringServiceBlockingStub providesLicenseMeteringService() {
    ManagedChannel licenseMeteringChannel =
        channelRegistry.forAddress(config.getHost(), config.getPort());
    return LicenseMeteringServiceGrpc.newBlockingStub(licenseMeteringChannel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
