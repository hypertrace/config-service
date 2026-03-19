package ai.traceable.aiapp.protection.config.service;

import ai.traceable.aiapp.protection.config.service.firewall.AiAppConfigServiceConfig;
import ai.traceable.aiapp.protection.config.service.firewall.AiAppEvaluationConfigContextModule;
import ai.traceable.anomaly.config.service.AnomalyConfigServiceConfig;
import ai.traceable.anomaly.config.service.v1.detector.DetectorConfigServiceGrpc;
import ai.traceable.anomaly.config.service.v1.global.AnomalyGlobalConfigServiceGrpc;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingConfigServiceGrpc;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class AiAppProtectionConfigServiceModule extends AbstractModule {
  private final Channel channel;

  public AiAppProtectionConfigServiceModule(Channel channel) {
    this.channel = channel;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(AiAppProtectionConfigServiceImpl.class);
    bind(Channel.class).toInstance(channel);
    install(new AiAppEvaluationConfigContextModule());
  }

  @Provides
  @Singleton
  AiAppConfigServiceConfig provideAiAppConfigServiceConfig(
      AnomalyConfigServiceConfig anomalyConfigServiceConfig) {
    return new AiAppConfigServiceConfig(anomalyConfigServiceConfig.getAiAppConfigServiceConfig());
  }

  @Provides
  @Singleton
  RateLimitingConfigServiceGrpc.RateLimitingConfigServiceBlockingStub
      provideRateLimitingConfigService() {
    return RateLimitingConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  @Singleton
  CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub
      provideCustomSignatureConfigService() {
    return CustomSignatureConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  @Singleton
  AnomalyGlobalConfigServiceGrpc.AnomalyGlobalConfigServiceBlockingStub
      provideAnomalyGlobalConfigService() {
    return AnomalyGlobalConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  @Singleton
  DetectorConfigServiceGrpc.DetectorConfigServiceBlockingStub provideDetectorConfigService() {
    return DetectorConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
