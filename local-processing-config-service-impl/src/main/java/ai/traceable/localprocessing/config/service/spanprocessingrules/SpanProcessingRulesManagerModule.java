package ai.traceable.localprocessing.config.service.spanprocessingrules;

import ai.traceable.localprocessing.config.service.spanprocessingrules.excludespanrules.ExcludeSpanRulesManagerModule;
import ai.traceable.localprocessing.config.service.spanprocessingrules.protectionspanrules.ProtectionSpanRulesManagerModule;
import ai.traceable.localprocessing.config.service.spanprocessingrules.ratelimitconfig.RateLimitConfigManagerModule;
import ai.traceable.span.processing.config.service.v1.SpanProcessingConfigServiceGrpc;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;
import io.grpc.Channel;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class SpanProcessingRulesManagerModule extends AbstractModule {

  private final Config config;

  public SpanProcessingRulesManagerModule(Config config) {
    this.config = config;
  }

  @Override
  protected void configure() {
    bind(SpanProcessingRulesManager.class).to(DefaultSpanProcessingRulesManager.class);
    install(new ExcludeSpanRulesManagerModule());
    install(new ProtectionSpanRulesManagerModule());
    install(new RateLimitConfigManagerModule(this.config));
  }

  @Provides
  SpanProcessingConfigServiceGrpc.SpanProcessingConfigServiceBlockingStub
      providesTraceableSpanProcessingConfigServiceBlockingStub(Channel channel) {
    return SpanProcessingConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
