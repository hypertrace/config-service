package ai.traceable.blocking.config.service.common.rules;

import ai.traceable.anomaly.config.service.v1.modsec.AnomalyModsecConfigServiceGrpc;
import ai.traceable.blocking.config.service.common.rules.fetchers.CustomSignatureRulesFetcher;
import ai.traceable.blocking.config.service.common.rules.fetchers.DlpRulesFetcher;
import ai.traceable.blocking.config.service.common.rules.fetchers.ExclusionRulesFetcher;
import ai.traceable.blocking.config.service.common.rules.fetchers.IpResolutionStrategyFetcher;
import ai.traceable.blocking.config.service.common.rules.fetchers.MaliciousSourcesRulesFetcher;
import ai.traceable.blocking.config.service.common.rules.fetchers.RegionRulesFetcher;
import ai.traceable.blocking.config.service.common.rules.fetchers.RulesFetcher;
import ai.traceable.blocking.config.service.common.rules.fetchers.RulesFetcher.RulesFetcherType;
import ai.traceable.config.utils.refresh.FileRefreshConfig;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionConfigServiceGrpc.DetectionExclusionConfigServiceBlockingStub;
import ai.traceable.ipresolutionstrategy.config.service.v1.IpResolutionStrategyConfigServiceGrpc.IpResolutionStrategyConfigServiceBlockingStub;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesConfigServiceGrpc;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingConfigServiceGrpc;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc;
import com.google.inject.AbstractModule;
import com.google.inject.multibindings.MapBinder;
import java.time.Clock;

public class BlockingRulesFetcherModule extends AbstractModule {
  @Override
  protected void configure() {
    MapBinder<RulesFetcher.RulesFetcherType, RulesFetcher> multiBinder =
        MapBinder.newMapBinder(binder(), RulesFetcher.RulesFetcherType.class, RulesFetcher.class);
    multiBinder
        .addBinding(RulesFetcher.RulesFetcherType.CUSTOM_SIGNATURE)
        .to(CustomSignatureRulesFetcher.class);
    multiBinder
        .addBinding(RulesFetcher.RulesFetcherType.MALICIOUS_SOURCES)
        .to(MaliciousSourcesRulesFetcher.class);
    multiBinder.addBinding(RulesFetcher.RulesFetcherType.REGION).to(RegionRulesFetcher.class);
    multiBinder.addBinding(RulesFetcher.RulesFetcherType.DLP).to(DlpRulesFetcher.class);
    multiBinder.addBinding(RulesFetcherType.EXCLUSION).to(ExclusionRulesFetcher.class);
    multiBinder
        .addBinding(RulesFetcherType.IP_RESOLUTION_STRATEGY)
        .to(IpResolutionStrategyFetcher.class);

    requireBinding(Clock.class);
    requireBinding(RegionConfigServiceGrpc.RegionConfigServiceBlockingStub.class);
    requireBinding(AnomalyModsecConfigServiceGrpc.AnomalyModsecConfigServiceBlockingStub.class);
    requireBinding(FileRefreshConfig.class);
    requireBinding(
        MaliciousSourcesConfigServiceGrpc.MaliciousSourcesConfigServiceBlockingStub.class);
    requireBinding(
        MaliciousSourcesConfigServiceGrpc.MaliciousSourcesConfigServiceBlockingStub.class);
    requireBinding(RateLimitingConfigServiceGrpc.RateLimitingConfigServiceBlockingStub.class);
    requireBinding(DetectionExclusionConfigServiceBlockingStub.class);
    requireBinding(IpResolutionStrategyConfigServiceBlockingStub.class);
  }
}
