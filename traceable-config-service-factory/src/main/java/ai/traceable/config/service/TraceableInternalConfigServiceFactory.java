package ai.traceable.config.service;

import ai.traceable.alerting.config.service.EventConditionConfigServiceImpl;
import ai.traceable.anomaly.config.service.AnomalyConfigServiceFactory;
import ai.traceable.api.attribute.override.service.ApiAttributeOverridesServiceFactory;
import ai.traceable.api.spec.config.service.ApiSpecConfigServiceFactory;
import ai.traceable.customsignature.config.service.CustomSignatureConfigServiceFactory;
import ai.traceable.data.classification.config.service.DataClassificationConfigServiceFactory;
import ai.traceable.data.exfiltration.config.service.detection.rule.DataExfiltrationDetectionRulesConfigServiceFactory;
import ai.traceable.iprange.config.service.IpRangeConfigServiceFactory;
import ai.traceable.licensestatus.config.service.LicenseStatusConfigServiceFactory;
import ai.traceable.localprocessing.config.service.ruleservice.LocalProcessingRulesServiceFactory;
import ai.traceable.ratelimiting.service.v1.RateLimitingConfigServiceImpl;
import ai.traceable.ratelimiting.service.v2.RateLimitingConfigServiceFactory;
import ai.traceable.region.config.service.RegionConfigServiceFactory;
import ai.traceable.reporting.config.service.ReportingConfigServiceFactory;
import ai.traceable.risk.config.service.RiskConfigServiceFactory;
import ai.traceable.sensitivedata.config.service.SensitiveDataConfigServicesProvider;
import ai.traceable.span.processing.config.service.SpanProcessingConfigServiceFactory;
import ai.traceable.threatmanagement.config.service.ThreatManagementConfigServiceFactory;
import ai.traceable.userattribution.config.service.UserAttributionConfigServiceFactory;
import ai.traceable.waf.provider.integration.service.WafIntegrationConfigServiceFactory;
import io.grpc.BindableService;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import org.hypertrace.config.service.ConfigServiceFactory;
import org.hypertrace.core.serviceframework.grpc.GrpcPlatformService;
import org.hypertrace.core.serviceframework.grpc.GrpcPlatformServiceFactory;
import org.hypertrace.core.serviceframework.grpc.GrpcServiceContainerEnvironment;

@RequiredArgsConstructor
public class TraceableInternalConfigServiceFactory implements GrpcPlatformServiceFactory {
  @Nonnull SharedConfigServiceProvidersFactory providersFactory;

  private final ConfigServiceFactory hypertraceConfigServiceFactory = new ConfigServiceFactory();

  @Override
  public List<GrpcPlatformService> buildServices(GrpcServiceContainerEnvironment environment) {
    SharedConfigServiceProviders providers =
        providersFactory.getProvidersForEnvironment(environment);
    return Stream.of(
            hypertraceConfigServiceFactory
                .buildServices(
                    providers.getLocalChannel(),
                    providers.getConfig(),
                    providers.getChangeEventGenerator())
                .stream(),
            wrap(
                new SensitiveDataConfigServicesProvider(
                        providers.getLocalChannel(),
                        providers.getConfig(),
                        providers.getChannelRegistry(),
                        providers.getChangeEventGenerator(),
                        providers.getFeatureCachingClient())
                    .getSensitiveDataConfigService()),
            wrap(
                LicenseStatusConfigServiceFactory.build(
                    providers.getChannelRegistry(),
                    providers.getLocalChannel(),
                    providers.getConfig())),
            wrap(
                LocalProcessingRulesServiceFactory.build(
                    providers.getLocalChannel(),
                    providers.getConfig(),
                    providers.getChangeEventGenerator())),
            wrap(
                RegionConfigServiceFactory.build(
                    providers.getLocalChannel(),
                    providers.getConfig(),
                    providers.getActivityEventProducer())),
            wrap(
                IpRangeConfigServiceFactory.build(
                    providers.getLocalChannel(),
                    providers.getConfig(),
                    providers.getActivityEventProducer())),
            wrap(
                CustomSignatureConfigServiceFactory.build(
                    providers.getLocalChannel(),
                    providers.getConfig(),
                    providers.getActivityEventProducer())),
            wrap(
                UserAttributionConfigServiceFactory.build(
                    providers.getLocalChannel(), providers.getChangeEventGenerator())),
            wrap(
                ThreatManagementConfigServiceFactory.build(
                    providers.getLocalChannel(),
                    providers.getConfig(),
                    providers.getChangeEventGenerator())),
            wrap(
                RiskConfigServiceFactory.build(
                    providers.getLocalChannel(),
                    providers.getConfig(),
                    providers.getChangeEventGenerator())),
            wrap(
                DataClassificationConfigServiceFactory.build(
                    providers.getLocalChannel(),
                    providers.getChangeEventGenerator(),
                    providers.getConfig(),
                    providers.getFeatureCachingClient())),
            wrap(
                DataExfiltrationDetectionRulesConfigServiceFactory.build(
                    providers.getLocalChannel(), providers.getChangeEventGenerator())),
            wrap(
                WafIntegrationConfigServiceFactory.build(
                    providers.getLocalChannel(),
                    providers.getConfig(),
                    providers.getChangeEventGenerator())),
            wrap(
                new RateLimitingConfigServiceImpl( // v1
                    providers.getLocalChannel(),
                    providers.getConfig(),
                    providers.getActivityEventProducer())),
            wrap(
                RateLimitingConfigServiceFactory.build( // v2
                    providers.getLocalChannel(),
                    providers.getConfig(),
                    providers.getActivityEventProducer())),
            wrap(
                SpanProcessingConfigServiceFactory.build(
                    providers.getLocalChannel(), providers.getConfig())),
            wrap(
                ApiSpecConfigServiceFactory.build(
                    providers.getLocalChannel(), providers.getConfig())),
            wrap(
                AnomalyConfigServiceFactory.build(
                    providers.getChannelRegistry(),
                    providers.getLocalChannel(),
                    providers.getConfig(),
                    providers.getChangeEventGenerator())),
            wrap(
                ApiAttributeOverridesServiceFactory.build(
                    providers.getLocalChannel(), providers.getChangeEventGenerator())),
            wrap(new EventConditionConfigServiceImpl(providers.getLocalChannel())),
            wrap(
                ReportingConfigServiceFactory.build(
                    providers.getLocalChannel(), providers.getChangeEventGenerator())))
        .flatMap(stream -> stream)
        .collect(Collectors.toUnmodifiableList());
  }

  public void checkAndReportStoreHealth() {
    this.hypertraceConfigServiceFactory.checkAndReportStoreHealth();
  }

  Stream<GrpcPlatformService> wrap(BindableService bindableService) {
    return Stream.of(new GrpcPlatformService(bindableService));
  }

  Stream<GrpcPlatformService> wrap(Collection<BindableService> bindableServices) {
    return bindableServices.stream().map(GrpcPlatformService::new);
  }
}
