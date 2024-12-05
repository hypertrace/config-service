package ai.traceable.config.service;

import ai.traceable.alerting.config.service.EventConditionConfigServiceImpl;
import ai.traceable.anomaly.config.service.AnomalyConfigServiceFactory;
import ai.traceable.anomalyscoring.config.service.AnomalyScoringConfigServiceFactory;
import ai.traceable.api.attribute.override.service.ApiAttributeOverridesServiceFactory;
import ai.traceable.api.gateway.config.service.ApiGatewayConfigServiceFactory;
import ai.traceable.api.spec.config.service.ApiSpecConfigServiceFactory;
import ai.traceable.ast.config.service.AstConfigServiceFactory;
import ai.traceable.ast.hooks.config.service.AstHooksConfigServiceFactory;
import ai.traceable.ast.scan.profile.config.service.AstScanProfileConfigServiceFactory;
import ai.traceable.auth.detection.config.service.AuthDetectionConfigServiceFactory;
import ai.traceable.azure.devops.integration.config.service.AzureDevopsIntegrationConfigServiceFactory;
import ai.traceable.customsignature.config.service.CustomSignatureConfigServiceFactory;
import ai.traceable.dashboard.config.service.DashboardConfigServiceFactory;
import ai.traceable.data.classification.config.service.DataClassificationConfigServiceFactory;
import ai.traceable.data.exfiltration.config.service.detection.rule.DataExfiltrationDetectionRulesConfigServiceFactory;
import ai.traceable.data.protection.config.service.DataProtectionConfigServiceFactory;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionConfigServiceFactory;
import ai.traceable.edge.bot.config.service.CaptchaSiteKeyConfigServiceFactory;
import ai.traceable.edge.decision.config.service.EdgeDecisionConfigServiceFactory;
import ai.traceable.fraud.policy.config.service.FraudPolicyConfigServiceFactory;
import ai.traceable.github.integration.config.service.GithubIntegrationConfigServiceFactory;
import ai.traceable.integration.config.service.IntegrationConfigServiceFactory;
import ai.traceable.iprange.config.service.IpRangeConfigServiceFactory;
import ai.traceable.jira.integration.config.service.JiraIntegrationConfigServiceFactory;
import ai.traceable.jwt.extraction.config.service.JwtExtractionConfigServiceFactory;
import ai.traceable.licensestatus.config.service.LicenseStatusConfigServiceFactory;
import ai.traceable.localprocessing.config.service.ruleservice.LocalProcessingRulesServiceFactory;
import ai.traceable.malicioussources.config.service.MaliciousSourcesConfigServiceFactory;
import ai.traceable.policy.config.service.TraceablePolicyConfigServiceFactory;
import ai.traceable.ratelimiting.service.v1.RateLimitingConfigServiceImpl;
import ai.traceable.ratelimiting.service.v2.RateLimitingConfigServiceFactory;
import ai.traceable.region.config.service.RegionConfigServiceFactory;
import ai.traceable.reporting.config.service.v2.ReportingConfigServiceFactory;
import ai.traceable.risk.config.service.RiskConfigServiceFactory;
import ai.traceable.risk.config.service.v2.ApiRiskConfigServiceFactory;
import ai.traceable.runner.logs.config.service.RunnerLogsConfigServiceFactory;
import ai.traceable.saved.filter.config.service.SavedFilterConfigServiceFactory;
import ai.traceable.saved.query.config.service.SavedQueryConfigServiceFactory;
import ai.traceable.sensitivedata.config.service.SensitiveDataConfigServicesProvider;
import ai.traceable.servicenow.itsm.integration.config.service.ServiceNowItsmIntegrationConfigServiceFactory;
import ai.traceable.sessionidentification.config.service.SessionIdentificationConfigServiceFactory;
import ai.traceable.span.processing.config.service.SpanProcessingConfigServiceFactory;
import ai.traceable.splunk.integration.config.service.SplunkIntegrationConfigServiceFactory;
import ai.traceable.syslog.integration.config.service.SyslogIntegrationConfigServiceFactory;
import ai.traceable.threatmanagement.config.service.ThreatManagementConfigServiceFactory;
import ai.traceable.userattribution.config.service.v1.UserAttributionConfigServiceFactory;
import ai.traceable.userattribution.config.service.v2.UserAttributionV2ConfigServiceFactory;
import ai.traceable.vulnerability.config.service.VulnerabilityConfigServiceFactory;
import ai.traceable.waf.provider.integration.service.WafIntegrationConfigServiceFactory;
import io.grpc.BindableService;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import org.hypertrace.config.service.ConfigServiceFactory;
import org.hypertrace.core.documentstore.Datastore;
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
    Datastore datastore =
        DataStoreUtils.initDataStore(providers.getConfig(), environment.getLifecycle());
    return Stream.of(
            hypertraceConfigServiceFactory
                .buildServices(
                    providers.getLocalChannel(),
                    providers.getConfig(),
                    providers.getChangeEventGenerator(),
                    environment,
                    datastore)
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
                    providers.getActivityEventProducer(),
                    providers.getChangeEventGenerator(),
                    providers.getFeatureCachingClient())),
            wrap(
                IpRangeConfigServiceFactory.build(
                    providers.getLocalChannel(),
                    providers.getConfig(),
                    providers.getActivityEventProducer(),
                    providers.getChangeEventGenerator())),
            wrap(
                MaliciousSourcesConfigServiceFactory.build(
                    providers.getLocalChannel(), providers.getChangeEventGenerator())),
            wrap(
                CustomSignatureConfigServiceFactory.build(
                    providers.getLocalChannel(),
                    providers.getConfig(),
                    providers.getActivityEventProducer(),
                    providers.getChangeEventGenerator())),
            wrap(
                UserAttributionConfigServiceFactory.build(
                    providers.getLocalChannel(), providers.getChangeEventGenerator())),
            wrap(
                UserAttributionV2ConfigServiceFactory.build(
                    providers.getLocalChannel(),
                    providers.getChangeEventGenerator(),
                    providers.getFeatureCachingClient())),
            wrap(
                DataProtectionConfigServiceFactory.build(
                    providers.getLocalChannel(), providers.getChangeEventGenerator())),
            wrap(
                SessionIdentificationConfigServiceFactory.build(
                    providers.getLocalChannel(),
                    providers.getChangeEventGenerator(),
                    providers.getFeatureCachingClient(),
                    providers.getConfig())),
            wrap(
                ThreatManagementConfigServiceFactory.build(
                    providers.getLocalChannel(),
                    providers.getConfig(),
                    providers.getChangeEventGenerator())),
            wrap(
                AnomalyScoringConfigServiceFactory.build(
                    providers.getLocalChannel(),
                    providers.getConfig(),
                    providers.getChangeEventGenerator())),
            wrap(
                RiskConfigServiceFactory.build(
                    providers.getLocalChannel(),
                    providers.getConfig(),
                    providers.getChangeEventGenerator())),
            wrap(
                ApiRiskConfigServiceFactory.build(
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
                JiraIntegrationConfigServiceFactory.build(
                    providers.getLocalChannel(), providers.getChangeEventGenerator())),
            wrap(
                new RateLimitingConfigServiceImpl( // v1
                    providers.getLocalChannel(),
                    providers.getConfig(),
                    providers.getActivityEventProducer())),
            wrap(
                RateLimitingConfigServiceFactory.build( // v2
                    providers.getLocalChannel(),
                    providers.getConfig(),
                    providers.getActivityEventProducer(),
                    providers.getChangeEventGenerator(),
                    environment.getChannelRegistry())),
            wrap(
                SpanProcessingConfigServiceFactory.build(
                    providers.getLocalChannel(),
                    providers.getConfig(),
                    providers.getChangeEventGenerator())),
            wrap(
                ApiSpecConfigServiceFactory.build(
                    providers.getLocalChannel(),
                    providers.getConfig(),
                    providers.getChangeEventGenerator())),
            wrap(
                AnomalyConfigServiceFactory.build(
                    providers.getChannelRegistry(),
                    providers.getLocalChannel(),
                    providers.getConfig(),
                    providers.getChangeEventGenerator())),
            wrap(
                ApiAttributeOverridesServiceFactory.build(
                    providers.getLocalChannel(), providers.getChangeEventGenerator())),
            wrap(
                new EventConditionConfigServiceImpl(
                    providers.getLocalChannel(),
                    providers.getChangeEventGenerator(),
                    providers.getFeatureCachingClient())),
            wrap(
                ai.traceable.reporting.config.service.v1.ReportingConfigServiceFactory.build( // v1
                    providers.getLocalChannel(), providers.getChangeEventGenerator())),
            wrap(
                ReportingConfigServiceFactory.build( // v2
                    providers.getLocalChannel(), providers.getChangeEventGenerator())),
            wrap(
                AuthDetectionConfigServiceFactory.build(
                    providers.getLocalChannel(),
                    providers.getChangeEventGenerator(),
                    providers.getConfig())),
            wrap(
                JwtExtractionConfigServiceFactory.build(
                    providers.getLocalChannel(),
                    providers.getChangeEventGenerator(),
                    providers.getConfig())),
            wrap(
                AstScanProfileConfigServiceFactory.build(
                    providers.getLocalChannel(), providers.getConfig())),
            wrap(
                IntegrationConfigServiceFactory.build(
                    providers.getLocalChannel(), providers.getChangeEventGenerator())),
            wrap(
                DetectionExclusionConfigServiceFactory.build(
                    providers.getLocalChannel(),
                    providers.getChangeEventGenerator(),
                    providers.getFeatureCachingClient(),
                    providers.getConfig(),
                    providers.getChannelRegistry())),
            wrap(
                AstConfigServiceFactory.build(
                    providers.getLocalChannel(),
                    providers.getChangeEventGenerator(),
                    providers.getConfig())),
            wrap(
                VulnerabilityConfigServiceFactory.build(
                    providers.getLocalChannel(),
                    providers.getChangeEventGenerator(),
                    providers.getConfig())),
            wrap(
                AstHooksConfigServiceFactory.build(
                    providers.getLocalChannel(), providers.getChangeEventGenerator())),
            wrap(
                ApiGatewayConfigServiceFactory.build(
                    providers.getLocalChannel(), providers.getChangeEventGenerator())),
            wrap(
                SplunkIntegrationConfigServiceFactory.build(
                    providers.getLocalChannel(),
                    providers.getConfig(),
                    providers.getChangeEventGenerator())),
            wrap(
                SyslogIntegrationConfigServiceFactory.build(
                    providers.getLocalChannel(),
                    providers.getConfig(),
                    providers.getChangeEventGenerator())),
            wrap(
                SavedFilterConfigServiceFactory.build(
                    providers.getLocalChannel(),
                    providers.getConfig(),
                    providers.getChangeEventGenerator(),
                    environment.getChannelRegistry())),
            wrap(
                SavedQueryConfigServiceFactory.build(
                    providers.getConfig(),
                    providers.getLocalChannel(),
                    providers.getChangeEventGenerator())),
            wrap(
                RunnerLogsConfigServiceFactory.build(
                    providers.getConfig(),
                    providers.getLocalChannel(),
                    providers.getChangeEventGenerator())),
            wrap(
                DashboardConfigServiceFactory.build(
                    providers.getLocalChannel(), providers.getChangeEventGenerator())),
            wrap(
                FraudPolicyConfigServiceFactory.build(
                    providers.getLocalChannel(), providers.getChangeEventGenerator())),
            wrap(
                ServiceNowItsmIntegrationConfigServiceFactory.build(
                    providers.getLocalChannel(), providers.getChangeEventGenerator())),
            wrap(
                AzureDevopsIntegrationConfigServiceFactory.build(
                    providers.getLocalChannel(), providers.getChangeEventGenerator())),
            wrap(
                CaptchaSiteKeyConfigServiceFactory.build(
                    providers.getLocalChannel(), providers.getChangeEventGenerator())),
            wrap(
                EdgeDecisionConfigServiceFactory.build(
                    providers.getLocalChannel(), providers.getChangeEventGenerator())),
            wrap(
                TraceablePolicyConfigServiceFactory.build(
                    providers.getLocalChannel(), providers.getChangeEventGenerator())),
            wrap(
                GithubIntegrationConfigServiceFactory.build(
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
