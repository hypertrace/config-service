package ai.traceable.config.service;

import ai.traceable.activity.event.producer.ActivityEventProducer;
import ai.traceable.activity.event.producer.ActivityEventProducerFactory;
import ai.traceable.alerting.config.service.EventConditionConfigServiceImpl;
import ai.traceable.anomaly.config.service.AnomalyConfigServiceFactory;
import ai.traceable.api.attribute.override.service.ApiAttributeOverridesServiceFactory;
import ai.traceable.blocking.config.service.BlockingConfigServiceFactory;
import ai.traceable.customsignature.config.service.CustomSignatureConfigServiceFactory;
import ai.traceable.data.classification.config.service.DataClassificationConfigServiceFactory;
import ai.traceable.data.exfiltration.config.service.detection.rule.DataExfiltrationDetectionRulesConfigServiceFactory;
import ai.traceable.external.userattribution.config.service.ExternalUserAttributionConfigServiceFactory;
import ai.traceable.iprange.config.service.IpRangeConfigServiceFactory;
import ai.traceable.licensestatus.config.service.LicenseStatusConfigServiceImpl;
import ai.traceable.localprocessing.config.service.LocalProcessingConfigServiceFactory;
import ai.traceable.localprocessing.config.service.ruleservice.LocalProcessingRulesServiceFactory;
import ai.traceable.ratelimiting.service.RateLimitingConfigServiceImpl;
import ai.traceable.region.config.service.RegionConfigServiceFactory;
import ai.traceable.reporting.config.service.ReportingConfigServiceFactory;
import ai.traceable.risk.config.service.RiskConfigServiceFactory;
import ai.traceable.sensitivedata.config.service.SensitiveDataConfigServicesProvider;
import ai.traceable.threatmanagement.config.service.ThreatManagementConfigServiceFactory;
import ai.traceable.userattribution.config.service.UserAttributionConfigServiceFactory;
import ai.traceable.waf.provider.integration.service.WafIntegrationConfigServiceFactory;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.time.Clock;
import java.util.List;
import java.util.stream.Collectors;
import org.hypertrace.config.service.ConfigServicesFactory;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.change.event.impl.ConfigChangeEventGeneratorFactory;
import org.hypertrace.config.service.store.ConfigStore;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.server.InterceptorUtil;
import org.hypertrace.core.serviceframework.PlatformService;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.notification.config.service.NotificationChannelConfigServiceImpl;
import org.hypertrace.notification.config.service.NotificationRuleConfigServiceImpl;
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

    GrpcChannelRegistry channelRegistry = new GrpcChannelRegistry();
    ManagedChannel managedChannel = channelRegistry.forAddress("localhost", internalServerPort);
    this.getLifecycle().shutdownComplete().thenRun(channelRegistry::shutdown);

    ConfigChangeEventGenerator configChangeEventGenerator =
        ConfigChangeEventGeneratorFactory.getInstance()
            .createConfigChangeEventGenerator(config, Clock.systemUTC());

    ServerBuilder<?> internalServerBuilder = ServerBuilder.forPort(internalServerPort);
    configStore = ConfigServicesFactory.buildConfigStore(getAppConfig());

    ConfigServicesFactory.buildAllConfigServices(
            config, configStore, internalServerPort, getLifecycle())
        .stream()
        .map(InterceptorUtil::wrapInterceptors)
        .forEach(internalServerBuilder::addService);
    SensitiveDataConfigServicesProvider sensitiveDataConfigServicesProvider =
        new SensitiveDataConfigServicesProvider(
            managedChannel, config, channelRegistry, configChangeEventGenerator);
    ActivityEventProducer activityEventProducer = ActivityEventProducerFactory.build(config);
    this.getLifecycle().shutdownComplete().thenRun(activityEventProducer::close);

    BindableService sensitiveDataConfigService =
        sensitiveDataConfigServicesProvider.getSensitiveDataConfigService();
    RateLimitingConfigServiceImpl rateLimitingConfigService =
        new RateLimitingConfigServiceImpl(managedChannel, config, activityEventProducer);
    LicenseStatusConfigServiceImpl licenseStatusConfigService =
        new LicenseStatusConfigServiceImpl(managedChannel, config);
    BindableService localProcessingRulesService =
        LocalProcessingRulesServiceFactory.build(
            managedChannel, config, configChangeEventGenerator);
    BindableService regionConfigService =
        RegionConfigServiceFactory.build(managedChannel, config, activityEventProducer);
    BindableService ipRangeConfigService =
        IpRangeConfigServiceFactory.build(managedChannel, config, activityEventProducer);

    BindableService customSignatureConfigService =
        CustomSignatureConfigServiceFactory.build(managedChannel, config, activityEventProducer);
    BindableService userAttributionConfigService =
        UserAttributionConfigServiceFactory.build(managedChannel, configChangeEventGenerator);
    BindableService threatManagementConfigService =
        ThreatManagementConfigServiceFactory.build(
            managedChannel, config, configChangeEventGenerator);
    BindableService riskConfigService =
        RiskConfigServiceFactory.build(managedChannel, config, configChangeEventGenerator);
    BindableService dataClassificationConfigService =
        DataClassificationConfigServiceFactory.build(
            managedChannel, configChangeEventGenerator, config);
    BindableService dataExfiltrationDetectionRulesConfigService =
        DataExfiltrationDetectionRulesConfigServiceFactory.build(
            managedChannel, configChangeEventGenerator);
    BindableService wafIntegrationConfigService =
        WafIntegrationConfigServiceFactory.build(
            managedChannel, config, configChangeEventGenerator);

    List<BindableService> anomalyConfigServices =
        AnomalyConfigServiceFactory.build(
            channelRegistry, managedChannel, config, configChangeEventGenerator);
    BindableService apiAttributeOverridesService =
        ApiAttributeOverridesServiceFactory.build(managedChannel);

    EventConditionConfigServiceImpl eventConditionConfigService =
        new EventConditionConfigServiceImpl(managedChannel);
    NotificationChannelConfigServiceImpl notificationChannelConfigService =
        new NotificationChannelConfigServiceImpl(managedChannel, configChangeEventGenerator);
    NotificationRuleConfigServiceImpl notificationRuleConfigService =
        new NotificationRuleConfigServiceImpl(managedChannel, configChangeEventGenerator);
    BindableService reportingConfigService =
        ReportingConfigServiceFactory.build(managedChannel, configChangeEventGenerator);

    internalServerBuilder.addServices(
        anomalyConfigServices.stream()
            .map(InterceptorUtil::wrapInterceptors)
            .collect(Collectors.toUnmodifiableList()));
    internalServerBuilder
        .addService(InterceptorUtil.wrapInterceptors(sensitiveDataConfigService))
        .addService(InterceptorUtil.wrapInterceptors(rateLimitingConfigService))
        .addService(InterceptorUtil.wrapInterceptors(licenseStatusConfigService))
        .addService(InterceptorUtil.wrapInterceptors(localProcessingRulesService))
        .addService(InterceptorUtil.wrapInterceptors(regionConfigService))
        .addService(InterceptorUtil.wrapInterceptors(ipRangeConfigService))
        .addService(InterceptorUtil.wrapInterceptors(customSignatureConfigService))
        .addService(InterceptorUtil.wrapInterceptors(userAttributionConfigService))
        .addService(InterceptorUtil.wrapInterceptors(threatManagementConfigService))
        .addService(InterceptorUtil.wrapInterceptors(riskConfigService))
        .addService(InterceptorUtil.wrapInterceptors(eventConditionConfigService))
        .addService(InterceptorUtil.wrapInterceptors(notificationRuleConfigService))
        .addService(InterceptorUtil.wrapInterceptors(notificationChannelConfigService))
        .addService(InterceptorUtil.wrapInterceptors(reportingConfigService))
        .addService(InterceptorUtil.wrapInterceptors(dataClassificationConfigService))
        .addService(InterceptorUtil.wrapInterceptors(dataExfiltrationDetectionRulesConfigService))
        .addService(InterceptorUtil.wrapInterceptors(wafIntegrationConfigService))
        .addService(InterceptorUtil.wrapInterceptors(apiAttributeOverridesService));

    internalTraceableConfigServer = internalServerBuilder.build();

    ServerBuilder<?> externalServerBuilder = ServerBuilder.forPort(externalServerPort);
    BindableService piiFilterConfigService =
        sensitiveDataConfigServicesProvider.getPiiFilterConfigService();
    BindableService localProcessingConfigService =
        LocalProcessingConfigServiceFactory.build(
            managedChannel, config, configChangeEventGenerator);
    BindableService blockingConfigService = BlockingConfigServiceFactory.build(managedChannel);
    BindableService externalUserAttributionConfigService =
        ExternalUserAttributionConfigServiceFactory.build(managedChannel);
    externalServerBuilder
        .addService(InterceptorUtil.wrapInterceptors(piiFilterConfigService))
        .addService(InterceptorUtil.wrapInterceptors(localProcessingConfigService))
        .addService(InterceptorUtil.wrapInterceptors(blockingConfigService))
        .addService(InterceptorUtil.wrapInterceptors(externalUserAttributionConfigService));
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
