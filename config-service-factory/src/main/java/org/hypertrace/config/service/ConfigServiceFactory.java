package org.hypertrace.config.service;

import static java.util.Objects.isNull;

import com.typesafe.config.Config;
import io.grpc.Channel;
import io.grpc.health.v1.HealthCheckResponse.ServingStatus;
import java.time.Clock;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hypertrace.alerting.config.service.EventConditionConfigServiceImpl;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.change.event.impl.ConfigChangeEventGeneratorFactory;
import org.hypertrace.config.service.store.ConfigStore;
import org.hypertrace.config.service.store.DocumentConfigStore;
import org.hypertrace.core.serviceframework.grpc.GrpcPlatformService;
import org.hypertrace.core.serviceframework.grpc.GrpcPlatformServiceFactory;
import org.hypertrace.core.serviceframework.grpc.GrpcServiceContainerEnvironment;
import org.hypertrace.label.application.rule.config.service.LabelApplicationRuleConfigServiceImpl;
import org.hypertrace.label.config.service.LabelsConfigServiceImpl;
import org.hypertrace.notification.config.service.NotificationChannelConfigServiceImpl;
import org.hypertrace.notification.config.service.NotificationRuleConfigServiceImpl;
import org.hypertrace.space.config.service.SpacesConfigServiceImpl;
import org.hypertrace.span.processing.config.service.SpanProcessingConfigServiceFactory;

public class ConfigServiceFactory implements GrpcPlatformServiceFactory {
  private static final String SERVICE_NAME = "config-service";
  private static final String STORE_REPORTING_NAME = "config-service-store";
  private static final String GENERIC_CONFIG_SERVICE_CONFIG = "generic.config.service";

  private ConfigStore store;
  private GrpcServiceContainerEnvironment grpcServiceContainerEnvironment;

  @Override
  public List<GrpcPlatformService> buildServices(
      GrpcServiceContainerEnvironment grpcServiceContainerEnvironment) {
    this.grpcServiceContainerEnvironment = grpcServiceContainerEnvironment;
    Config config = grpcServiceContainerEnvironment.getConfig(SERVICE_NAME);
    this.store = this.buildConfigStore(config);
    Channel localChannel = this.getLocalChannel();
    ConfigChangeEventGenerator configChangeEventGenerator = this.buildChangeEventGenerator(config);

    return Stream.of(
            new ConfigServiceGrpcImpl(this.store),
            new SpacesConfigServiceImpl(localChannel),
            new LabelsConfigServiceImpl(localChannel, config, configChangeEventGenerator),
            new LabelApplicationRuleConfigServiceImpl(
                localChannel, config, configChangeEventGenerator),
            new EventConditionConfigServiceImpl(localChannel, configChangeEventGenerator),
            new NotificationRuleConfigServiceImpl(localChannel, configChangeEventGenerator),
            new NotificationChannelConfigServiceImpl(localChannel, configChangeEventGenerator),
            SpanProcessingConfigServiceFactory.build(localChannel))
        .map(GrpcPlatformService::new)
        .collect(Collectors.toUnmodifiableList());
  }

  public void checkAndReportStoreHealth() {
    if (isNull(this.store) || isNull(this.grpcServiceContainerEnvironment)) {
      return;
    }

    if (this.store.healthCheck()) {
      this.grpcServiceContainerEnvironment.reportServiceStatus(
          STORE_REPORTING_NAME, ServingStatus.SERVING);
    } else {
      this.grpcServiceContainerEnvironment.reportServiceStatus(
          STORE_REPORTING_NAME, ServingStatus.NOT_SERVING);
    }
  }

  protected Channel getLocalChannel() {
    return grpcServiceContainerEnvironment
        .getChannelRegistry()
        .forName(grpcServiceContainerEnvironment.getInProcessChannelName());
  }

  protected ConfigChangeEventGenerator buildChangeEventGenerator(Config config) {
    return ConfigChangeEventGeneratorFactory.getInstance()
        .createConfigChangeEventGenerator(config, Clock.systemUTC());
  }

  protected ConfigStore buildConfigStore(Config config) {
    try {
      ConfigStore configStore = new DocumentConfigStore();
      configStore.init(config.getConfig(GENERIC_CONFIG_SERVICE_CONFIG));
      return configStore;
    } catch (Exception e) {
      throw new RuntimeException("Error in getting or initializing config store", e);
    }
  }
}
