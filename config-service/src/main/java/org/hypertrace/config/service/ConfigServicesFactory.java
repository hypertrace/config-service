package org.hypertrace.config.service;

import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.List;
import org.hypertrace.alerting.config.service.EventConditionConfigServiceImpl;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.change.event.impl.ConfigChangeEventGeneratorFactory;
import org.hypertrace.config.service.store.ConfigStore;
import org.hypertrace.config.service.store.DocumentConfigStore;
import org.hypertrace.core.serviceframework.spi.PlatformServiceLifecycle;
import org.hypertrace.label.application.rule.config.service.LabelApplicationRuleConfigServiceImpl;
import org.hypertrace.label.config.service.LabelsConfigServiceImpl;
import org.hypertrace.notification.config.service.NotificationChannelConfigServiceImpl;
import org.hypertrace.notification.config.service.NotificationRuleConfigServiceImpl;
import org.hypertrace.space.config.service.SpacesConfigServiceImpl;

public class ConfigServicesFactory {
  private static final String GENERIC_CONFIG_SERVICE_CONFIG = "generic.config.service";

  public static List<BindableService> buildAllConfigServices(
      Config config, int port, PlatformServiceLifecycle lifecycle) {
    return buildAllConfigServices(
        config, ConfigServicesFactory.buildConfigStore(config), port, lifecycle);
  }

  public static List<BindableService> buildAllConfigServices(
      Config config, ConfigStore configStore, int port, PlatformServiceLifecycle lifecycle) {
    ManagedChannel configChannel =
        ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
    lifecycle.shutdownComplete().thenRun(configChannel::shutdown);

    return List.of(
        new ConfigServiceGrpcImpl(configStore),
        new SpacesConfigServiceImpl(configChannel),
        new LabelsConfigServiceImpl(configChannel, config),
        new LabelApplicationRuleConfigServiceImpl(configChannel),
        new EventConditionConfigServiceImpl(configChannel),
        new NotificationRuleConfigServiceImpl(configChannel),
        new NotificationChannelConfigServiceImpl(configChannel));
  }

  public static ConfigStore buildConfigStore(Config config) {
    try {
      ConfigStore configStore = new DocumentConfigStore();
      Config serviceConfig = config.getConfig(GENERIC_CONFIG_SERVICE_CONFIG);
      ConfigChangeEventGenerator configChangeEventGenerator =
          ConfigChangeEventGeneratorFactory.getInstance()
              .createConfigChangeEventGenerator(serviceConfig);
      configStore.init(serviceConfig, configChangeEventGenerator);
      return configStore;
    } catch (Exception e) {
      throw new RuntimeException("Error in getting or initializing config store", e);
    }
  }
}
