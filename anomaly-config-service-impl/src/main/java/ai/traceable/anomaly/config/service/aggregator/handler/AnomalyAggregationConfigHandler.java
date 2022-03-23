package ai.traceable.anomaly.config.service.aggregator.handler;

import static ai.traceable.anomaly.config.service.common.AnomalyConfigServiceUtils.mergeConfigs;

import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.aggregator.EventAggregationConfig;
import ai.traceable.anomaly.config.service.v1.aggregator.EventAggregationFamilyConfig;
import ai.traceable.anomaly.config.service.v1.aggregator.EventAggregationGlobalConfig;
import ai.traceable.anomaly.config.service.v1.aggregator.ScopedAnomalyEventAggregationConfig;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.Map;
import java.util.stream.Collectors;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;

public class AnomalyAggregationConfigHandler {

  public Value convert(ScopedAnomalyEventAggregationConfig config)
      throws InvalidProtocolBufferException {
    return ConfigProtoConverter.convertToValue(config);
  }

  public ScopedAnomalyEventAggregationConfig convert(Value config)
      throws InvalidProtocolBufferException {
    ScopedAnomalyEventAggregationConfig.Builder builder =
        ScopedAnomalyEventAggregationConfig.newBuilder();

    if (config != null && config.getKindCase() != Value.KindCase.NULL_VALUE) {
      ConfigProtoConverter.mergeFromValue(config, builder);
    }
    return builder.build();
  }

  public ScopedAnomalyEventAggregationConfig merge(
      ScopedAnomalyEventAggregationConfig preferredConfig,
      ScopedAnomalyEventAggregationConfig fallbackConfig) {

    if (fallbackConfig.equals(ScopedAnomalyEventAggregationConfig.getDefaultInstance())) {
      return preferredConfig;
    }

    if (preferredConfig.equals(ScopedAnomalyEventAggregationConfig.getDefaultInstance())) {
      return fallbackConfig;
    }

    return ScopedAnomalyEventAggregationConfig.newBuilder()
        .setConfigScope(preferredConfig.getConfigScope())
        .setEventAggregationConfig(
            mergeEventAggregationConfig(
                preferredConfig.getEventAggregationConfig(),
                fallbackConfig.getEventAggregationConfig()))
        .build();
  }

  private EventAggregationConfig mergeEventAggregationConfig(
      EventAggregationConfig preferredEventAggregationConfig,
      EventAggregationConfig fallbackEventAggregationConfig) {

    if (fallbackEventAggregationConfig.equals(EventAggregationConfig.getDefaultInstance())) {
      return preferredEventAggregationConfig;
    }

    if (preferredEventAggregationConfig.equals(EventAggregationConfig.getDefaultInstance())) {
      return fallbackEventAggregationConfig;
    }

    Map<AnomalyEventFamily, EventAggregationFamilyConfig> preferredEventFamilyConfigMap =
        preferredEventAggregationConfig.getFamilyConfigsList().stream()
            .collect(
                Collectors.toMap(
                    EventAggregationFamilyConfig::getAnomalyEventFamily,
                    eventAggregation -> eventAggregation));

    fallbackEventAggregationConfig
        .getFamilyConfigsList()
        .forEach(
            eventAggregationFamilyConfig -> {
              if (!preferredEventFamilyConfigMap.containsKey(
                  eventAggregationFamilyConfig.getAnomalyEventFamily())) {
                preferredEventFamilyConfigMap.put(
                    eventAggregationFamilyConfig.getAnomalyEventFamily(),
                    eventAggregationFamilyConfig);
              }
              preferredEventFamilyConfigMap.put(
                  eventAggregationFamilyConfig.getAnomalyEventFamily(),
                  (EventAggregationFamilyConfig)
                      mergeConfigs(
                          eventAggregationFamilyConfig,
                          preferredEventFamilyConfigMap.get(
                              eventAggregationFamilyConfig.getAnomalyEventFamily())));
            });

    return EventAggregationConfig.newBuilder()
        .setGlobalConfig(
            (EventAggregationGlobalConfig)
                mergeConfigs(
                    fallbackEventAggregationConfig.getGlobalConfig(),
                    preferredEventAggregationConfig.getGlobalConfig()))
        .addAllFamilyConfigs(preferredEventFamilyConfigMap.values())
        .build();
  }
}
