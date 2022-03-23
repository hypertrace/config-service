package ai.traceable.anomaly.config.service.aggregator;

import ai.traceable.anomaly.config.service.aggregator.handler.AnomalyAggregationConfigHandler;
import ai.traceable.anomaly.config.service.v1.AnomalyApiScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.aggregator.AggregationConfig;
import ai.traceable.anomaly.config.service.v1.aggregator.EventAggregationConfig;
import ai.traceable.anomaly.config.service.v1.aggregator.EventAggregationFamilyConfig;
import ai.traceable.anomaly.config.service.v1.aggregator.EventAggregationGlobalConfig;
import ai.traceable.anomaly.config.service.v1.aggregator.ScopedAnomalyEventAggregationConfig;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AggregationConfigHandlerTest {

  public static final String API_ID = "apiId";
  private final AnomalyAggregationConfigHandler anomalyAggregationConfigHandler =
      new AnomalyAggregationConfigHandler();

  @Test
  public void testScopedAnomalyEventAggregationConfigConversion()
      throws InvalidProtocolBufferException {
    ScopedAnomalyEventAggregationConfig anomalyEventAggregationConfig =
        getScopedAnomalyEventAggregationConfig();
    Value anomalyEventAggregationConfigVal =
        anomalyAggregationConfigHandler.convert(anomalyEventAggregationConfig);
    ScopedAnomalyEventAggregationConfig actualAnomalyEventAggregationConfig =
        anomalyAggregationConfigHandler.convert(anomalyEventAggregationConfigVal);
    Assertions.assertEquals(anomalyEventAggregationConfig, actualAnomalyEventAggregationConfig);
  }

  @Test
  public void testScopedAnomalyEventMerging() {
    ScopedAnomalyEventAggregationConfig preferredAnomalyEventAggregationConfig =
        getScopedAnomalyEventAggregationConfig();
    ScopedAnomalyEventAggregationConfig fallbackAnomalyEventAggregationConfig =
        getScopedAnomalyEventAggregationFallbackConfig();
    ScopedAnomalyEventAggregationConfig mergedAnomalyEventAggregationConfig =
        anomalyAggregationConfigHandler.merge(
            preferredAnomalyEventAggregationConfig, fallbackAnomalyEventAggregationConfig);
    Assertions.assertEquals(
        2, mergedAnomalyEventAggregationConfig.getEventAggregationConfig().getFamilyConfigsCount());
    Assertions.assertEquals(
        getPreferredGlobalConfig(),
        mergedAnomalyEventAggregationConfig.getEventAggregationConfig().getGlobalConfig());
    mergedAnomalyEventAggregationConfig
        .getEventAggregationConfig()
        .getFamilyConfigsList()
        .forEach(
            eventConfig -> {
              if (eventConfig.getAnomalyEventFamily()
                  == AnomalyEventFamily.ANOMALY_EVENT_FAMILY_API_DEF) {
                Assertions.assertEquals(getPreferredAggregationApiDefConfig(), eventConfig);
              } else if (eventConfig.getAnomalyEventFamily()
                  == AnomalyEventFamily.ANOMALY_EVENT_FAMILY_MODSEC) {
                Assertions.assertEquals(getFallbackModsecConfig(), eventConfig);
              }
            });
  }

  private ScopedAnomalyEventAggregationConfig getScopedAnomalyEventAggregationConfig() {
    return ScopedAnomalyEventAggregationConfig.newBuilder()
        .setConfigScope(
            AnomalyConfigScope.newBuilder()
                .setApiScope(AnomalyApiScope.newBuilder().setId(API_ID).build())
                .build())
        .setEventAggregationConfig(
            EventAggregationConfig.newBuilder()
                .setGlobalConfig(getPreferredGlobalConfig())
                .addAllFamilyConfigs(List.of(getPreferredAggregationApiDefConfig()))
                .build())
        .build();
  }

  private ScopedAnomalyEventAggregationConfig getScopedAnomalyEventAggregationFallbackConfig() {
    return ScopedAnomalyEventAggregationConfig.newBuilder()
        .setConfigScope(
            AnomalyConfigScope.newBuilder()
                .setApiScope(AnomalyApiScope.newBuilder().setId(API_ID).build())
                .build())
        .setEventAggregationConfig(
            EventAggregationConfig.newBuilder()
                .setGlobalConfig(getFallbackGlobalConfig())
                .addAllFamilyConfigs(List.of(getFallbackApiDefConfig(), getFallbackModsecConfig()))
                .build())
        .build();
  }

  private EventAggregationFamilyConfig getFallbackModsecConfig() {
    return EventAggregationFamilyConfig.newBuilder()
        .setAnomalyEventFamily(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_MODSEC)
        .setAggregationConfig(
            AggregationConfig.newBuilder()
                .setMaxEventsWithScorePerParamValue(10)
                .setMaxEventsWithScorePerParamValue(10)
                .build())
        .build();
  }

  private EventAggregationGlobalConfig getFallbackGlobalConfig() {
    return EventAggregationGlobalConfig.newBuilder()
        .setParamNameMaxDuration("1d")
        .setParamValueMaxDuration("1d")
        .build();
  }

  private EventAggregationFamilyConfig getFallbackApiDefConfig() {
    return EventAggregationFamilyConfig.newBuilder()
        .setAnomalyEventFamily(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_API_DEF)
        .setAggregationConfig(
            AggregationConfig.newBuilder()
                .setMaxEventsWithScorePerParamValue(10)
                .setMaxEventsWithScorePerParamValue(10)
                .build())
        .build();
  }

  private EventAggregationGlobalConfig getPreferredGlobalConfig() {
    return EventAggregationGlobalConfig.newBuilder()
        .setParamNameMaxDuration("2d")
        .setParamValueMaxDuration("2d")
        .build();
  }

  private EventAggregationFamilyConfig getPreferredAggregationApiDefConfig() {
    return EventAggregationFamilyConfig.newBuilder()
        .setAnomalyEventFamily(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_API_DEF)
        .setAggregationConfig(
            AggregationConfig.newBuilder()
                .setMaxEventsWithScorePerParamValue(20)
                .setMaxEventsWithScorePerParamValue(20)
                .setMaxUsersPerParam(20)
                .build())
        .build();
  }
}
