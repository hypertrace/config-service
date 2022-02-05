package ai.traceable.anomaly.config.service.global.status;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.anomaly.config.service.v1.AnomalyApiScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyServiceScope;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatusChange;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.List;
import org.junit.jupiter.api.Test;

public class GlobalConfigStatusConverterTest {

  private final ScopedGlobalConfigStatusChangeConverter converter =
      new ScopedGlobalConfigStatusChangeConverter();

  @Test
  public void testConvert() throws InvalidProtocolBufferException {
    ScopedAnomalyConfigStatusChange config;

    config = ScopedAnomalyConfigStatusChange.getDefaultInstance();
    assertEquals(config, converter.convert(converter.convert(config)));

    for (AnomalyConfigScope scope : getSampleScopesList()) {
      for (AnomalyConfigStatusChange status : getSampleStatusList()) {
        config =
            ScopedAnomalyConfigStatusChange.newBuilder()
                .setConfigScope(scope)
                .setConfigStatus(status)
                .build();
        assertEquals(config, converter.convert(converter.convert(config)));
      }
    }
  }

  @Test
  public void testMerge() {
    for (AnomalyConfigStatusChange highPriorityConfig : getSampleStatusList()) {
      for (AnomalyConfigStatusChange lowPriorityConfig : getSampleStatusList()) {
        assertEquals(
            merge(highPriorityConfig, lowPriorityConfig),
            converter.merge(highPriorityConfig, lowPriorityConfig));
        assertEquals(
            getStatus(merge(highPriorityConfig, lowPriorityConfig)),
            converter.merge(highPriorityConfig, getStatus(lowPriorityConfig)));
      }
    }
  }

  private List<AnomalyConfigScope> getSampleScopesList() {
    return List.of(
        AnomalyConfigScope.newBuilder()
            .setCustomerScope(AnomalyCustomerScope.getDefaultInstance())
            .build(),
        AnomalyConfigScope.newBuilder()
            .setServiceScope(AnomalyServiceScope.newBuilder().setId("service"))
            .build(),
        AnomalyConfigScope.newBuilder()
            .setApiScope(
                AnomalyApiScope.newBuilder()
                    .setId("api")
                    .setServiceScope(AnomalyServiceScope.newBuilder().setId("service")))
            .build());
  }

  private AnomalyConfigStatusChange merge(
      AnomalyConfigStatusChange highPriorityConfig, AnomalyConfigStatusChange lowPriorityConfig) {
    AnomalyConfigStatusChange.Builder builder = lowPriorityConfig.toBuilder();
    if (highPriorityConfig.hasDisabled()) {
      builder.setDisabled(highPriorityConfig.getDisabled());
    }
    if (highPriorityConfig.hasInternal()) {
      builder.setInternal(highPriorityConfig.getInternal());
    }
    return builder.build();
  }

  private List<AnomalyConfigStatusChange> getSampleStatusList() {
    return List.of(
        AnomalyConfigStatusChange.getDefaultInstance(),
        AnomalyConfigStatusChange.newBuilder().setDisabled(true).build(),
        AnomalyConfigStatusChange.newBuilder().setDisabled(false).build(),
        AnomalyConfigStatusChange.newBuilder().setInternal(true).build(),
        AnomalyConfigStatusChange.newBuilder().setInternal(false).build(),
        AnomalyConfigStatusChange.newBuilder().setDisabled(true).setInternal(true).build(),
        AnomalyConfigStatusChange.newBuilder().setDisabled(false).setInternal(true).build(),
        AnomalyConfigStatusChange.newBuilder().setDisabled(true).setInternal(false).build(),
        AnomalyConfigStatusChange.newBuilder().setDisabled(false).setInternal(false).build());
  }

  private AnomalyConfigStatus getStatus(AnomalyConfigStatusChange status) {
    return AnomalyConfigStatus.newBuilder()
        .setDisabled(status.getDisabled())
        .setInternal(status.getInternal())
        .build();
  }
}
