package ai.traceable.threatmanagement.config.service.threatautoblocking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.threatmanagement.config.service.v1.ExcludeAutoBlockingConfig;
import ai.traceable.threatmanagement.config.service.v1.ScopeConfig;
import ai.traceable.threatmanagement.config.service.v1.ThreatAutoBlockingActionConfig;
import ai.traceable.threatmanagement.config.service.v1.ThreatAutoBlockingActionType;
import ai.traceable.threatmanagement.config.service.v1.UpdateThreatAutoBlockingConfigRequest;
import ai.traceable.threatmanagement.config.service.v1.UpdateThreatAutoBlockingConfigRequest.ExpirationDetails;
import java.time.Clock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ThreatAutoBlockingActionConfigConverterTest {
  private static final long CURRENT_TIME_MILLIS = 1000;

  private ThreatAutoBlockingActionConfigConverter configConverter;

  @BeforeEach
  void setup() {
    Clock clock = mock(Clock.class);
    when(clock.millis()).thenReturn(CURRENT_TIME_MILLIS);
    this.configConverter = new ThreatAutoBlockingActionConfigConverter(clock);
  }

  @Test
  void convertDoNothingActionType() {
    UpdateThreatAutoBlockingConfigRequest request =
        UpdateThreatAutoBlockingConfigRequest.newBuilder()
            .setActionType(ThreatAutoBlockingActionType.THREAT_AUTO_BLOCKING_ACTION_TYPE_NO_ACTION)
            .build();

    assertEquals(
        ThreatAutoBlockingActionConfig.newBuilder()
            .setActionType(ThreatAutoBlockingActionType.THREAT_AUTO_BLOCKING_ACTION_TYPE_NO_ACTION)
            .setScope(ScopeConfig.newBuilder().getDefaultInstanceForType())
            .build(),
        configConverter.convert(request));
  }

  @Test
  void convertBlockingActionType_noExpiration() {
    UpdateThreatAutoBlockingConfigRequest request =
        UpdateThreatAutoBlockingConfigRequest.newBuilder()
            .setActionType(ThreatAutoBlockingActionType.THREAT_AUTO_BLOCKING_ACTION_TYPE_BLOCK)
            .build();

    assertEquals(
        ThreatAutoBlockingActionConfig.newBuilder()
            .setActionType(ThreatAutoBlockingActionType.THREAT_AUTO_BLOCKING_ACTION_TYPE_BLOCK)
            .setScope(ScopeConfig.newBuilder().getDefaultInstanceForType())
            .build(),
        configConverter.convert(request));
  }

  @Test
  void convertBlockingActionType_expirationDuration() {
    UpdateThreatAutoBlockingConfigRequest request =
        UpdateThreatAutoBlockingConfigRequest.newBuilder()
            .setActionType(ThreatAutoBlockingActionType.THREAT_AUTO_BLOCKING_ACTION_TYPE_BLOCK)
            .addExcludeConfigs(
                ExcludeAutoBlockingConfig.newBuilder().addUserIdRegexes("^a").build())
            .setExpirationDetails(ExpirationDetails.newBuilder().setDuration("PT1H2M34S").build())
            .build();

    assertEquals(
        ThreatAutoBlockingActionConfig.newBuilder()
            .setActionType(ThreatAutoBlockingActionType.THREAT_AUTO_BLOCKING_ACTION_TYPE_BLOCK)
            .addExcludeConfigs(
                ExcludeAutoBlockingConfig.newBuilder().addUserIdRegexes("^a").build())
            .setExpirationDetails(
                ThreatAutoBlockingActionConfig.ExpirationDetails.newBuilder()
                    .setDuration("PT1H2M34S")
                    .setTimestampMillis(CURRENT_TIME_MILLIS + 3754000)
                    .build())
            .setScope(ScopeConfig.newBuilder().getDefaultInstanceForType())
            .build(),
        configConverter.convert(request));
  }
}
