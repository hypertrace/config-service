package ai.traceable.threatmanagement.config.service.threatautoblocking;

import static ai.traceable.threatmanagement.config.service.v1.ThreatAutoBlockingActionType.THREAT_AUTO_BLOCKING_ACTION_TYPE_BLOCK;
import static ai.traceable.threatmanagement.config.service.v1.ThreatAutoBlockingActionType.THREAT_AUTO_BLOCKING_ACTION_TYPE_NO_ACTION;

import ai.traceable.threatmanagement.config.service.v1.ThreatAutoBlockingActionConfig;
import ai.traceable.threatmanagement.config.service.v1.ThreatAutoBlockingActionType;
import ai.traceable.threatmanagement.config.service.v1.UpdateThreatAutoBlockingConfigRequest;
import com.google.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class ThreatAutoBlockingActionConfigConverter {
  private final Clock clock;

  @Inject
  ThreatAutoBlockingActionConfigConverter(Clock clock) {
    this.clock = clock;
  }

  public ThreatAutoBlockingActionConfig convert(UpdateThreatAutoBlockingConfigRequest request) {
    ThreatAutoBlockingActionType actionType = request.getActionType();
    ThreatAutoBlockingActionConfig.Builder builder =
        ThreatAutoBlockingActionConfig.newBuilder().setActionType(actionType);
    if (actionType == THREAT_AUTO_BLOCKING_ACTION_TYPE_NO_ACTION) {
      return builder.build();
    }
    if (actionType == THREAT_AUTO_BLOCKING_ACTION_TYPE_BLOCK) {
      if (request.hasExpirationDetails()) {
        builder.setExpirationDetails(convertExpirationDetails(request.getExpirationDetails()));
      }
      return builder.addAllExcludeConfigs(request.getExcludeConfigsList()).build();
    }
    throw new IllegalArgumentException(
        String.format("Invalid threat auto blocking action config type %s", actionType));
  }

  private ThreatAutoBlockingActionConfig.ExpirationDetails convertExpirationDetails(
      UpdateThreatAutoBlockingConfigRequest.ExpirationDetails expirationDetails) {
    String duration = expirationDetails.getDuration();
    return ThreatAutoBlockingActionConfig.ExpirationDetails.newBuilder()
        .setDuration(duration)
        .setTimestampMillis(clock.millis() + Duration.parse(duration).toMillis())
        .build();
  }
}
