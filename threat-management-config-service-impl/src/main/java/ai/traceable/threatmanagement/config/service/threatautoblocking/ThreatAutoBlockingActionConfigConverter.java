package ai.traceable.threatmanagement.config.service.threatautoblocking;

import static ai.traceable.threatmanagement.config.service.v1.ThreatAutoBlockingActionType.THREAT_AUTO_BLOCKING_ACTION_TYPE_BLOCK;

import ai.traceable.threatmanagement.config.service.v1.ThreatAutoBlockingActionConfig;
import ai.traceable.threatmanagement.config.service.v1.ThreatAutoBlockingActionType;
import ai.traceable.threatmanagement.config.service.v1.UpdateThreatAutoBlockingConfigRequest;
import ai.traceable.threatmanagement.config.service.v1.UpdateThreatAutoBlockingConfigRequest.ExpirationDetails;
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
    switch (actionType) {
      case THREAT_AUTO_BLOCKING_ACTION_TYPE_NO_ACTION:
        return buildDefaultConfig(actionType);
      case THREAT_AUTO_BLOCKING_ACTION_TYPE_BLOCK:
        if (!request.hasExpirationDetails()) {
          return buildDefaultConfig(actionType);
        }

        return getBlockingActionTypeConfig(request.getExpirationDetails());
      case THREAT_AUTO_BLOCKING_ACTION_TYPE_UNSPECIFIED:
      case UNRECOGNIZED:
      default:
        throw new IllegalArgumentException(
            String.format("Invalid threat auto blocking action config type %s", actionType));
    }
  }

  private ThreatAutoBlockingActionConfig getBlockingActionTypeConfig(
      ExpirationDetails expirationDetails) {
    if (!expirationDetails.hasDuration()) {
      return buildDefaultConfig(THREAT_AUTO_BLOCKING_ACTION_TYPE_BLOCK);
    }

    String duration = expirationDetails.getDuration();
    return ThreatAutoBlockingActionConfig.newBuilder()
        .setActionType(THREAT_AUTO_BLOCKING_ACTION_TYPE_BLOCK)
        .setExpirationDetails(
            ThreatAutoBlockingActionConfig.ExpirationDetails.newBuilder()
                .setDuration(duration)
                .setTimestampMillis(clock.millis() + Duration.parse(duration).toMillis()))
        .build();
  }

  private ThreatAutoBlockingActionConfig buildDefaultConfig(
      ThreatAutoBlockingActionType actionType) {
    return ThreatAutoBlockingActionConfig.newBuilder().setActionType(actionType).build();
  }
}
