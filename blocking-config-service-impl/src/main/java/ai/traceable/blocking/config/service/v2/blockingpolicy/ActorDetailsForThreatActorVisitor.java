package ai.traceable.blocking.config.service.v2.blockingpolicy;

import ai.traceable.blocking.config.service.common.blockingpolicy.data.ActorBlockingDetails;
import ai.traceable.blocking.config.service.v2.ActorDetails;
import ai.traceable.blocking.config.service.v2.ActorDetails.Builder;
import ai.traceable.blocking.config.service.v2.BlockingDetailsCondition;

public class ActorDetailsForThreatActorVisitor extends AbstractBlockingDetailsVisitor {
  @Override
  public BlockingDetailsCondition visit(ActorBlockingDetails actorBlockingDetails) {
    Builder builder =
        ActorDetails.newBuilder().addAllIpAddresses(actorBlockingDetails.getIpAddresses());
    if (actorBlockingDetails.getUserId() != null) {
      builder.setUserId(actorBlockingDetails.getUserId());
    }
    return BlockingDetailsCondition.newBuilder().setActorDetails(builder).build();
  }
}
