package ai.traceable.blocking.config.service.v2.blockingpolicy;

import ai.traceable.blocking.config.service.common.blockingpolicy.data.ActorBlockingDetails;
import ai.traceable.blocking.config.service.v2.BlockingDetailsCondition;
import ai.traceable.blocking.config.service.v2.IpDetails;

public class IpDetailsForThreatActorVisitor extends AbstractBlockingDetailsVisitor {
  @Override
  public BlockingDetailsCondition visit(ActorBlockingDetails actorBlockingDetails) {
    return BlockingDetailsCondition.newBuilder()
        .setIpDetails(
            IpDetails.newBuilder().addAllIpAddresses(actorBlockingDetails.getIpAddresses()))
        .build();
  }
}
