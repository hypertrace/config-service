package ai.traceable.blocking.config.service.common.blockingpolicy.data;

public interface BlockingDetailsVisitor<T> {
  T visit(ActorBlockingDetails actorBlockingDetails);

  T visit(CombinationBlockingDetails combinationBlockingDetails);

  T visit(CustomSignatureBlockingDetails customSignatureBlockingDetails);

  T visit(IpBlockingDetails ipBlockingDetails);

  T visit(IpTypeBlockingDetails ipTypeBlockingDetails);

  T visit(ModsecBlockingDetails modsecBlockingDetails);

  T visit(RegionBlockingDetails regionBlockingDetails);
}
