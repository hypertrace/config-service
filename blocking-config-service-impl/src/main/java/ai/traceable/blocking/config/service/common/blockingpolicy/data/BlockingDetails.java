package ai.traceable.blocking.config.service.common.blockingpolicy.data;

public interface BlockingDetails {
  <T> T accept(BlockingDetailsVisitor<T> v);
}
