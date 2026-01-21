package ai.traceable.ipresolutionstrategy.config.service.v1.store;

import ai.traceable.ipresolutionstrategy.config.service.v1.IpResolutionStrategyConfig;
import java.time.Instant;
import org.hypertrace.config.objectstore.ContextualConfigObject;

class MockContextualConfigObject implements ContextualConfigObject<IpResolutionStrategyConfig> {

  private final IpResolutionStrategyConfig data;

  MockContextualConfigObject(IpResolutionStrategyConfig data) {
    this.data = data;
  }

  @Override
  public String getContext() {
    return data.getId();
  }

  @Override
  public IpResolutionStrategyConfig getData() {
    return data;
  }

  @Override
  public Instant getCreationTimestamp() {
    return null;
  }

  @Override
  public String getCreatedByEmail() {
    return "";
  }

  @Override
  public Instant getLastUserUpdateTimestamp() {
    return null;
  }

  @Override
  public String getLastUserUpdateEmail() {
    return "";
  }

  @Override
  public Instant getLastUpdatedTimestamp() {
    return null;
  }

  @Override
  public String getLastUpdateEmail() {
    return "";
  }
}
