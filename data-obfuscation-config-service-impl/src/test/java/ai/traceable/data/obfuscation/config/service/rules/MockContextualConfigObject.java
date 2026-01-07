package ai.traceable.data.obfuscation.config.service.rules;

import ai.traceable.data.obfuscation.config.service.v1.ObfuscationStrategy;
import java.time.Instant;
import org.hypertrace.config.objectstore.ContextualConfigObject;

class MockContextualConfigObject implements ContextualConfigObject<ObfuscationStrategy> {

  private final ObfuscationStrategy data;

  MockContextualConfigObject(ObfuscationStrategy data) {
    this.data = data;
  }

  @Override
  public String getContext() {
    return data.getId();
  }

  @Override
  public ObfuscationStrategy getData() {
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
