package ai.traceable.data.obfuscation.config.service.rules;

import ai.traceable.data.obfuscation.config.service.v1.ObfuscationStrategy;
import java.time.Instant;
import java.util.Optional;
import org.hypertrace.config.objectstore.DeletedContextualConfigObject;

public class MockDeletedContextualConfigObject
    implements DeletedContextualConfigObject<ObfuscationStrategy> {
  private final ObfuscationStrategy data;

  MockDeletedContextualConfigObject(ObfuscationStrategy data) {
    this.data = data;
  }

  @Override
  public String getContext() {
    return data.getId();
  }

  @Override
  public Optional<ObfuscationStrategy> getDeletedData() {
    return Optional.of(data);
  }

  @Override
  public Instant getCreationTimestamp() {
    return null;
  }

  @Override
  public Instant getLastUpdatedTimestamp() {
    return null;
  }
}
