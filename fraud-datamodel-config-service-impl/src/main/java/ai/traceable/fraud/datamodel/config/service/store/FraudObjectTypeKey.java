package ai.traceable.fraud.datamodel.config.service.store;

import ai.traceable.fraud.datamodel.config.service.v1.ObjectTypeReference;
import lombok.Value;
import org.hypertrace.core.documentstore.Key;

@Value
public class FraudObjectTypeKey implements Key {
  private static final String SEPARATOR = ":";

  String tenantId;
  ObjectTypeReference objectTypeReference;

  @Override
  public String toString() {
    return String.join(
        SEPARATOR,
        this.tenantId,
        this.objectTypeReference.getObjectKind().name(),
        this.objectTypeReference.getId());
  }
}
