package ai.traceable.audit.utils;

import java.time.Instant;
import lombok.Builder;
import lombok.Data;
import org.hypertrace.config.objectstore.ContextualConfigObject;

@Data
@Builder
public class ContextualObjectImpl<T> implements ContextualConfigObject<T> {
  String context;
  T data;
  Instant creationTimestamp;
  Instant lastUpdatedTimestamp;
  String createdByEmail;
  Instant lastUserUpdateTimestamp;
  String lastUserUpdateEmail;
  String lastUpdateEmail;
}
