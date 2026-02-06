package ai.traceable.audit.utils;

import static ai.traceable.audit.utils.AuditFilterUtils.getLastUserUpdateDetails;

import ai.traceable.config.commons.v1.AuditDetails;
import ai.traceable.config.commons.v1.CreationDetails;
import ai.traceable.config.commons.v1.LastUpdateDetails;
import com.google.protobuf.Timestamp;
import java.time.Instant;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.tuple.Pair;
import org.hypertrace.config.objectstore.ContextualConfigObject;

@UtilityClass
public class AuditDetailsBuilder {

  public static AuditDetails buildAuditDetails(
      ContextualConfigObject<?> contextual, UserVisibleEmailConfig config) {
    AuditDetails.Builder builder = AuditDetails.newBuilder();

    Instant creationTimestamp = contextual.getCreationTimestamp();
    if (creationTimestamp != null && creationTimestamp.getEpochSecond() > 0) {
      CreationDetails.Builder creationBuilder =
          CreationDetails.newBuilder().setCreatedAt(convert(creationTimestamp));
      if (contextual.getCreatedByEmail() != null) {
        creationBuilder.setCreatedBy(contextual.getCreatedByEmail());
      }
      builder.setCreationDetails(creationBuilder);
    }

    Pair<Instant, String> lastUserUpdateDetails = getLastUserUpdateDetails(contextual, config);
    Instant updateTimestamp = lastUserUpdateDetails.getLeft();
    String userEmail = lastUserUpdateDetails.getRight();
    if (updateTimestamp != null && updateTimestamp.getEpochSecond() > 0) {
      LastUpdateDetails.Builder updateBuilder =
          LastUpdateDetails.newBuilder()
              .setUpdatedAt(convert(updateTimestamp))
              .setUpdatedBy(userEmail);
      builder.setLastUserUpdateDetails(updateBuilder);
    }

    return builder.build();
  }

  private static Timestamp convert(Instant instant) {
    return Timestamp.newBuilder()
        .setSeconds(instant.getEpochSecond())
        .setNanos(instant.getNano())
        .build();
  }
}
