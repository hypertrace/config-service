package ai.traceable.audit.utils;

import static ai.traceable.audit.utils.AuditFilterUtils.getLastUserUpdateDetails;

import ai.traceable.config.commons.v1.AuditDetails;
import ai.traceable.config.commons.v1.CreationDetails;
import ai.traceable.config.commons.v1.LastUpdateDetails;
import com.google.common.base.Strings;
import com.google.protobuf.Timestamp;
import java.time.Instant;
import javax.annotation.Nullable;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.tuple.Pair;
import org.hypertrace.config.objectstore.ContextualConfigObject;

@UtilityClass
public class AuditDetailsBuilder {

  public static AuditDetails buildAuditDetails(
      ContextualConfigObject<?> contextual, UserVisibleEmailConfig config) {
    AuditDetails.Builder builder = AuditDetails.newBuilder();
    CreationDetails creationDetails = buildCreationDetails(contextual);
    LastUpdateDetails lastUpdateDetails = buildLastUpdateDetails(contextual, config);

    if (creationDetails != null) {
      builder.setCreationDetails(creationDetails);
    }

    if (lastUpdateDetails != null) {
      builder.setLastUserUpdateDetails(lastUpdateDetails);
    }

    return builder.build();
  }

  @Nullable
  private static LastUpdateDetails buildLastUpdateDetails(
      ContextualConfigObject<?> contextual, UserVisibleEmailConfig config) {
    Pair<Instant, String> lastUserUpdateDetails = getLastUserUpdateDetails(contextual, config);
    boolean hasValidTimestamp = isValidTimestamp(lastUserUpdateDetails.getLeft());
    boolean hasUpdatedByEmail = !Strings.isNullOrEmpty(lastUserUpdateDetails.getRight());
    if (hasValidTimestamp || hasUpdatedByEmail) {
      LastUpdateDetails.Builder updateBuilder = LastUpdateDetails.newBuilder();
      if (hasValidTimestamp) {
        updateBuilder.setUpdatedAt(convert(lastUserUpdateDetails.getLeft()));
      }
      if (hasUpdatedByEmail) {
        updateBuilder.setUpdatedBy(lastUserUpdateDetails.getRight());
      }
      return updateBuilder.build();
    }

    return null;
  }

  @Nullable
  private static CreationDetails buildCreationDetails(ContextualConfigObject<?> contextual) {
    boolean hasValidTimestamp = isValidTimestamp(contextual.getCreationTimestamp());
    boolean hasCreatedByEmail = !Strings.isNullOrEmpty(contextual.getCreatedByEmail());

    if (hasValidTimestamp || hasCreatedByEmail) {
      CreationDetails.Builder creationBuilder = CreationDetails.newBuilder();
      if (hasValidTimestamp) {
        creationBuilder.setCreatedAt(convert(contextual.getCreationTimestamp()));
      }
      if (hasCreatedByEmail) {
        creationBuilder.setCreatedBy(contextual.getCreatedByEmail());
      }
      return creationBuilder.build();
    }

    return null;
  }

  private static boolean isValidTimestamp(Instant creationTimestamp) {
    return creationTimestamp != null && creationTimestamp.getEpochSecond() > 0;
  }

  private static Timestamp convert(Instant instant) {
    return Timestamp.newBuilder()
        .setSeconds(instant.getEpochSecond())
        .setNanos(instant.getNano())
        .build();
  }
}
