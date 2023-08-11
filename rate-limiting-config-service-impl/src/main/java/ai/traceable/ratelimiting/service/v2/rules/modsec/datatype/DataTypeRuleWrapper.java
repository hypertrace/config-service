package ai.traceable.ratelimiting.service.v2.rules.modsec.datatype;

import ai.traceable.data.classification.config.service.v1.DataTypeRule;
import ai.traceable.ratelimiting.config.service.v2.DatatypeCondition.RegexBasedMatching;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

@Value
@Builder
public class DataTypeRuleWrapper {
  @NonNull String modsecRuleId;
  @NonNull String dataTypeId;
  @NonNull RegexBasedMatching customLocation;
  @NonNull DataTypeRule rule;
}
