package ai.traceable.modsecurity.rule.secrule.variables;

import static ai.traceable.modsecurity.rule.secrule.ModsecRuleConstants.COLON;

import java.util.Optional;
import lombok.Value;

@Value
public class ModsecVariable {

  private final ModsecVariableMetadata metadata;
  private final Optional<ModsecVariableMetadataOperator> metadataOperator;
  private final Optional<ModsecVariableMetadataKey> metadataKey;

  public ModsecVariable(ModsecVariableMetadata metadata) {
    this.metadata = metadata;
    this.metadataOperator = Optional.empty();
    this.metadataKey = Optional.empty();
  }

  public ModsecVariable(
      ModsecVariableMetadata metadata,
      ModsecVariableMetadataOperator metadataOperator,
      ModsecVariableMetadataKey metadataKey) {
    this.metadata = metadata;
    this.metadataOperator = Optional.ofNullable(metadataOperator);
    this.metadataKey = Optional.ofNullable(metadataKey);
    validate();
  }

  public ModsecVariable(
      ModsecVariableMetadata metadata, ModsecVariableMetadataOperator metadataOperator) {
    this.metadata = metadata;
    this.metadataOperator = Optional.ofNullable(metadataOperator);
    this.metadataKey = Optional.empty();
    validate();
  }

  public ModsecVariable(ModsecVariableMetadata metadata, ModsecVariableMetadataKey metadataKey) {
    this.metadata = metadata;
    this.metadataOperator = Optional.empty();
    this.metadataKey = Optional.ofNullable(metadataKey);
    validate();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    metadataOperator.ifPresent(op -> sb.append(op));
    sb.append(metadata.name());
    metadataKey.ifPresent(key -> sb.append(COLON).append(key.toString()));
    return sb.toString();
  }

  private void validate() {
    boolean isValid =
        metadataOperator
            .filter(ModsecVariableMetadataOperator::needsMetadataKey)
            .map(op -> metadataKey.isPresent())
            .orElse(true);
    if (!isValid) {
      throw new IllegalArgumentException(
          String.format(
              "Modsec Variable Metadata operator %s needs a metadata key", metadataOperator.get()));
    }
  }
}
