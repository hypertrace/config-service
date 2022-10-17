package ai.traceable.anomaly.config.service.override.common.validator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.anomaly.config.service.v1.override.DetectionOverrideConditionsConfig;
import ai.traceable.anomaly.config.service.v1.override.KeyMatchCondition;
import ai.traceable.anomaly.config.service.v1.override.KeyMetadata;
import ai.traceable.anomaly.config.service.v1.override.KeyValueMatchCondition;
import ai.traceable.anomaly.config.service.v1.override.MatchCondition;
import ai.traceable.anomaly.config.service.v1.override.MatchConditionClause;
import ai.traceable.anomaly.config.service.v1.override.MatchConditionClauseOperator;
import ai.traceable.anomaly.config.service.v1.override.MatchConditionsClauseGroup;
import ai.traceable.anomaly.config.service.v1.override.MatchOperator;
import ai.traceable.anomaly.config.service.v1.override.MatchValue;
import io.grpc.Status;
import org.junit.jupiter.api.Test;

public class DetectionOverrideConditionsValidatorTest {

  private final DetectionOverrideConditionsValidator conditionsValidator =
      new DetectionOverrideConditionsValidator();

  @Test
  public void testValidateConditionsConfig() {
    DetectionOverrideConditionsConfig config =
        DetectionOverrideConditionsConfig.getDefaultInstance();
    Status status = conditionsValidator.validateConditionsConfig(config);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals(
        "Conditions Config should have a match condition clause group", status.getDescription());
  }

  @Test
  public void testValidateMatchConditionsClauseGroup() {
    DetectionOverrideConditionsConfig config =
        DetectionOverrideConditionsConfig.newBuilder()
            .setMatchConditionsClauseGroup(MatchConditionsClauseGroup.getDefaultInstance())
            .build();
    Status status = conditionsValidator.validateConditionsConfig(config);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals(
        "Match condition clause operator in match condition clause group should have a valid operator",
        status.getDescription());
  }

  @Test
  public void testValidateConditions() {
    DetectionOverrideConditionsConfig config =
        DetectionOverrideConditionsConfig.newBuilder()
            .setMatchConditionsClauseGroup(
                MatchConditionsClauseGroup.newBuilder()
                    .setClauseType(
                        MatchConditionClauseOperator.MATCH_CONDITION_CLAUSE_OPERATOR_AND))
            .build();
    Status status = conditionsValidator.validateConditionsConfig(config);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals(
        "Conditions should have at least one match condition clause", status.getDescription());

    config =
        DetectionOverrideConditionsConfig.newBuilder()
            .setMatchConditionsClauseGroup(
                MatchConditionsClauseGroup.newBuilder()
                    .setClauseType(MatchConditionClauseOperator.MATCH_CONDITION_CLAUSE_OPERATOR_AND)
                    .addConditions(MatchConditionClause.getDefaultInstance()))
            .build();
    status = conditionsValidator.validateConditionsConfig(config);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals(
        "Invalid case of match condition clause CONDITION_NOT_SET", status.getDescription());
  }

  @Test
  public void testValidateKeyValueMatchCondition() {
    DetectionOverrideConditionsConfig config =
        DetectionOverrideConditionsConfig.newBuilder()
            .setMatchConditionsClauseGroup(
                MatchConditionsClauseGroup.newBuilder()
                    .setClauseType(MatchConditionClauseOperator.MATCH_CONDITION_CLAUSE_OPERATOR_AND)
                    .addConditions(
                        MatchConditionClause.newBuilder()
                            .setKeyValueCondition(KeyValueMatchCondition.getDefaultInstance())))
            .build();
    Status status = conditionsValidator.validateConditionsConfig(config);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals(
        "KeyValueMatchCondition should have a key/value match condition", status.getDescription());
  }

  @Test
  public void testValidateKeyMatchCondition() {
    DetectionOverrideConditionsConfig config =
        DetectionOverrideConditionsConfig.newBuilder()
            .setMatchConditionsClauseGroup(
                MatchConditionsClauseGroup.newBuilder()
                    .setClauseType(MatchConditionClauseOperator.MATCH_CONDITION_CLAUSE_OPERATOR_AND)
                    .addConditions(
                        MatchConditionClause.newBuilder()
                            .setKeyValueCondition(
                                KeyValueMatchCondition.newBuilder()
                                    .setKeyMatchCondition(KeyMatchCondition.getDefaultInstance())
                                    .setValueMatchCondition(MatchCondition.getDefaultInstance()))))
            .build();
    Status status = conditionsValidator.validateConditionsConfig(config);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals("Metadata should have a valid key metadata", status.getDescription());
  }

  @Test
  public void testValidateMatchCondition() {
    DetectionOverrideConditionsConfig config =
        DetectionOverrideConditionsConfig.newBuilder()
            .setMatchConditionsClauseGroup(
                MatchConditionsClauseGroup.newBuilder()
                    .setClauseType(MatchConditionClauseOperator.MATCH_CONDITION_CLAUSE_OPERATOR_AND)
                    .addConditions(
                        MatchConditionClause.newBuilder()
                            .setKeyValueCondition(
                                KeyValueMatchCondition.newBuilder()
                                    .setKeyMatchCondition(
                                        KeyMatchCondition.newBuilder()
                                            .setMetadata(KeyMetadata.KEY_METADATA_URL))
                                    .setValueMatchCondition(MatchCondition.getDefaultInstance()))))
            .build();
    Status status = conditionsValidator.validateConditionsConfig(config);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals("Operator should have a valid match operator", status.getDescription());

    config =
        DetectionOverrideConditionsConfig.newBuilder()
            .setMatchConditionsClauseGroup(
                MatchConditionsClauseGroup.newBuilder()
                    .setClauseType(MatchConditionClauseOperator.MATCH_CONDITION_CLAUSE_OPERATOR_AND)
                    .addConditions(
                        MatchConditionClause.newBuilder()
                            .setKeyValueCondition(
                                KeyValueMatchCondition.newBuilder()
                                    .setKeyMatchCondition(
                                        KeyMatchCondition.newBuilder()
                                            .setMetadata(KeyMetadata.KEY_METADATA_URL))
                                    .setValueMatchCondition(
                                        MatchCondition.newBuilder()
                                            .setOperator(
                                                MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)))))
            .build();
    status = conditionsValidator.validateConditionsConfig(config);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals("MatchCondition should have a match value", status.getDescription());
  }

  @Test
  public void testValidateMatchValue() {
    DetectionOverrideConditionsConfig config =
        DetectionOverrideConditionsConfig.newBuilder()
            .setMatchConditionsClauseGroup(
                MatchConditionsClauseGroup.newBuilder()
                    .setClauseType(MatchConditionClauseOperator.MATCH_CONDITION_CLAUSE_OPERATOR_AND)
                    .addConditions(
                        MatchConditionClause.newBuilder()
                            .setKeyValueCondition(
                                KeyValueMatchCondition.newBuilder()
                                    .setKeyMatchCondition(
                                        KeyMatchCondition.newBuilder()
                                            .setMetadata(KeyMetadata.KEY_METADATA_URL))
                                    .setValueMatchCondition(
                                        MatchCondition.newBuilder()
                                            .setOperator(MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)
                                            .setValue(MatchValue.getDefaultInstance())))))
            .build();
    Status status = conditionsValidator.validateConditionsConfig(config);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals("Invalid case of match value VALUE_NOT_SET", status.getDescription());

    config =
        DetectionOverrideConditionsConfig.newBuilder()
            .setMatchConditionsClauseGroup(
                MatchConditionsClauseGroup.newBuilder()
                    .setClauseType(MatchConditionClauseOperator.MATCH_CONDITION_CLAUSE_OPERATOR_AND)
                    .addConditions(
                        MatchConditionClause.newBuilder()
                            .setKeyValueCondition(
                                KeyValueMatchCondition.newBuilder()
                                    .setKeyMatchCondition(
                                        KeyMatchCondition.newBuilder()
                                            .setMetadata(KeyMetadata.KEY_METADATA_URL))
                                    .setValueMatchCondition(
                                        MatchCondition.newBuilder()
                                            .setOperator(MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)
                                            .setValue(
                                                MatchValue.newBuilder().setStringValue(""))))))
            .build();
    status = conditionsValidator.validateConditionsConfig(config);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals("Value shouldn't have an empty string value", status.getDescription());

    config =
        DetectionOverrideConditionsConfig.newBuilder()
            .setMatchConditionsClauseGroup(
                MatchConditionsClauseGroup.newBuilder()
                    .setClauseType(MatchConditionClauseOperator.MATCH_CONDITION_CLAUSE_OPERATOR_AND)
                    .addConditions(
                        MatchConditionClause.newBuilder()
                            .setKeyValueCondition(
                                KeyValueMatchCondition.newBuilder()
                                    .setKeyMatchCondition(
                                        KeyMatchCondition.newBuilder()
                                            .setMetadata(KeyMetadata.KEY_METADATA_URL))
                                    .setValueMatchCondition(
                                        MatchCondition.newBuilder()
                                            .setOperator(MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)
                                            .setValue(
                                                MatchValue.newBuilder().setStringValue("*"))))))
            .build();
    status = conditionsValidator.validateConditionsConfig(config);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    config =
        DetectionOverrideConditionsConfig.newBuilder()
            .setMatchConditionsClauseGroup(
                MatchConditionsClauseGroup.newBuilder()
                    .setClauseType(MatchConditionClauseOperator.MATCH_CONDITION_CLAUSE_OPERATOR_AND)
                    .addConditions(
                        MatchConditionClause.newBuilder()
                            .setKeyValueCondition(
                                KeyValueMatchCondition.newBuilder()
                                    .setKeyMatchCondition(
                                        KeyMatchCondition.newBuilder()
                                            .setMetadata(KeyMetadata.KEY_METADATA_URL))
                                    .setValueMatchCondition(
                                        MatchCondition.newBuilder()
                                            .setOperator(MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)
                                            .setValue(
                                                MatchValue.newBuilder().setStringValue("."))))))
            .build();
    status = conditionsValidator.validateConditionsConfig(config);
    assertEquals(Status.OK, status);
  }
}
