package ai.traceable.jwt.extraction.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.jwt.extraction.config.service.v1.CreateJwtExtractionRuleRequest;
import ai.traceable.jwt.extraction.config.service.v1.JwtExtractionRule;
import ai.traceable.jwt.extraction.config.service.v1.JwtExtractionRuleScope;
import ai.traceable.jwt.extraction.config.service.v1.JwtLocation;
import ai.traceable.jwt.extraction.config.service.v1.JwtProcessingInstruction;
import ai.traceable.jwt.extraction.config.service.v1.Predicate;
import ai.traceable.jwt.extraction.config.service.v1.StringPredicate;
import ai.traceable.jwt.extraction.config.service.v1.UpdateJwtExtractionRuleRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class JwtExtractionConfigRuleBuilderTest {

  @Mock UuidGenerator mockUuidGenerator;
  @InjectMocks JwtExtractionConfigRuleBuilder ruleBuilder;

  @Test
  void buildsCreateRules() {
    when(mockUuidGenerator.generateRandomId()).thenReturn("created-id");
    CreateJwtExtractionRuleRequest unscopedRequest =
        CreateJwtExtractionRuleRequest.newBuilder()
            .setPredicate(
                Predicate.newBuilder()
                    .setUrlPredicate(
                        StringPredicate.newBuilder()
                            .setValue("url")
                            .setOperator(
                                StringPredicate.RelationalOperator.RELATIONAL_OPERATOR_EQUALS)
                            .build()))
            .addLocations(
                JwtLocation.newBuilder()
                    .setRequestHeader(
                        StringPredicate.newBuilder()
                            .setOperator(
                                StringPredicate.RelationalOperator.RELATIONAL_OPERATOR_EQUALS)
                            .setValue("Authorization")
                            .build())
                    .setRegexCaptureGroup("^(?:(?i)Bearer:? )?(.*)$")
                    .build())
            .addInstructions(
                JwtProcessingInstruction.newBuilder()
                    .setAction(
                        JwtProcessingInstruction.Action.newBuilder()
                            .setAddNewAttribute("traceableai.jwt.header.alg")
                            .build())
                    .setValueExtraction(
                        JwtProcessingInstruction.ValueExtraction.newBuilder()
                            .setHeaderKey("alg")
                            .setRawValue(
                                JwtProcessingInstruction.ValueExtraction.RawValue.newBuilder()
                                    .build())
                            .build())
                    .build())
            .build();

    JwtExtractionRule expectedUnscopedRule =
        JwtExtractionRule.newBuilder()
            .setId("created-id")
            .setPredicate(unscopedRequest.getPredicate())
            .addAllLocations(unscopedRequest.getLocationsList())
            .addAllInstructions(unscopedRequest.getInstructionsList())
            .build();

    assertEquals(expectedUnscopedRule, ruleBuilder.build(unscopedRequest));

    JwtExtractionRuleScope scope =
        JwtExtractionRuleScope.newBuilder()
            .setEnvironmentScope(
                JwtExtractionRuleScope.EnvironmentScope.newBuilder()
                    .addEnvironmentNames("test-env")
                    .build())
            .build();

    assertEquals(
        expectedUnscopedRule.toBuilder().setScope(scope).build(),
        ruleBuilder.build(unscopedRequest.toBuilder().setScope(scope).build()));
  }

  @Test
  void buildsUpdateRule() {
    UpdateJwtExtractionRuleRequest unscopedRequest =
        UpdateJwtExtractionRuleRequest.newBuilder()
            .setId("update-id")
            .setPredicate(
                Predicate.newBuilder()
                    .setUrlPredicate(
                        StringPredicate.newBuilder()
                            .setValue("url")
                            .setOperator(
                                StringPredicate.RelationalOperator.RELATIONAL_OPERATOR_EQUALS)
                            .build()))
            .addLocations(
                JwtLocation.newBuilder()
                    .setRequestHeader(
                        StringPredicate.newBuilder()
                            .setOperator(
                                StringPredicate.RelationalOperator.RELATIONAL_OPERATOR_EQUALS)
                            .setValue("Authorization")
                            .build())
                    .setRegexCaptureGroup("^(?:(?i)Bearer:? )?(.*)$")
                    .build())
            .addInstructions(
                JwtProcessingInstruction.newBuilder()
                    .setAction(
                        JwtProcessingInstruction.Action.newBuilder()
                            .setAddNewAttribute("traceableai.jwt.header.alg")
                            .build())
                    .setValueExtraction(
                        JwtProcessingInstruction.ValueExtraction.newBuilder()
                            .setHeaderKey("alg")
                            .setRawValue(
                                JwtProcessingInstruction.ValueExtraction.RawValue.newBuilder()
                                    .build())
                            .build())
                    .build())
            .build();

    JwtExtractionRule expectedUnscopedRule =
        JwtExtractionRule.newBuilder()
            .setId("update-id")
            .setPredicate(unscopedRequest.getPredicate())
            .addAllLocations(unscopedRequest.getLocationsList())
            .addAllInstructions(unscopedRequest.getInstructionsList())
            .build();

    assertEquals(expectedUnscopedRule, ruleBuilder.build(unscopedRequest));

    JwtExtractionRuleScope scope =
        JwtExtractionRuleScope.newBuilder()
            .setEnvironmentScope(
                JwtExtractionRuleScope.EnvironmentScope.newBuilder()
                    .addEnvironmentNames("test-env")
                    .build())
            .build();

    assertEquals(
        expectedUnscopedRule.toBuilder().setScope(scope).build(),
        ruleBuilder.build(unscopedRequest.toBuilder().setScope(scope).build()));
  }
}
