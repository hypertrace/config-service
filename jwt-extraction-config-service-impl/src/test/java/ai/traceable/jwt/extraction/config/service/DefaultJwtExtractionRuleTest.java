package ai.traceable.jwt.extraction.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.jwt.extraction.config.service.v1.JwtExtractionRule;
import ai.traceable.jwt.extraction.config.service.v1.JwtLocation;
import ai.traceable.jwt.extraction.config.service.v1.JwtProcessingInstruction;
import ai.traceable.jwt.extraction.config.service.v1.Predicate;
import ai.traceable.jwt.extraction.config.service.v1.StringPredicate;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class DefaultJwtExtractionRuleTest {
  public static final JwtExtractionRule DEFAULT_RULE =
      JwtExtractionRule.newBuilder()
          .setId("default-jwt-auth-header-extraction")
          .setDefault(true)
          .setPredicate(Predicate.newBuilder().build())
          .addLocations(
              JwtLocation.newBuilder()
                  .setRequestHeader(
                      StringPredicate.newBuilder()
                          .setOperator(
                              StringPredicate.RelationalOperator.RELATIONAL_OPERATOR_EQUALS)
                          .setValue("authorization")
                          .build())
                  .setRegexCaptureGroup("^(?i)Bearer\\s*:?\\s*(.*)$")
                  .build())
          .addInstructions(
              JwtProcessingInstruction.newBuilder()
                  .setValueExtraction(
                      JwtProcessingInstruction.ValueExtraction.newBuilder()
                          .setPayloadClaimName("iss")
                          .build())
                  .setAction(
                      JwtProcessingInstruction.Action.newBuilder()
                          .setAddNewAttribute("iss")
                          .build()))
          .addInstructions(
              JwtProcessingInstruction.newBuilder()
                  .setValueExtraction(
                      JwtProcessingInstruction.ValueExtraction.newBuilder()
                          .setPayloadClaimName("aud")
                          .build())
                  .setAction(
                      JwtProcessingInstruction.Action.newBuilder()
                          .setAddNewAttribute("aud")
                          .build()))
          .addInstructions(
              JwtProcessingInstruction.newBuilder()
                  .setValueExtraction(
                      JwtProcessingInstruction.ValueExtraction.newBuilder()
                          .setPayloadClaimName("exp")
                          .build())
                  .setAction(
                      JwtProcessingInstruction.Action.newBuilder()
                          .setAddNewAttribute("exp")
                          .build()))
          .addInstructions(
              JwtProcessingInstruction.newBuilder()
                  .setValueExtraction(
                      JwtProcessingInstruction.ValueExtraction.newBuilder()
                          .setHeaderKey("alg")
                          .build())
                  .setAction(
                      JwtProcessingInstruction.Action.newBuilder()
                          .setAddNewAttribute("alg")
                          .build()))
          .build();

  @Test
  void loadsDefaultsSuccessfully() {
    Config config = ConfigFactory.parseResources("default-rules.conf");

    DefaultJwtExtractionRuleConfig defaultRuleConfig = new DefaultJwtExtractionRuleConfig(config);

    assertEquals(1, defaultRuleConfig.getAll().size());
    assertTrue(defaultRuleConfig.isDefaultConfig("default-jwt-auth-header-extraction"));
    assertFalse(defaultRuleConfig.isDefaultConfig(UUID.randomUUID().toString()));
    assertEquals(DEFAULT_RULE, defaultRuleConfig.getAll().iterator().next());
  }
}
