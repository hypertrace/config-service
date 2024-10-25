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
import java.util.Iterator;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class DefaultJwtExtractionRuleTest {
  public static final JwtExtractionRule DEFAULT_RULE_1 =
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
                          .setPayloadClaimName("nbf")
                          .build())
                  .setAction(
                      JwtProcessingInstruction.Action.newBuilder()
                          .setAddNewAttribute("nbf")
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

  public static final JwtExtractionRule DEFAULT_RULE_2 =
      JwtExtractionRule.newBuilder()
          .setId("default-jwt-amzn-oidc-data-extraction")
          .setDefault(true)
          .setPredicate(Predicate.newBuilder().build())
          .addLocations(
              JwtLocation.newBuilder()
                  .setRequestHeader(
                      StringPredicate.newBuilder()
                          .setOperator(
                              StringPredicate.RelationalOperator.RELATIONAL_OPERATOR_EQUALS)
                          .setValue("x-amzn-oidc-data")
                          .build())
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
                          .setPayloadClaimName("nbf")
                          .build())
                  .setAction(
                      JwtProcessingInstruction.Action.newBuilder()
                          .setAddNewAttribute("nbf")
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
          .addInstructions(
              JwtProcessingInstruction.newBuilder()
                  .setValueExtraction(
                      JwtProcessingInstruction.ValueExtraction.newBuilder()
                          .setHeaderKey("signer")
                          .build())
                  .setAction(
                      JwtProcessingInstruction.Action.newBuilder()
                          .setAddNewAttribute("signer")
                          .build()))
          .build();

  @Test
  void loadsDefaultsSuccessfully() {
    Config config = ConfigFactory.parseResources("default-rules.conf");

    DefaultJwtExtractionRuleConfig defaultRuleConfig = new DefaultJwtExtractionRuleConfig(config);

    assertEquals(2, defaultRuleConfig.getAll().size());
    assertTrue(defaultRuleConfig.isDefaultConfig("default-jwt-auth-header-extraction"));
    assertFalse(defaultRuleConfig.isDefaultConfig(UUID.randomUUID().toString()));
    Iterator<JwtExtractionRule> iterator = defaultRuleConfig.getAll().iterator();
    assertEquals(DEFAULT_RULE_1, iterator.next());
    assertEquals(DEFAULT_RULE_2, iterator.next());
  }
}
