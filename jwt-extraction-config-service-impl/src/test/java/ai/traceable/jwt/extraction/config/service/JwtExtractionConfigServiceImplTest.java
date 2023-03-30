package ai.traceable.jwt.extraction.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.jwt.extraction.config.service.v1.CreateJwtExtractionRuleRequest;
import ai.traceable.jwt.extraction.config.service.v1.DeleteJwtExtractionRuleRequest;
import ai.traceable.jwt.extraction.config.service.v1.GetJwtExtractionRulesRequest;
import ai.traceable.jwt.extraction.config.service.v1.JwtExtractionConfigServiceGrpc;
import ai.traceable.jwt.extraction.config.service.v1.JwtExtractionConfigServiceGrpc.JwtExtractionConfigServiceBlockingStub;
import ai.traceable.jwt.extraction.config.service.v1.JwtExtractionRule;
import ai.traceable.jwt.extraction.config.service.v1.JwtExtractionRuleFilter;
import ai.traceable.jwt.extraction.config.service.v1.JwtExtractionRuleScope;
import ai.traceable.jwt.extraction.config.service.v1.JwtLocation;
import ai.traceable.jwt.extraction.config.service.v1.JwtProcessingInstruction;
import ai.traceable.jwt.extraction.config.service.v1.Predicate;
import ai.traceable.jwt.extraction.config.service.v1.StringPredicate;
import ai.traceable.jwt.extraction.config.service.v1.UpdateJwtExtractionRuleRequest;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.util.List;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class JwtExtractionConfigServiceImplTest {
  MockGenericConfigService mockGenericConfigService;
  @Mock ConfigChangeEventGenerator mockConfigChangeEventGenerator;
  @Mock JwtExtractionConfigRequestValidator mockValidator;
  @Mock UuidGenerator mockUuidGenerator;

  JwtExtractionConfigServiceBlockingStub stub;

  static final JwtExtractionRule SCOPED_RULE =
      JwtExtractionRule.newBuilder()
          .setId("scoped-rule")
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
                          .setAddNewAttribute("token.jwt.header.alg")
                          .build())
                  .setValueExtraction(
                      JwtProcessingInstruction.ValueExtraction.newBuilder()
                          .setHeaderKey("alg")
                          .setRawValue(
                              JwtProcessingInstruction.ValueExtraction.RawValue.newBuilder()
                                  .build())
                          .build())
                  .build())
          .setScope(
              JwtExtractionRuleScope.newBuilder()
                  .setEnvironmentScope(
                      JwtExtractionRuleScope.EnvironmentScope.newBuilder()
                          .addEnvironmentNames("env")
                          .build()))
          .build();

  static final JwtExtractionRule UNSCOPED_RULE =
      JwtExtractionRule.newBuilder()
          .setId("unscoped-rule")
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
                          .setAddNewAttribute("token.jwt.header.alg")
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

  JwtExtractionRule DEFAULT_RULE =
      JwtExtractionRule.newBuilder()
          .setId("f496d554-4685-4de4-a074-4186ee579a4c")
          .setDefault(true)
          .setPredicate(Predicate.newBuilder().build())
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
                          .setAddNewAttribute("token.jwt.header.role")
                          .build())
                  .setValueExtraction(
                      JwtProcessingInstruction.ValueExtraction.newBuilder()
                          .setPayloadClaimName("role")
                          .setRawValue(
                              JwtProcessingInstruction.ValueExtraction.RawValue.newBuilder()
                                  .build())
                          .build())
                  .build())
          .build();

  @BeforeEach
  void setUp() {
    mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    mockGenericConfigService
        .addService(
            new JwtExtractionConfigServiceImpl(
                mockValidator,
                new JwtExtractionRuleManager(
                    new DefaultJwtExtractionRuleConfig(
                        ConfigFactory.parseResources("default-rules.conf")),
                    new UserDefinedJwtExtractionRuleStore(
                        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel()),
                        mockConfigChangeEventGenerator),
                    new DeletedDefaultJwtExtractionRuleStore(
                        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel()),
                        mockConfigChangeEventGenerator)),
                new JwtExtractionConfigRuleBuilder(mockUuidGenerator)))
        .start();
    stub = JwtExtractionConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
  }

  @AfterEach
  void afterEach() {
    mockGenericConfigService.shutdown();
  }

  @Test
  void createReadUpdateDeleteTest() throws IOException {
    CreateJwtExtractionRuleRequest unscopedCreateRequest =
        CreateJwtExtractionRuleRequest.newBuilder()
            .setPredicate(UNSCOPED_RULE.getPredicate())
            .addAllLocations(UNSCOPED_RULE.getLocationsList())
            .addAllInstructions(UNSCOPED_RULE.getInstructionsList())
            .build();

    CreateJwtExtractionRuleRequest scopedCreateRequest =
        CreateJwtExtractionRuleRequest.newBuilder()
            .setPredicate(SCOPED_RULE.getPredicate())
            .addAllLocations(UNSCOPED_RULE.getLocationsList())
            .addAllInstructions(UNSCOPED_RULE.getInstructionsList())
            .setScope(SCOPED_RULE.getScope())
            .build();
    when(mockUuidGenerator.generateRandomId())
        .thenReturn(UNSCOPED_RULE.getId(), SCOPED_RULE.getId());

    assertEquals(UNSCOPED_RULE, this.stub.createJwtExtractionRule(unscopedCreateRequest).getRule());
    assertEquals(SCOPED_RULE, this.stub.createJwtExtractionRule(scopedCreateRequest).getRule());

    assertEquals(
        List.of(DEFAULT_RULE, SCOPED_RULE, UNSCOPED_RULE),
        this.stub
            .getJwtExtractionRules(GetJwtExtractionRulesRequest.getDefaultInstance())
            .getRulesList());
    assertEquals(
        List.of(DEFAULT_RULE, SCOPED_RULE, UNSCOPED_RULE),
        this.stub
            .getJwtExtractionRules(
                GetJwtExtractionRulesRequest.newBuilder()
                    .setFilter(
                        JwtExtractionRuleFilter.newBuilder()
                            .setScope(
                                JwtExtractionRuleScope.newBuilder()
                                    .setEnvironmentScope(
                                        SCOPED_RULE.getScope().getEnvironmentScope())))
                    .build())
            .getRulesList());
    assertEquals(
        List.of(DEFAULT_RULE, UNSCOPED_RULE),
        this.stub
            .getJwtExtractionRules(
                GetJwtExtractionRulesRequest.newBuilder()
                    .setFilter(
                        JwtExtractionRuleFilter.newBuilder()
                            .setScope(JwtExtractionRuleScope.getDefaultInstance()))
                    .build())
            .getRulesList());

    JwtExtractionRule expectedUpdatedRule =
        UNSCOPED_RULE.toBuilder()
            .setScope(
                JwtExtractionRuleScope.newBuilder()
                    .setEnvironmentScope(
                        SCOPED_RULE.getScope().getEnvironmentScope().toBuilder()
                            .addEnvironmentNames("other-env")
                            .build()))
            .build();

    UpdateJwtExtractionRuleRequest ruleUpdateRequest =
        UpdateJwtExtractionRuleRequest.newBuilder()
            .setId(expectedUpdatedRule.getId())
            .setPredicate(expectedUpdatedRule.getPredicate())
            .addAllLocations(UNSCOPED_RULE.getLocationsList())
            .addAllInstructions(UNSCOPED_RULE.getInstructionsList())
            .setScope(expectedUpdatedRule.getScope())
            .build();

    assertEquals(
        expectedUpdatedRule, this.stub.updateJwtExtractionRule(ruleUpdateRequest).getRule());

    assertEquals(
        List.of(DEFAULT_RULE, SCOPED_RULE, expectedUpdatedRule),
        this.stub
            .getJwtExtractionRules(
                GetJwtExtractionRulesRequest.newBuilder()
                    .setFilter(
                        JwtExtractionRuleFilter.newBuilder()
                            .setScope(
                                JwtExtractionRuleScope.newBuilder()
                                    .setEnvironmentScope(
                                        SCOPED_RULE.getScope().getEnvironmentScope())))
                    .build())
            .getRulesList());

    assertEquals(
        List.of(DEFAULT_RULE, expectedUpdatedRule),
        this.stub
            .getJwtExtractionRules(
                GetJwtExtractionRulesRequest.newBuilder()
                    .setFilter(
                        JwtExtractionRuleFilter.newBuilder()
                            .setScope(
                                JwtExtractionRuleScope.newBuilder()
                                    .setEnvironmentScope(
                                        JwtExtractionRuleScope.EnvironmentScope.newBuilder()
                                            .addEnvironmentNames("other-env")
                                            .build())))
                    .build())
            .getRulesList());

    this.stub.deleteJwtExtractionRule(
        DeleteJwtExtractionRuleRequest.newBuilder().setId(SCOPED_RULE.getId()).build());

    assertEquals(
        List.of(DEFAULT_RULE, expectedUpdatedRule),
        this.stub
            .getJwtExtractionRules(
                GetJwtExtractionRulesRequest.newBuilder()
                    .setFilter(
                        JwtExtractionRuleFilter.newBuilder()
                            .setScope(
                                JwtExtractionRuleScope.newBuilder()
                                    .setEnvironmentScope(
                                        SCOPED_RULE.getScope().getEnvironmentScope())))
                    .build())
            .getRulesList());

    this.stub.deleteJwtExtractionRule(
        DeleteJwtExtractionRuleRequest.newBuilder().setId(expectedUpdatedRule.getId()).build());

    JwtExtractionRule modifiedDefaultRule =
        DEFAULT_RULE.toBuilder()
            .setScope(
                JwtExtractionRuleScope.newBuilder()
                    .setEnvironmentScope(
                        JwtExtractionRuleScope.EnvironmentScope.newBuilder()
                            .addEnvironmentNames("other-env")
                            .build()))
            .setDefault(false)
            .build();
    assertEquals(
        modifiedDefaultRule,
        this.stub
            .updateJwtExtractionRule(
                UpdateJwtExtractionRuleRequest.newBuilder()
                    .setId(DEFAULT_RULE.getId())
                    .setPredicate(DEFAULT_RULE.getPredicate())
                    .addAllLocations(DEFAULT_RULE.getLocationsList())
                    .addAllInstructions(DEFAULT_RULE.getInstructionsList())
                    .setScope(modifiedDefaultRule.getScope())
                    .build())
            .getRule());

    assertEquals(
        List.of(modifiedDefaultRule),
        this.stub
            .getJwtExtractionRules(GetJwtExtractionRulesRequest.getDefaultInstance())
            .getRulesList());

    this.stub.deleteJwtExtractionRule(
        DeleteJwtExtractionRuleRequest.newBuilder().setId(modifiedDefaultRule.getId()).build());

    assertEquals(
        List.of(),
        this.stub
            .getJwtExtractionRules(GetJwtExtractionRulesRequest.getDefaultInstance())
            .getRulesList());
  }
}
