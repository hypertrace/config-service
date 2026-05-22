package ai.traceable.genai.system.discovery.config.service.v1.validation;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import ai.traceable.genai.system.discovery.config.service.v1.CompositeCondition;
import ai.traceable.genai.system.discovery.config.service.v1.Condition;
import ai.traceable.genai.system.discovery.config.service.v1.CreateGenAiSystemDiscoveryRuleRequest;
import ai.traceable.genai.system.discovery.config.service.v1.DeleteGenAiSystemDiscoveryRuleRequest;
import ai.traceable.genai.system.discovery.config.service.v1.DynamicNameExtractionAction;
import ai.traceable.genai.system.discovery.config.service.v1.EntityPathReference;
import ai.traceable.genai.system.discovery.config.service.v1.ExtractionAction;
import ai.traceable.genai.system.discovery.config.service.v1.GenAiInfoExtractionAction;
import ai.traceable.genai.system.discovery.config.service.v1.GenAiNameExtractionAction;
import ai.traceable.genai.system.discovery.config.service.v1.GenAiSystemDiscoveryRule;
import ai.traceable.genai.system.discovery.config.service.v1.GenAiSystemDiscoveryRuleData;
import ai.traceable.genai.system.discovery.config.service.v1.GetGenAiSystemDiscoveryRulesRequest;
import ai.traceable.genai.system.discovery.config.service.v1.KeyValueCondition;
import ai.traceable.genai.system.discovery.config.service.v1.LeafCondition;
import ai.traceable.genai.system.discovery.config.service.v1.MatchCondition;
import ai.traceable.genai.system.discovery.config.service.v1.MatchGroupOperation;
import ai.traceable.genai.system.discovery.config.service.v1.NoOperation;
import ai.traceable.genai.system.discovery.config.service.v1.Operator;
import ai.traceable.genai.system.discovery.config.service.v1.ReferenceCondition;
import ai.traceable.genai.system.discovery.config.service.v1.ReferenceExtractionAction;
import ai.traceable.genai.system.discovery.config.service.v1.StaticNameAction;
import ai.traceable.genai.system.discovery.config.service.v1.UpdateGenAiSystemDiscoveryRuleRequest;
import io.grpc.StatusRuntimeException;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GenAiSystemDiscoveryRulesValidatorImplTest {

  private GenAiSystemDiscoveryRulesValidatorImpl validator;

  @Mock private RequestContext requestContext;

  @BeforeEach
  void setUp() {
    validator = new GenAiSystemDiscoveryRulesValidatorImpl();
    when(requestContext.getTenantId()).thenReturn(java.util.Optional.of("test-tenant"));
  }

  @Test
  void testValidateGetGenAiSystemDiscoveryRulesRequest_Valid() {
    GetGenAiSystemDiscoveryRulesRequest request =
        GetGenAiSystemDiscoveryRulesRequest.newBuilder().build();
    assertDoesNotThrow(() -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void testValidateGetGenAiSystemDiscoveryRulesRequest_InvalidContext() {
    when(requestContext.getTenantId()).thenReturn(java.util.Optional.empty());
    GetGenAiSystemDiscoveryRulesRequest request =
        GetGenAiSystemDiscoveryRulesRequest.newBuilder().build();
    assertThrows(
        StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void testValidateCreateGenAiSystemDiscoveryRuleRequest_Valid() {
    CreateGenAiSystemDiscoveryRuleRequest request = createValidCreateRequest();
    assertDoesNotThrow(() -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void testValidateCreateGenAiSystemDiscoveryRuleRequest_MissingName() {
    GenAiSystemDiscoveryRuleData ruleData =
        GenAiSystemDiscoveryRuleData.newBuilder()
            .setDescription("Test description")
            .setCondition(createValidCondition())
            .setGenAiInfoExtractionAction(createValidGenAiInfoExtractionAction())
            .build();
    CreateGenAiSystemDiscoveryRuleRequest request =
        CreateGenAiSystemDiscoveryRuleRequest.newBuilder()
            .setGenAiSystemDiscoveryRuleData(ruleData)
            .build();
    assertThrows(
        StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void testValidateUpdateGenAiSystemDiscoveryRuleRequest_Valid() {
    UpdateGenAiSystemDiscoveryRuleRequest request =
        UpdateGenAiSystemDiscoveryRuleRequest.newBuilder()
            .setGenAiSystemDiscoveryRule(createValidGenAiSystemDiscoveryRule())
            .build();
    assertDoesNotThrow(() -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void testValidateUpdateGenAiSystemDiscoveryRuleRequest_MissingRule() {
    UpdateGenAiSystemDiscoveryRuleRequest request =
        UpdateGenAiSystemDiscoveryRuleRequest.newBuilder().build();
    assertThrows(
        StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void testValidateDeleteGenAiSystemDiscoveryRuleRequest_Valid() {
    DeleteGenAiSystemDiscoveryRuleRequest request =
        DeleteGenAiSystemDiscoveryRuleRequest.newBuilder().setRuleId("rule-123").build();
    assertDoesNotThrow(() -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void testValidateDeleteGenAiSystemDiscoveryRuleRequest_MissingRuleId() {
    DeleteGenAiSystemDiscoveryRuleRequest request =
        DeleteGenAiSystemDiscoveryRuleRequest.newBuilder().build();
    assertThrows(
        StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void testValidateGenAiSystemDiscoveryRule_MissingRuleId() {
    GenAiSystemDiscoveryRule rule =
        GenAiSystemDiscoveryRule.newBuilder()
            .setGenAiSystemDiscoveryRuleData(createValidGenAiSystemDiscoveryRuleData())
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateOrThrow(
                requestContext,
                UpdateGenAiSystemDiscoveryRuleRequest.newBuilder()
                    .setGenAiSystemDiscoveryRule(rule)
                    .build()));
  }

  @Test
  void testValidateCondition_InvalidConditionCase() {
    Condition condition = Condition.newBuilder().build();
    GenAiSystemDiscoveryRuleData ruleData =
        GenAiSystemDiscoveryRuleData.newBuilder()
            .setName("Test Rule")
            .setDescription("Test Description")
            .setCondition(condition)
            .setGenAiInfoExtractionAction(createValidGenAiInfoExtractionAction())
            .build();
    CreateGenAiSystemDiscoveryRuleRequest request =
        CreateGenAiSystemDiscoveryRuleRequest.newBuilder()
            .setGenAiSystemDiscoveryRuleData(ruleData)
            .build();
    assertThrows(
        StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void testValidateLeafCondition_ValidUrlCondition() {
    LeafCondition leafCondition =
        LeafCondition.newBuilder().setUrlCondition(createValidMatchCondition()).build();
    Condition condition = Condition.newBuilder().setLeafCondition(leafCondition).build();
    GenAiSystemDiscoveryRuleData ruleData =
        createValidGenAiSystemDiscoveryRuleData().toBuilder().setCondition(condition).build();
    CreateGenAiSystemDiscoveryRuleRequest request =
        CreateGenAiSystemDiscoveryRuleRequest.newBuilder()
            .setGenAiSystemDiscoveryRuleData(ruleData)
            .build();
    assertDoesNotThrow(() -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void testValidateLeafCondition_InvalidConditionCase() {
    LeafCondition leafCondition = LeafCondition.newBuilder().build();
    Condition condition = Condition.newBuilder().setLeafCondition(leafCondition).build();
    GenAiSystemDiscoveryRuleData ruleData =
        createValidGenAiSystemDiscoveryRuleData().toBuilder().setCondition(condition).build();
    CreateGenAiSystemDiscoveryRuleRequest request =
        CreateGenAiSystemDiscoveryRuleRequest.newBuilder()
            .setGenAiSystemDiscoveryRuleData(ruleData)
            .build();
    assertThrows(
        StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void testValidateMatchCondition_MissingOperator() {
    MatchCondition matchCondition = MatchCondition.newBuilder().setValue("test").build();
    LeafCondition leafCondition =
        LeafCondition.newBuilder().setUrlCondition(matchCondition).build();
    Condition condition = Condition.newBuilder().setLeafCondition(leafCondition).build();
    GenAiSystemDiscoveryRuleData ruleData =
        createValidGenAiSystemDiscoveryRuleData().toBuilder().setCondition(condition).build();
    CreateGenAiSystemDiscoveryRuleRequest request =
        CreateGenAiSystemDiscoveryRuleRequest.newBuilder()
            .setGenAiSystemDiscoveryRuleData(ruleData)
            .build();
    assertThrows(
        StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void testValidateMatchCondition_BlankValue() {
    MatchCondition matchCondition =
        MatchCondition.newBuilder().setOperator(Operator.OPERATOR_EQUALS).setValue("").build();
    LeafCondition leafCondition =
        LeafCondition.newBuilder().setUrlCondition(matchCondition).build();
    Condition condition = Condition.newBuilder().setLeafCondition(leafCondition).build();
    GenAiSystemDiscoveryRuleData ruleData =
        createValidGenAiSystemDiscoveryRuleData().toBuilder().setCondition(condition).build();
    CreateGenAiSystemDiscoveryRuleRequest request =
        CreateGenAiSystemDiscoveryRuleRequest.newBuilder()
            .setGenAiSystemDiscoveryRuleData(ruleData)
            .build();
    assertThrows(
        StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void testValidateKeyValueCondition_MissingKeyMatchCondition() {
    KeyValueCondition attributeCondition =
        KeyValueCondition.newBuilder().setValueMatchCondition(createValidMatchCondition()).build();
    LeafCondition leafCondition =
        LeafCondition.newBuilder().setAttributeCondition(attributeCondition).build();
    Condition condition = Condition.newBuilder().setLeafCondition(leafCondition).build();
    GenAiSystemDiscoveryRuleData ruleData =
        createValidGenAiSystemDiscoveryRuleData().toBuilder().setCondition(condition).build();
    CreateGenAiSystemDiscoveryRuleRequest request =
        CreateGenAiSystemDiscoveryRuleRequest.newBuilder()
            .setGenAiSystemDiscoveryRuleData(ruleData)
            .build();
    assertThrows(
        StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void testValidateCompositeCondition_MissingOperator() {
    CompositeCondition compositeCondition =
        CompositeCondition.newBuilder().addChildren(createValidCondition()).build();
    Condition condition = Condition.newBuilder().setCompositeCondition(compositeCondition).build();
    GenAiSystemDiscoveryRuleData ruleData =
        createValidGenAiSystemDiscoveryRuleData().toBuilder().setCondition(condition).build();
    CreateGenAiSystemDiscoveryRuleRequest request =
        CreateGenAiSystemDiscoveryRuleRequest.newBuilder()
            .setGenAiSystemDiscoveryRuleData(ruleData)
            .build();
    assertThrows(
        StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void testValidateCompositeCondition_MissingChildren() {
    CompositeCondition compositeCondition =
        CompositeCondition.newBuilder()
            .setOperator(CompositeCondition.LogicalOperator.LOGICAL_OPERATOR_AND)
            .build();
    Condition condition = Condition.newBuilder().setCompositeCondition(compositeCondition).build();
    GenAiSystemDiscoveryRuleData ruleData =
        createValidGenAiSystemDiscoveryRuleData().toBuilder().setCondition(condition).build();
    CreateGenAiSystemDiscoveryRuleRequest request =
        CreateGenAiSystemDiscoveryRuleRequest.newBuilder()
            .setGenAiSystemDiscoveryRuleData(ruleData)
            .build();
    assertThrows(
        StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void testValidateGenAiInfoExtractionAction_Valid() {
    GenAiInfoExtractionAction action = createValidGenAiInfoExtractionAction();
    GenAiSystemDiscoveryRuleData ruleData =
        createValidGenAiSystemDiscoveryRuleData().toBuilder()
            .setGenAiInfoExtractionAction(action)
            .build();
    CreateGenAiSystemDiscoveryRuleRequest request =
        CreateGenAiSystemDiscoveryRuleRequest.newBuilder()
            .setGenAiSystemDiscoveryRuleData(ruleData)
            .build();
    assertDoesNotThrow(() -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void testValidateStaticNameAction_BlankValue() {
    StaticNameAction staticNameAction = StaticNameAction.newBuilder().setValue("").build();
    GenAiNameExtractionAction action =
        GenAiNameExtractionAction.newBuilder().setStaticNameAction(staticNameAction).build();
    GenAiInfoExtractionAction infoAction =
        GenAiInfoExtractionAction.newBuilder()
            .setProviderAction(action)
            .setModelAction(action)
            .build();
    GenAiSystemDiscoveryRuleData ruleData =
        createValidGenAiSystemDiscoveryRuleData().toBuilder()
            .setGenAiInfoExtractionAction(infoAction)
            .build();
    CreateGenAiSystemDiscoveryRuleRequest request =
        CreateGenAiSystemDiscoveryRuleRequest.newBuilder()
            .setGenAiSystemDiscoveryRuleData(ruleData)
            .build();
    assertThrows(
        StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void testValidateDynamicNameExtractionAction_InvalidCase() {
    DynamicNameExtractionAction action = DynamicNameExtractionAction.newBuilder().build();
    GenAiNameExtractionAction nameAction =
        GenAiNameExtractionAction.newBuilder().setDynamicNameExtractionAction(action).build();
    GenAiInfoExtractionAction infoAction =
        GenAiInfoExtractionAction.newBuilder()
            .setProviderAction(nameAction)
            .setModelAction(nameAction)
            .build();
    GenAiSystemDiscoveryRuleData ruleData =
        createValidGenAiSystemDiscoveryRuleData().toBuilder()
            .setGenAiInfoExtractionAction(infoAction)
            .build();
    CreateGenAiSystemDiscoveryRuleRequest request =
        CreateGenAiSystemDiscoveryRuleRequest.newBuilder()
            .setGenAiSystemDiscoveryRuleData(ruleData)
            .build();
    assertThrows(
        StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void testValidateExtractionAction_BlankKey() {
    ExtractionAction extractionAction =
        ExtractionAction.newBuilder()
            .setKey("")
            .setNoOperation(NoOperation.newBuilder().build())
            .build();
    DynamicNameExtractionAction dynamicAction =
        DynamicNameExtractionAction.newBuilder()
            .setAttributeExtractionAction(extractionAction)
            .build();
    GenAiNameExtractionAction nameAction =
        GenAiNameExtractionAction.newBuilder()
            .setDynamicNameExtractionAction(dynamicAction)
            .build();
    GenAiInfoExtractionAction infoAction =
        GenAiInfoExtractionAction.newBuilder().setModelAction(nameAction).build();
    GenAiSystemDiscoveryRuleData ruleData =
        createValidGenAiSystemDiscoveryRuleData().toBuilder()
            .setGenAiInfoExtractionAction(infoAction)
            .build();
    CreateGenAiSystemDiscoveryRuleRequest request =
        CreateGenAiSystemDiscoveryRuleRequest.newBuilder()
            .setGenAiSystemDiscoveryRuleData(ruleData)
            .build();
    assertThrows(
        StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void testValidateMatchGroupOperation_BlankMatchGroup() {
    MatchGroupOperation matchGroupOperation =
        MatchGroupOperation.newBuilder().setMatchGroupRegex(".*").setMatchGroup("").build();
    ExtractionAction extractionAction =
        ExtractionAction.newBuilder()
            .setKey("test-key")
            .setMatchGroupOperation(matchGroupOperation)
            .build();
    DynamicNameExtractionAction dynamicAction =
        DynamicNameExtractionAction.newBuilder()
            .setAttributeExtractionAction(extractionAction)
            .build();
    GenAiNameExtractionAction nameAction =
        GenAiNameExtractionAction.newBuilder()
            .setDynamicNameExtractionAction(dynamicAction)
            .build();
    GenAiInfoExtractionAction infoAction =
        GenAiInfoExtractionAction.newBuilder()
            .setProviderAction(nameAction)
            .setModelAction(nameAction)
            .build();
    GenAiSystemDiscoveryRuleData ruleData =
        createValidGenAiSystemDiscoveryRuleData().toBuilder()
            .setGenAiInfoExtractionAction(infoAction)
            .build();
    CreateGenAiSystemDiscoveryRuleRequest request =
        CreateGenAiSystemDiscoveryRuleRequest.newBuilder()
            .setGenAiSystemDiscoveryRuleData(ruleData)
            .build();
    assertThrows(
        StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void testValidateMatchGroupOperation_BlankRegex() {
    MatchGroupOperation matchGroupOperation =
        MatchGroupOperation.newBuilder().setMatchGroupRegex("").setMatchGroup("group1").build();
    ExtractionAction extractionAction =
        ExtractionAction.newBuilder()
            .setKey("test-key")
            .setMatchGroupOperation(matchGroupOperation)
            .build();
    DynamicNameExtractionAction dynamicAction =
        DynamicNameExtractionAction.newBuilder()
            .setAttributeExtractionAction(extractionAction)
            .build();
    GenAiNameExtractionAction nameAction =
        GenAiNameExtractionAction.newBuilder()
            .setDynamicNameExtractionAction(dynamicAction)
            .build();
    GenAiInfoExtractionAction infoAction =
        GenAiInfoExtractionAction.newBuilder()
            .setProviderAction(nameAction)
            .setModelAction(nameAction)
            .build();
    GenAiSystemDiscoveryRuleData ruleData =
        createValidGenAiSystemDiscoveryRuleData().toBuilder()
            .setGenAiInfoExtractionAction(infoAction)
            .build();
    CreateGenAiSystemDiscoveryRuleRequest request =
        CreateGenAiSystemDiscoveryRuleRequest.newBuilder()
            .setGenAiSystemDiscoveryRuleData(ruleData)
            .build();
    assertThrows(
        StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void validateReferenceCondition_nonBlankReferenceAndValue_shouldPass() {
    ReferenceCondition referenceCondition =
        ReferenceCondition.newBuilder()
            .setReference(
                EntityPathReference.newBuilder()
                    .setEntityType("API")
                    .setPath("aiModelLocation")
                    .build())
            .setValueMatch(
                MatchCondition.newBuilder()
                    .setOperator(Operator.OPERATOR_STARTS_WITH)
                    .setValue("gpt")
                    .build())
            .build();
    LeafCondition leafCondition =
        LeafCondition.newBuilder().setReferenceCondition(referenceCondition).build();
    Condition condition = Condition.newBuilder().setLeafCondition(leafCondition).build();
    GenAiSystemDiscoveryRuleData ruleData =
        createValidGenAiSystemDiscoveryRuleData().toBuilder().setCondition(condition).build();
    CreateGenAiSystemDiscoveryRuleRequest request =
        CreateGenAiSystemDiscoveryRuleRequest.newBuilder()
            .setGenAiSystemDiscoveryRuleData(ruleData)
            .build();
    assertDoesNotThrow(() -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void validateReferenceCondition_blankReferenceEntityType_shouldThrow() {
    ReferenceCondition referenceCondition =
        ReferenceCondition.newBuilder()
            .setReference(
                EntityPathReference.newBuilder()
                    .setEntityType("")
                    .setPath("aiModelLocation")
                    .build())
            .setValueMatch(
                MatchCondition.newBuilder()
                    .setOperator(Operator.OPERATOR_STARTS_WITH)
                    .setValue("gpt")
                    .build())
            .build();
    LeafCondition leafCondition =
        LeafCondition.newBuilder().setReferenceCondition(referenceCondition).build();
    Condition condition = Condition.newBuilder().setLeafCondition(leafCondition).build();
    GenAiSystemDiscoveryRuleData ruleData =
        createValidGenAiSystemDiscoveryRuleData().toBuilder().setCondition(condition).build();
    CreateGenAiSystemDiscoveryRuleRequest request =
        CreateGenAiSystemDiscoveryRuleRequest.newBuilder()
            .setGenAiSystemDiscoveryRuleData(ruleData)
            .build();
    assertThrows(
        StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void validateReferenceCondition_blankReferencePath_shouldThrow() {
    ReferenceCondition referenceCondition =
        ReferenceCondition.newBuilder()
            .setReference(EntityPathReference.newBuilder().setEntityType("API").setPath("").build())
            .setValueMatch(
                MatchCondition.newBuilder()
                    .setOperator(Operator.OPERATOR_STARTS_WITH)
                    .setValue("gpt")
                    .build())
            .build();
    LeafCondition leafCondition =
        LeafCondition.newBuilder().setReferenceCondition(referenceCondition).build();
    Condition condition = Condition.newBuilder().setLeafCondition(leafCondition).build();
    GenAiSystemDiscoveryRuleData ruleData =
        createValidGenAiSystemDiscoveryRuleData().toBuilder().setCondition(condition).build();
    CreateGenAiSystemDiscoveryRuleRequest request =
        CreateGenAiSystemDiscoveryRuleRequest.newBuilder()
            .setGenAiSystemDiscoveryRuleData(ruleData)
            .build();
    assertThrows(
        StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void validateReferenceCondition_blankValueMatchValue_shouldThrow() {
    ReferenceCondition referenceCondition =
        ReferenceCondition.newBuilder()
            .setReference(
                EntityPathReference.newBuilder()
                    .setEntityType("API")
                    .setPath("aiModelLocation")
                    .build())
            .setValueMatch(
                MatchCondition.newBuilder()
                    .setOperator(Operator.OPERATOR_STARTS_WITH)
                    .setValue("")
                    .build())
            .build();
    LeafCondition leafCondition =
        LeafCondition.newBuilder().setReferenceCondition(referenceCondition).build();
    Condition condition = Condition.newBuilder().setLeafCondition(leafCondition).build();
    GenAiSystemDiscoveryRuleData ruleData =
        createValidGenAiSystemDiscoveryRuleData().toBuilder().setCondition(condition).build();
    CreateGenAiSystemDiscoveryRuleRequest request =
        CreateGenAiSystemDiscoveryRuleRequest.newBuilder()
            .setGenAiSystemDiscoveryRuleData(ruleData)
            .build();
    assertThrows(
        StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void validateReferenceExtractionAction_nonBlankReference_shouldPass() {
    ReferenceExtractionAction referenceExtractionAction =
        ReferenceExtractionAction.newBuilder()
            .setReference(
                EntityPathReference.newBuilder()
                    .setEntityType("API")
                    .setPath("aiModelLocation")
                    .build())
            .build();
    DynamicNameExtractionAction dynamicAction =
        DynamicNameExtractionAction.newBuilder()
            .setReferenceExtractionAction(referenceExtractionAction)
            .build();
    GenAiNameExtractionAction nameAction =
        GenAiNameExtractionAction.newBuilder()
            .setDynamicNameExtractionAction(dynamicAction)
            .build();
    GenAiNameExtractionAction providerAction =
        GenAiNameExtractionAction.newBuilder()
            .setStaticNameAction(StaticNameAction.newBuilder().setValue("test-provider").build())
            .build();
    GenAiInfoExtractionAction infoAction =
        GenAiInfoExtractionAction.newBuilder()
            .setProviderAction(providerAction)
            .setModelAction(nameAction)
            .build();
    GenAiSystemDiscoveryRuleData ruleData =
        createValidGenAiSystemDiscoveryRuleData().toBuilder()
            .setGenAiInfoExtractionAction(infoAction)
            .build();
    CreateGenAiSystemDiscoveryRuleRequest request =
        CreateGenAiSystemDiscoveryRuleRequest.newBuilder()
            .setGenAiSystemDiscoveryRuleData(ruleData)
            .build();
    assertDoesNotThrow(() -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void validateReferenceExtractionAction_blankReferenceEntityType_shouldThrow() {
    ReferenceExtractionAction referenceExtractionAction =
        ReferenceExtractionAction.newBuilder()
            .setReference(
                EntityPathReference.newBuilder()
                    .setEntityType("")
                    .setPath("aiModelLocation")
                    .build())
            .build();
    DynamicNameExtractionAction dynamicAction =
        DynamicNameExtractionAction.newBuilder()
            .setReferenceExtractionAction(referenceExtractionAction)
            .build();
    GenAiNameExtractionAction nameAction =
        GenAiNameExtractionAction.newBuilder()
            .setDynamicNameExtractionAction(dynamicAction)
            .build();
    GenAiInfoExtractionAction infoAction =
        GenAiInfoExtractionAction.newBuilder().setModelAction(nameAction).build();
    GenAiSystemDiscoveryRuleData ruleData =
        createValidGenAiSystemDiscoveryRuleData().toBuilder()
            .setGenAiInfoExtractionAction(infoAction)
            .build();
    CreateGenAiSystemDiscoveryRuleRequest request =
        CreateGenAiSystemDiscoveryRuleRequest.newBuilder()
            .setGenAiSystemDiscoveryRuleData(ruleData)
            .build();
    assertThrows(
        StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));
  }

  @Test
  void validateReferenceExtractionAction_blankReferencePath_shouldThrow() {
    ReferenceExtractionAction referenceExtractionAction =
        ReferenceExtractionAction.newBuilder()
            .setReference(EntityPathReference.newBuilder().setEntityType("API").setPath("").build())
            .build();
    DynamicNameExtractionAction dynamicAction =
        DynamicNameExtractionAction.newBuilder()
            .setReferenceExtractionAction(referenceExtractionAction)
            .build();
    GenAiNameExtractionAction nameAction =
        GenAiNameExtractionAction.newBuilder()
            .setDynamicNameExtractionAction(dynamicAction)
            .build();
    GenAiInfoExtractionAction infoAction =
        GenAiInfoExtractionAction.newBuilder().setModelAction(nameAction).build();
    GenAiSystemDiscoveryRuleData ruleData =
        createValidGenAiSystemDiscoveryRuleData().toBuilder()
            .setGenAiInfoExtractionAction(infoAction)
            .build();
    CreateGenAiSystemDiscoveryRuleRequest request =
        CreateGenAiSystemDiscoveryRuleRequest.newBuilder()
            .setGenAiSystemDiscoveryRuleData(ruleData)
            .build();
    assertThrows(
        StatusRuntimeException.class, () -> validator.validateOrThrow(requestContext, request));
  }

  private CreateGenAiSystemDiscoveryRuleRequest createValidCreateRequest() {
    return CreateGenAiSystemDiscoveryRuleRequest.newBuilder()
        .setGenAiSystemDiscoveryRuleData(createValidGenAiSystemDiscoveryRuleData())
        .build();
  }

  private GenAiSystemDiscoveryRule createValidGenAiSystemDiscoveryRule() {
    return GenAiSystemDiscoveryRule.newBuilder()
        .setRuleId("rule-123")
        .setGenAiSystemDiscoveryRuleData(createValidGenAiSystemDiscoveryRuleData())
        .build();
  }

  private GenAiSystemDiscoveryRuleData createValidGenAiSystemDiscoveryRuleData() {
    return GenAiSystemDiscoveryRuleData.newBuilder()
        .setName("Test Rule")
        .setDescription("Test Description")
        .setCondition(createValidCondition())
        .setGenAiInfoExtractionAction(createValidGenAiInfoExtractionAction())
        .build();
  }

  private Condition createValidCondition() {
    return Condition.newBuilder().setLeafCondition(createValidLeafCondition()).build();
  }

  private LeafCondition createValidLeafCondition() {
    return LeafCondition.newBuilder().setUrlCondition(createValidMatchCondition()).build();
  }

  private MatchCondition createValidMatchCondition() {
    return MatchCondition.newBuilder()
        .setOperator(Operator.OPERATOR_EQUALS)
        .setValue("test-value")
        .build();
  }

  private GenAiInfoExtractionAction createValidGenAiInfoExtractionAction() {
    GenAiNameExtractionAction nameAction =
        GenAiNameExtractionAction.newBuilder()
            .setStaticNameAction(StaticNameAction.newBuilder().setValue("test-provider").build())
            .build();
    return GenAiInfoExtractionAction.newBuilder()
        .setProviderAction(nameAction)
        .setModelAction(nameAction)
        .build();
  }
}
