package ai.traceable.span.processing.config.service.servicenaming;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.span.processing.config.service.v1.CreateServiceNamingRuleRequest;
import ai.traceable.span.processing.config.service.v1.ServiceNamingRule;
import ai.traceable.span.processing.config.service.v1.ServiceNamingRuleAction;
import ai.traceable.span.processing.config.service.v1.ServiceNamingRuleAction.StaticNameAssignment;
import ai.traceable.span.processing.config.service.v1.ServiceNamingRuleCondition;
import ai.traceable.span.processing.config.service.v1.ServiceNamingRuleCondition.AttributeCondition;
import ai.traceable.span.processing.config.service.v1.ServiceNamingRuleCondition.ConditionMatchOperator;
import ai.traceable.span.processing.config.service.v1.ServiceNamingRuleScope;
import ai.traceable.span.processing.config.service.v1.ServiceNamingRuleScope.EnvironmentScope;
import ai.traceable.span.processing.config.service.v1.UpdateServiceNamingRuleRequest;
import com.google.protobuf.util.Timestamps;
import java.text.ParseException;
import java.time.Instant;
import java.util.List;
import org.hypertrace.config.objectstore.ConfigObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ServiceNamingRuleBuilderTest {
  private static final ServiceNamingRule BASIC_RULE =
      ServiceNamingRule.newBuilder()
          .setId("rule-id")
          .setName("naming rule")
          .setDescription("rule description")
          .setRank(5)
          .setScope(
              ServiceNamingRuleScope.newBuilder()
                  .setEnvironmentScope(
                      EnvironmentScope.newBuilder().addEnvironmentNames("first-env")))
          .setEnabled(true)
          .addConditions(
              ServiceNamingRuleCondition.newBuilder()
                  .setAttributeCondition(
                      AttributeCondition.newBuilder()
                          .setAttributeKey("attr-key")
                          .setOperator(ConditionMatchOperator.CONDITION_MATCH_OPERATOR_EQUALS)
                          .setValue("val")))
          .setAction(
              ServiceNamingRuleAction.newBuilder()
                  .setStaticNameAssignment(
                      StaticNameAssignment.newBuilder().setServiceName("custom-name")))
          .build();
  @Mock UuidGenerator mockUuidGenerator;
  @Mock ConfigObject<ServiceNamingRule> mockConfigObject;
  @InjectMocks ServiceNamingRuleBuilder ruleBuilder;

  @Test
  void buildNewRule() {
    when(mockUuidGenerator.generateRandomId()).thenReturn(BASIC_RULE.getId());
    assertEquals(
        BASIC_RULE.toBuilder().setRank(1).build(), // As first rule, we'd expect rank 1
        this.ruleBuilder.buildNewRule(
            List.of(),
            CreateServiceNamingRuleRequest.newBuilder()
                .setName(BASIC_RULE.getName())
                .setDescription(BASIC_RULE.getDescription())
                .setScope(BASIC_RULE.getScope())
                .setEnabled(BASIC_RULE.getEnabled())
                .addAllConditions(BASIC_RULE.getConditionsList())
                .setAction(BASIC_RULE.getAction())
                .build()));
  }

  @Test
  void buildUpdatedRule() {
    assertEquals(
        BASIC_RULE.toBuilder().clearDescription().clearScope().build(),
        this.ruleBuilder.buildUpdatedRule(
            BASIC_RULE.toBuilder().setName("other").setEnabled(false).build(),
            UpdateServiceNamingRuleRequest.newBuilder()
                .setId(BASIC_RULE.getId())
                .setName(BASIC_RULE.getName())
                .setEnabled(BASIC_RULE.getEnabled())
                .addAllConditions(BASIC_RULE.getConditionsList())
                .setAction(BASIC_RULE.getAction())
                .build()));
  }

  @Test
  void buildRuleWithTimestamps() throws ParseException {
    String createTime = "2022-06-05T10:00:20.021Z";
    String updateTime = "2022-09-03T16:00:20.021Z";
    when(mockConfigObject.getData()).thenReturn(BASIC_RULE);
    when(mockConfigObject.getCreationTimestamp()).thenReturn(Instant.parse(createTime));
    when(mockConfigObject.getLastUpdatedTimestamp()).thenReturn(Instant.parse(updateTime));
    assertEquals(
        BASIC_RULE.toBuilder()
            .setCreationTimestamp(Timestamps.parse(createTime))
            .setLastUpdatedTimestamp(Timestamps.parse(updateTime))
            .build(),
        this.ruleBuilder.buildFromConfigObject(mockConfigObject));
  }
}
