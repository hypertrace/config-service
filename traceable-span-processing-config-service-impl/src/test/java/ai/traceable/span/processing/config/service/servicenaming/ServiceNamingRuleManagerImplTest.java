package ai.traceable.span.processing.config.service.servicenaming;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.span.processing.config.service.v1.CreateServiceNamingRuleRequest;
import ai.traceable.span.processing.config.service.v1.ServiceNamingRule;
import ai.traceable.span.processing.config.service.v1.ServiceNamingRuleFilter;
import ai.traceable.span.processing.config.service.v1.ServiceNamingRuleScope;
import ai.traceable.span.processing.config.service.v1.ServiceNamingRuleScope.EnvironmentScope;
import ai.traceable.span.processing.config.service.v1.UpdateServiceNamingRuleRequest;
import java.util.List;
import java.util.Optional;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ServiceNamingRuleManagerImplTest {

  private static final ServiceNamingRule RULE_1 =
      ServiceNamingRule.newBuilder().setId("rule-1").build();
  private static final ServiceNamingRule RULE_2 =
      ServiceNamingRule.newBuilder().setId("rule-2").build();

  @Mock ServiceNamingRuleStore mockStore;
  @Mock ServiceNamingRequestValidator mockValidator;
  @Mock ServiceNamingRuleBuilder mockBuilder;

  @Mock ContextualConfigObject<ServiceNamingRule> mockRule1ConfigObject;
  @Mock ContextualConfigObject<ServiceNamingRule> mockRule2ConfigObject;
  @Mock RequestContext mockRequestContext;
  @InjectMocks ServiceNamingRuleManagerImpl manager;

  @Test
  void returnsRules() {
    ServiceNamingRuleFilter testFilter =
        ServiceNamingRuleFilter.newBuilder()
            .setScope(
                ServiceNamingRuleScope.newBuilder()
                    .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentNames("test")))
            .build();

    when(mockStore.getAllObjects(mockRequestContext, testFilter))
        .thenReturn(List.of(mockRule1ConfigObject, mockRule2ConfigObject));
    when(mockBuilder.buildFromConfigObject(mockRule1ConfigObject)).thenReturn(RULE_1);
    when(mockBuilder.buildFromConfigObject(mockRule2ConfigObject)).thenReturn(RULE_2);
    assertEquals(List.of(RULE_1, RULE_2), manager.getRules(mockRequestContext, testFilter));
    verify(mockValidator, times(1)).validateOrThrow(mockRequestContext);
  }

  @Test
  void updatesRule() {
    UpdateServiceNamingRuleRequest request =
        UpdateServiceNamingRuleRequest.newBuilder().setId(RULE_1.getId()).build();
    when(mockStore.getData(mockRequestContext, RULE_1.getId())).thenReturn(Optional.of(RULE_2));
    when(mockBuilder.buildUpdatedRule(RULE_2, request)).thenReturn(RULE_1);
    when(mockStore.upsertObject(mockRequestContext, RULE_1)).thenReturn(mockRule1ConfigObject);
    when(mockBuilder.buildFromConfigObject(mockRule1ConfigObject)).thenReturn(RULE_1);
    assertEquals(RULE_1, manager.updateRule(mockRequestContext, request));
    verify(mockValidator, times(1)).validateOrThrow(mockRequestContext, request);
  }

  @Test
  void createsRule() {
    CreateServiceNamingRuleRequest request =
        CreateServiceNamingRuleRequest.newBuilder().setName("rule 2 name").build();
    when(mockStore.getAllConfigData(mockRequestContext)).thenReturn(List.of(RULE_1));
    when(mockBuilder.buildNewRule(List.of(RULE_1), request)).thenReturn(RULE_2);
    when(mockStore.upsertObject(mockRequestContext, RULE_2)).thenReturn(mockRule2ConfigObject);
    when(mockBuilder.buildFromConfigObject(mockRule2ConfigObject)).thenReturn(RULE_2);
    assertEquals(RULE_2, manager.createRule(mockRequestContext, request));
    verify(mockValidator, times(1)).validateOrThrow(mockRequestContext, request);
  }

  @Test
  void deletesRule() {
    when(mockStore.getData(mockRequestContext, RULE_1.getId())).thenReturn(Optional.of(RULE_1));
    manager.deleteRule(mockRequestContext, RULE_1.getId());
    verify(mockValidator, times(1)).validateOrThrow(mockRequestContext);
    verify(mockStore, times(1)).deleteObject(mockRequestContext, RULE_1.getId());
  }
}
