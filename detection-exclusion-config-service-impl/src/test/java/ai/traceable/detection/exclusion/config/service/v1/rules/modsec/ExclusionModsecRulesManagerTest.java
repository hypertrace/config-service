package ai.traceable.detection.exclusion.config.service.v1.rules.modsec;

import static ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion.MODSEC_RULE_VERSION_V3_SECARG_LIMITS_DETECTION_ONLY_MODE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistry;
import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionModsecRule;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRule;
import ai.traceable.detection.exclusion.config.service.v1.GetExclusionModsecRulesResponse;
import ai.traceable.detection.exclusion.config.service.v1.ModsecBlobData;
import ai.traceable.detection.exclusion.config.service.v1.rules.modsec.ModsecBlobConverter.ModsecBlobResult;
import ai.traceable.detection.exclusion.config.service.v1.rules.modsec.ModsecClauseConverter.ModsecClauseResult;
import ai.traceable.detection.exclusion.config.service.v1.rules.modsec.ModsecClauseConverter.ServiceDetail;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class ExclusionModsecRulesManagerTest {
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId("tenant");

  @Mock private ModsecRulesRegistry modsecRulesRegistry;

  @Mock private ModsecClauseConverter modsecClauseConverter;

  @Mock private ModsecBlobConverter modsecBlobConverter;

  @Mock private ModsecBlobValidator modsecBlobValidator;

  @InjectMocks private ExclusionModsecRulesManager exclusionModsecRulesManager;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    when(modsecBlobValidator.validate(any(RequestContext.class), anyString(), anyString()))
        .thenReturn(true);
    when(modsecRulesRegistry.getModsecHeader(
            MODSEC_RULE_VERSION_V3_SECARG_LIMITS_DETECTION_ONLY_MODE))
        .thenReturn("SecRuleEngine DetectionOnly");
  }

  @Test
  void testGetModsecRules() {
    List<String> serviceNames = Arrays.asList("service1", "service2");

    DetectionExclusionRule exclusionRule1 = mock(DetectionExclusionRule.class);
    DetectionExclusionRule exclusionRule2 = mock(DetectionExclusionRule.class);
    when(exclusionRule1.getId()).thenReturn("rule1");
    when(exclusionRule2.getId()).thenReturn("rule2");
    List<DetectionExclusionRule> exclusionRules = List.of(exclusionRule1, exclusionRule2);

    ModsecClauseConverter.ModsecClauseResult clauseResult =
        mock(ModsecClauseConverter.ModsecClauseResult.class);

    Clause mockClause = mock(Clause.class);
    when(clauseResult.getClauses()).thenReturn(Collections.singletonList(mockClause));
    when(modsecClauseConverter.convert(REQUEST_CONTEXT, exclusionRule1)).thenReturn(clauseResult);
    ModsecBlobResult blobResult1 = new ModsecBlobResult("blob1", "rule1");
    when(modsecBlobConverter.convertToModsecRule(eq("rule1"), anyList(), any(AtomicLong.class)))
        .thenReturn(blobResult1);

    // Empty blob
    ModsecClauseResult clauseResult2 = new ModsecClauseResult(List.of(), List.of());
    when(modsecClauseConverter.convert(REQUEST_CONTEXT, exclusionRule2)).thenReturn(clauseResult2);
    ModsecBlobResult blobResult2 = new ModsecBlobResult("", "rule2");
    when(modsecBlobConverter.convertToModsecRule(eq("rule2"), anyList(), any(AtomicLong.class)))
        .thenReturn(blobResult2);

    GetExclusionModsecRulesResponse response =
        exclusionModsecRulesManager.getModsecRules(REQUEST_CONTEXT, exclusionRules, serviceNames);
    assertEquals("SecRuleEngine DetectionOnly", response.getModsecDirectivesBlob());

    List<ModsecBlobData> modsecBlobsData = response.getModsecBlobsDataList();
    assertEquals(1, modsecBlobsData.size());
    assertEquals("blob1", modsecBlobsData.get(0).getModsecBlob());
    assertEquals(List.of("rule1", "rule2"), modsecBlobsData.get(0).getRuleIdsList());
    assertEquals(List.of("service1", "service2"), modsecBlobsData.get(0).getServiceNamesList());

    List<DetectionExclusionModsecRule> modsecRules = response.getModsecRulesList();
    assertEquals(2, modsecRules.size());
    assertEquals(exclusionRule1, modsecRules.get(0).getRule());
    assertEquals(List.of("rule1"), modsecRules.get(0).getAssociatedModsecRuleIdsList());
    assertEquals(exclusionRule2, modsecRules.get(1).getRule());
    // As modsec blob is empty
    assertTrue(modsecRules.get(1).getAssociatedModsecRuleIdsList().isEmpty());
  }

  @Test
  void testServiceScope() {
    DetectionExclusionRule exclusionRule1 = mock(DetectionExclusionRule.class);
    when(exclusionRule1.getId()).thenReturn("rule1");

    Clause mockClause = mock(Clause.class);
    ModsecClauseResult clauseResult =
        new ModsecClauseResult(List.of(mockClause), List.of(new ServiceDetail("service2", false)));

    when(modsecClauseConverter.convert(REQUEST_CONTEXT, exclusionRule1)).thenReturn(clauseResult);

    ModsecBlobResult blobResult1 = new ModsecBlobResult("", "rule1");
    when(modsecBlobConverter.convertToModsecRule(
            eq("rule1"), eq(List.of(mockClause)), any(AtomicLong.class)))
        .thenReturn(blobResult1);

    List<String> serviceNames = Arrays.asList("service1", "service2");
    GetExclusionModsecRulesResponse response =
        exclusionModsecRulesManager.getModsecRules(
            REQUEST_CONTEXT, List.of(exclusionRule1), serviceNames);

    assertEquals("SecRuleEngine DetectionOnly", response.getModsecDirectivesBlob());

    List<ModsecBlobData> modsecBlobsData = response.getModsecBlobsDataList();
    assertEquals(2, modsecBlobsData.size());
    assertEquals("", modsecBlobsData.get(0).getModsecBlob());
    assertTrue(modsecBlobsData.get(0).getRuleIdsList().isEmpty());
    assertEquals(
        Collections.singletonList("service1"), modsecBlobsData.get(0).getServiceNamesList());
    assertEquals("", modsecBlobsData.get(1).getModsecBlob());
    assertEquals(Collections.singletonList("rule1"), modsecBlobsData.get(1).getRuleIdsList());
    assertEquals(
        Collections.singletonList("service2"), modsecBlobsData.get(1).getServiceNamesList());

    List<DetectionExclusionModsecRule> modsecRules = response.getModsecRulesList();
    assertEquals(1, modsecRules.size());
    assertEquals(exclusionRule1, modsecRules.get(0).getRule());
    assertTrue(modsecRules.get(0).getAssociatedModsecRuleIdsList().isEmpty());
  }

  @Test
  void testGetModsecRules_withServiceScope() {
    DetectionExclusionRule exclusionRule1 = mock(DetectionExclusionRule.class);
    when(exclusionRule1.getId()).thenReturn("rule1");
    DetectionExclusionRule exclusionRule2 = mock(DetectionExclusionRule.class);
    when(exclusionRule2.getId()).thenReturn("rule2");
    List<DetectionExclusionRule> exclusionRules = List.of(exclusionRule1, exclusionRule2);

    Clause mockClause1 = mock(Clause.class);
    Clause mockClause2 = mock(Clause.class);
    ModsecClauseResult clauseResult1 =
        new ModsecClauseResult(List.of(mockClause1), List.of(new ServiceDetail("service1", false)));
    ModsecClauseResult clauseResult2 =
        new ModsecClauseResult(List.of(mockClause2), List.of(new ServiceDetail("service2", true)));

    when(modsecClauseConverter.convert(REQUEST_CONTEXT, exclusionRule1)).thenReturn(clauseResult1);
    when(modsecClauseConverter.convert(REQUEST_CONTEXT, exclusionRule2)).thenReturn(clauseResult2);

    ModsecBlobResult blobResult1 = new ModsecBlobResult("modsecBlob1", "rule1");
    ModsecBlobResult blobResult2 = new ModsecBlobResult("modsecBlob2", "rule2");
    when(modsecBlobConverter.convertToModsecRule(
            eq("rule1"), eq(List.of(mockClause1)), any(AtomicLong.class)))
        .thenReturn(blobResult1);
    when(modsecBlobConverter.convertToModsecRule(
            eq("rule2"), eq(List.of(mockClause2)), any(AtomicLong.class)))
        .thenReturn(blobResult2);

    List<String> serviceNames = Arrays.asList("service1", "service2", "service3");
    GetExclusionModsecRulesResponse response =
        exclusionModsecRulesManager.getModsecRules(REQUEST_CONTEXT, exclusionRules, serviceNames);

    assertEquals("SecRuleEngine DetectionOnly", response.getModsecDirectivesBlob());

    List<ModsecBlobData> modsecBlobsData = response.getModsecBlobsDataList();
    assertEquals(3, modsecBlobsData.size());
    assertEquals("modsecBlob1\n\nmodsecBlob2", modsecBlobsData.get(0).getModsecBlob());
    assertEquals(List.of("rule1", "rule2"), modsecBlobsData.get(0).getRuleIdsList());
    assertEquals(
        Collections.singletonList("service1"), modsecBlobsData.get(0).getServiceNamesList());
    assertEquals("", modsecBlobsData.get(1).getModsecBlob());
    assertTrue(modsecBlobsData.get(1).getRuleIdsList().isEmpty());
    assertEquals(
        Collections.singletonList("service2"), modsecBlobsData.get(1).getServiceNamesList());
    assertEquals("modsecBlob2", modsecBlobsData.get(2).getModsecBlob());
    assertEquals(Collections.singletonList("rule2"), modsecBlobsData.get(2).getRuleIdsList());
    assertEquals(
        Collections.singletonList("service3"), modsecBlobsData.get(2).getServiceNamesList());

    List<DetectionExclusionModsecRule> modsecRules = response.getModsecRulesList();
    assertEquals(2, modsecRules.size());
    assertEquals(exclusionRule1, modsecRules.get(0).getRule());
    assertEquals(List.of("rule1"), modsecRules.get(0).getAssociatedModsecRuleIdsList());
    assertEquals(exclusionRule2, modsecRules.get(1).getRule());
    assertEquals(List.of("rule2"), modsecRules.get(1).getAssociatedModsecRuleIdsList());
  }
}
