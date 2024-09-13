package ai.traceable.detection.exclusion.config.service.v1.rules.modsec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.customsignature.config.service.modsec.CustomModsecRuleConverter;
import ai.traceable.customsignature.config.service.v1.Clause;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class ModsecBlobConverterTest {

  @Mock private CustomModsecRuleConverter customModsecRuleConverter;

  @InjectMocks private ModsecBlobConverter modsecBlobConverter;

  @BeforeEach
  public void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  void testConvertToModsecRule() throws Exception {
    String ruleIdentifier = "test-rule";
    Clause clause1 = mock(Clause.class);
    Clause clause2 = mock(Clause.class);
    List<Clause> andClausesList = List.of(clause1, clause2);
    AtomicLong modsecIdAssignment = new AtomicLong(0);
    String expectedModsecRule = "modsec-rule";

    when(customModsecRuleConverter.getValidatedModsecRuleWithCustomLogMsg(
            anyLong(),
            eq("test-rule"),
            argThat(a -> a.contains(ruleIdentifier)),
            eq(andClausesList),
            anyString()))
        .thenReturn(expectedModsecRule);

    assertEquals(
        expectedModsecRule,
        modsecBlobConverter
            .convertToModsecRule(ruleIdentifier, andClausesList, modsecIdAssignment)
            .getModsecBlob());
    assertEquals(
        ruleIdentifier,
        modsecBlobConverter
            .convertToModsecRule(ruleIdentifier, andClausesList, modsecIdAssignment)
            .getRuleId());
    assertEquals(2, modsecIdAssignment.get());
  }

  @Test
  void testConvertToModsecRule_EmptyBlob() {
    String ruleIdentifier = "test-rule";
    List<Clause> andClausesList = Collections.emptyList();
    AtomicLong modsecIdAssignment = new AtomicLong(0);
    assertEquals(
        "",
        modsecBlobConverter
            .convertToModsecRule(ruleIdentifier, andClausesList, modsecIdAssignment)
            .getModsecBlob());
    assertEquals(
        "test-rule",
        modsecBlobConverter
            .convertToModsecRule(ruleIdentifier, andClausesList, modsecIdAssignment)
            .getRuleId());
  }

  @Test
  void testConvertToModsecRule_Exception() throws Exception {
    String ruleIdentifier = "test-rule";
    Clause clause = mock(Clause.class);

    List<Clause> andClausesList = List.of(clause);
    AtomicLong modsecIdAssignment = new AtomicLong(0);

    doThrow(new RuntimeException("Test Exception"))
        .when(customModsecRuleConverter)
        .getValidatedModsecRuleWithCustomLogMsg(
            anyLong(), anyString(), anyString(), anyList(), anyString());

    assertEquals(
        "",
        modsecBlobConverter
            .convertToModsecRule(ruleIdentifier, andClausesList, modsecIdAssignment)
            .getModsecBlob());
    assertEquals(
        "",
        modsecBlobConverter
            .convertToModsecRule(ruleIdentifier, andClausesList, modsecIdAssignment)
            .getRuleId());
    assertEquals(2, modsecIdAssignment.get());
  }
}
