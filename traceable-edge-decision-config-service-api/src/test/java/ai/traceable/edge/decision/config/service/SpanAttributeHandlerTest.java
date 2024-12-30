package ai.traceable.edge.decision.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleCategory;
import ai.traceable.edge.decision.config.service.v1.SpanAttributeDecoration;
import java.util.List;
import org.junit.jupiter.api.Test;

class SpanAttributeHandlerTest {

  @Test
  void testGetSpanAttributeDecorationsForViolations() {
    String id = "123";
    boolean isExemption = false;
    EdgeDecisionRuleCategory category =
        EdgeDecisionRuleCategory.EDGE_DECISION_RULE_CATEGORY_THREAT_ACTOR;
    String info = "Test Info";

    List<SpanAttributeDecoration> decorations =
        SpanAttributeHandler.getSpanAttributeDecorations(id, isExemption, category, info);

    assertNotNull(decorations);
    assertEquals(2, decorations.size());

    SpanAttributeDecoration categoryDecoration = decorations.get(0);
    SpanAttributeDecoration infoDecoration = decorations.get(1);

    assertEquals(
        "traceableai.blocked.violations_123.category",
        categoryDecoration.getSpanAttributeKey().getStaticValue().getStringValue());
    assertEquals(
        category.toString(),
        categoryDecoration.getSpanAttributeValue().getStaticValue().getStringValue());

    assertEquals(
        "traceableai.blocked.violations_123.info",
        infoDecoration.getSpanAttributeKey().getStaticValue().getStringValue());
    assertEquals(info, infoDecoration.getSpanAttributeValue().getStaticValue().getStringValue());
  }

  @Test
  void testGetSpanAttributeDecorationsForExemptions() {
    String id = "456";
    boolean isExemption = true;
    EdgeDecisionRuleCategory category =
        EdgeDecisionRuleCategory.EDGE_DECISION_RULE_CATEGORY_RATE_LIMIT;
    String info = "Exemption Info";

    List<SpanAttributeDecoration> decorations =
        SpanAttributeHandler.getSpanAttributeDecorations(id, isExemption, category, info);

    assertNotNull(decorations);
    assertEquals(2, decorations.size());

    SpanAttributeDecoration categoryDecoration = decorations.get(0);
    SpanAttributeDecoration infoDecoration = decorations.get(1);

    assertEquals(
        "traceableai.blocked.exemptions_456.category",
        categoryDecoration.getSpanAttributeKey().getStaticValue().getStringValue());
    assertEquals(
        category.toString(),
        categoryDecoration.getSpanAttributeValue().getStaticValue().getStringValue());

    assertEquals(
        "traceableai.blocked.exemptions_456.info",
        infoDecoration.getSpanAttributeKey().getStaticValue().getStringValue());
    assertEquals(info, infoDecoration.getSpanAttributeValue().getStaticValue().getStringValue());
  }
}
