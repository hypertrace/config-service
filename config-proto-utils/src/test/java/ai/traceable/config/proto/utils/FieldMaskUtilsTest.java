package ai.traceable.config.proto.utils;

import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRule;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleStatus;
import com.google.protobuf.FieldMask;
import java.io.IOException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FieldMaskUtilsTest {
  @Test
  public void test() throws IOException {
    var existing =
        ResourceUtils.readProtoFromYaml("edge-decision-rule-1.yaml", EdgeDecisionRule.newBuilder())
            .build();
    var update = EdgeDecisionRule.newBuilder().setId(existing.getId()).setName("test").build();
    var updated =
        FieldMaskUtils.applyFieldMask(
            existing, update, FieldMask.newBuilder().addPaths("name").build());
    System.out.println(ProtoUtils.serialize(updated));
    Assertions.assertEquals(existing.getId(), updated.getId());
    Assertions.assertEquals("test", updated.getName());
    Assertions.assertEquals(existing.getRuleDefinition(), updated.getRuleDefinition());

    update =
        EdgeDecisionRule.newBuilder()
            .setId(existing.getId())
            .setName("test2")
            .setRuleStatus(EdgeDecisionRuleStatus.newBuilder().setDisabled(true))
            .build();
    updated =
        FieldMaskUtils.applyFieldMask(
            existing,
            update,
            FieldMask.newBuilder().addPaths("name").addPaths("rule_status.disabled").build());
    System.out.println(ProtoUtils.serialize(updated));
    Assertions.assertEquals(existing.getId(), updated.getId());
    Assertions.assertEquals("test2", updated.getName());
    Assertions.assertEquals(existing.getRuleDefinition(), updated.getRuleDefinition());
    Assertions.assertTrue(updated.getRuleStatus().getDisabled());

    update =
        EdgeDecisionRule.newBuilder()
            .setId(existing.getId())
            .setName("test3")
            .setRuleStatus(EdgeDecisionRuleStatus.newBuilder().setDisabled(true))
            .build();
    updated = FieldMaskUtils.applyFieldMask(existing, update, FieldMask.getDefaultInstance());
    // ignore updates if field mask is empty.
    Assertions.assertEquals(existing, updated);
  }
}
