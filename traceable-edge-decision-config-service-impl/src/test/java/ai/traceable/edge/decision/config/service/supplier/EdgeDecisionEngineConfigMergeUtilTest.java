package ai.traceable.edge.decision.config.service.supplier;

import ai.traceable.config.proto.utils.ResourceUtils;
import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.VariableDerivationMapping;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRule;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.io.IOException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class EdgeDecisionEngineConfigMergeUtilTest {
  @Test
  public void testMergeConfigs() {
    EdgeDecisionEngineConfig config1 =
        EdgeDecisionEngineConfig.newBuilder()
            .setId("config1")
            .setName("config1")
            .setVersion(1)
            .addCommonVariables(
                VariableDerivationMapping.newBuilder()
                    .setName("config1_var1")
                    .addRules(DerivationRule.getDefaultInstance()))
            .addDecisionRules(
                EdgeDecisionRule.newBuilder().setId("config1_rule1").setName("config1_rule1"))
            .setCustomConfig(
                Value.newBuilder()
                    .setStructValue(
                        Struct.newBuilder()
                            .putFields("test", Value.newBuilder().setStringValue("test").build())))
            .build();
    EdgeDecisionEngineConfig config2 =
        EdgeDecisionEngineConfig.newBuilder()
            .setId("config2")
            .setName("config2")
            .setVersion(2)
            .addCommonVariables(
                VariableDerivationMapping.newBuilder()
                    .setName("config2_var1")
                    .addRules(DerivationRule.getDefaultInstance()))
            .addDecisionRules(
                EdgeDecisionRule.newBuilder().setId("config2_rule1").setName("config2_rule1"))
            .build();

    var merged = EdgeDecisionEngineConfigMergeUtil.merge(config1, config2);
    Assertions.assertNotNull(merged.getCustomConfig());
    Assertions.assertEquals(1, merged.getCustomConfig().getStructValue().getFieldsCount());
    Assertions.assertEquals(2, merged.getCommonVariablesCount());
    Assertions.assertEquals(2, merged.getDecisionRulesCount());
  }

  @Test
  public void test() throws IOException {
    var config1 =
        ResourceUtils.readProto(
                "configs/edge_decision_engine_config1.json", EdgeDecisionEngineConfig.newBuilder())
            .build();
    var config2 =
        ResourceUtils.readProto(
                "configs/edge_decision_engine_config2.json", EdgeDecisionEngineConfig.newBuilder())
            .build();
    var merged = EdgeDecisionEngineConfigMergeUtil.merge(config1, config2);
    Assertions.assertNotNull(merged.getCustomConfig());
    Assertions.assertEquals(1, merged.getCustomConfig().getStructValue().getFieldsCount());
  }
}
