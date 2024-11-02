package ai.traceable.edge.decision.engine.config.v1;

import static ai.traceable.edge.decision.engine.config.v1.ResourceUtils.readProtoFromYaml;

import java.io.IOException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class EdgeDecisionConfigSerdeTest {
  @Test
  public void testEdgeDecisionConfigSerde() throws IOException {
    var edgeDecisionConfig =
        readProtoFromYaml("edge-decision-engine-config.yaml", EdgeDecisionEngineConfig.newBuilder())
            .build()
            .getEdgeDecisionConfig();
    Assertions.assertEquals(1, edgeDecisionConfig.getOrderedEdgeDecisionSpecConfigsCount());
    Assertions.assertEquals(7, edgeDecisionConfig.getBlockRulesCount());

    var edgeDecisionSpecConfig = edgeDecisionConfig.getOrderedEdgeDecisionSpecConfigs(0);
    Assertions.assertEquals(3, edgeDecisionSpecConfig.getEdgeDecisionSpecsCount());
  }
}
