package ai.traceable.edge.decision.config.service.v1;

import static ai.traceable.edge.decision.engine.config.v1.ResourceUtils.readProtoFromYaml;

import java.io.IOException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class EdgeDecisionConfigSerdeTest {

  @Test
  public void testEdgeDecisionConfigSerde() throws IOException {
    EdgeDecisionEngineConfig edgeDecisionConfig =
        readProtoFromYaml(
                "edge-decision-engine-configs.yaml", EdgeDecisionEngineConfig.newBuilder())
            .build();
    Assertions.assertEquals(7, edgeDecisionConfig.getCommonVariablesCount());
    Assertions.assertEquals(6, edgeDecisionConfig.getDecisionRulesCount());
    Assertions.assertEquals(1, edgeDecisionConfig.getEdgeDecisionSpecConfigsCount());
    Assertions.assertEquals(
        3, edgeDecisionConfig.getEdgeDecisionSpecConfigs(0).getEdgeDecisionSpecsCount());
  }
}
