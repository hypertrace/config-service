package ai.traceable.edge.decision.engine.config.v1;

import static ai.traceable.edge.decision.engine.config.v1.ResourceUtils.readProtoFromYaml;

import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfigs;
import java.io.IOException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class EdgeDecisionConfigSerdeTest {
  @Test
  public void testEdgeDecisionConfigSerde() throws IOException {
    EdgeDecisionEngineConfigs edgeDecisionConfigs =
        readProtoFromYaml(
                "edge-decision-engine-configs.yaml", EdgeDecisionEngineConfigs.newBuilder())
            .build();
    Assertions.assertEquals(7, edgeDecisionConfigs.getCommonVariablesCount());
    Assertions.assertEquals(7, edgeDecisionConfigs.getDecisionRulesCount());
    Assertions.assertEquals(1, edgeDecisionConfigs.getScopedEdgeDecisionSpecConfigsCount());
    Assertions.assertEquals(
        3, edgeDecisionConfigs.getScopedEdgeDecisionSpecConfigs(0).getEdgeDecisionSpecsCount());
  }
}
