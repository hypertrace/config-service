package ai.traceable.config.proto.utils;

import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRule;
import java.io.IOException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ProtoUtilsTest {
  @Test
  public void test() throws IOException {
    var existing =
        ResourceUtils.readProtoFromYaml("edge-decision-rule-1.yaml", EdgeDecisionRule.newBuilder())
            .build();
    var matches = ProtoUtils.find(existing, DataTransformationConfig.class);
    System.out.println(matches);
    Assertions.assertEquals(2, matches.size());
  }
}
