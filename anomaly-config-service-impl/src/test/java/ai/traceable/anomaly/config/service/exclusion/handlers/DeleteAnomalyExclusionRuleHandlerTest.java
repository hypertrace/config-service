package ai.traceable.anomaly.config.service.exclusion.handlers;

import static ai.traceable.anomaly.config.service.exclusion.ExclusionTestUtils.getAllConfigs;
import static ai.traceable.anomaly.config.service.exclusion.ExclusionTestUtils.mockConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import ai.traceable.anomaly.config.service.exclusion.ExclusionTestUtils;
import ai.traceable.anomaly.config.service.v1.exclusion.DeleteAnomalyExclusionRuleRequest;
import com.google.protobuf.Value;
import java.util.List;
import java.util.Map;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DeleteAnomalyExclusionRuleHandlerTest {

  private ConfigServiceHandler configServiceHandler;
  private MockGenericConfigService mockConfigService;
  private ConfigServiceBlockingStub configServiceBlockingStub;
  private DeleteAnomalyExclusionRuleHandler deleteAnomalyExclusionRuleHandler;

  @BeforeEach
  void setUp() {
    mockConfigService = new MockGenericConfigService().mockDelete().mockUpsert().mockGetAll();
    mockConfigService.start();
    configServiceBlockingStub = ConfigServiceGrpc.newBlockingStub(mockConfigService.channel());
    configServiceHandler = spy(new ConfigServiceHandler(configServiceBlockingStub));
    deleteAnomalyExclusionRuleHandler = new DeleteAnomalyExclusionRuleHandler(configServiceHandler);
  }

  @AfterEach
  void teardown() {
    mockConfigService.shutdown();
  }

  @Test
  void testDeleteRule() {
    Value mockExclusionRuleConfigFirst = mockConfig("first_id", "first_config_name");
    Value mockExclusionRuleConfigSecond = mockConfig("second_id", "second_config_name");

    ExclusionTestUtils.upsertConfigs(
        Map.of(
            "first_id", mockExclusionRuleConfigFirst,
            "second_id", mockExclusionRuleConfigSecond),
        configServiceBlockingStub);

    DeleteAnomalyExclusionRuleRequest deleteRequest =
        DeleteAnomalyExclusionRuleRequest.newBuilder().setRuleId("first_id").build();
    deleteAnomalyExclusionRuleHandler.deleteRule(deleteRequest);
    List<Value> remainingConfigs = getAllConfigs(configServiceBlockingStub);
    verify(configServiceHandler).deleteExclusionConfigByRuleId("first_id");
    assertEquals(1, remainingConfigs.size());
    assertEquals(mockExclusionRuleConfigSecond, remainingConfigs.get(0));
  }
}
