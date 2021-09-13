package ai.traceable.data.handling.config.service.store;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.data.handling.config.service.utils.DataHandlingRuleRankCalculator;
import ai.traceable.data.handling.config.service.v1.DataHandlingRule;
import ai.traceable.data.handling.config.service.v1.DataHandlingRuleAction;
import ai.traceable.data.handling.config.service.v1.DataHandlingRuleCondition;
import ai.traceable.data.handling.config.service.v1.DataHandlingRuleData;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Status;
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.config.service.v1.ContextSpecificConfig;
import org.hypertrace.config.service.v1.DeleteConfigRequest;
import org.hypertrace.config.service.v1.GetAllConfigsRequest;
import org.hypertrace.config.service.v1.GetAllConfigsResponse;
import org.hypertrace.config.service.v1.GetConfigRequest;
import org.hypertrace.config.service.v1.GetConfigResponse;
import org.hypertrace.config.service.v1.UpsertConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigResponse;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DataHandlingRuleStoreTest {

  private static final DataHandlingRule RULE_1 =
      DataHandlingRule.newBuilder()
          .setId("first-id")
          .setRank(1)
          .setData(
              DataHandlingRuleData.newBuilder()
                  .setName("first rule")
                  .addActions(DataHandlingRuleAction.getDefaultInstance())
                  .addConditions(DataHandlingRuleCondition.getDefaultInstance()))
          .build();

  private static final DataHandlingRule RULE_2 =
      DataHandlingRule.newBuilder()
          .setId("second-id")
          .setRank(2)
          .setData(
              DataHandlingRuleData.newBuilder()
                  .setName("second rule")
                  .addActions(DataHandlingRuleAction.getDefaultInstance())
                  .addConditions(DataHandlingRuleCondition.getDefaultInstance()))
          .build();

  private static final Value RULE_1_AS_VALUE =
      fromJson(
          "{"
              + "  \"id\": \"first-id\","
              + "  \"rank\": 1,"
              + "  \"data\": {"
              + "    \"name\": \"first rule\","
              + "    \"conditions\": [{"
              + "    }],"
              + "    \"actions\": [{"
              + "    }]"
              + "  }"
              + "}");

  private static final Value RULE_2_AS_VALUE =
      fromJson(
          "{"
              + "  \"id\": \"second-id\","
              + "  \"rank\": 2,"
              + "  \"data\": {"
              + "    \"name\": \"second rule\","
              + "    \"conditions\": [{"
              + "    }],"
              + "    \"actions\": [{"
              + "    }]"
              + "  }"
              + "}");

  @SneakyThrows
  private static Value fromJson(String json) {
    Value.Builder valueBuilder = Value.newBuilder();
    JsonFormat.parser().merge(json, valueBuilder);
    return valueBuilder.build();
  }

  @Mock ConfigServiceBlockingStub mockStub;
  @Mock DataHandlingRuleRankCalculator mockRankCalculator;

  @Mock(answer = Answers.CALLS_REAL_METHODS)
  RequestContext mockRequestContext;

  DataHandlingRuleStore store;

  @BeforeEach
  void beforeEach() {
    this.mockStub = mock(ConfigServiceBlockingStub.class);
    this.store = new DataHandlingRuleStore(this.mockStub, mockRankCalculator);
  }

  @Test
  void generatesConfigReadRequestForGetAll() {
    when(this.mockRankCalculator.orderFromRanks(any()))
        .thenAnswer(invocation -> invocation.getArguments()[0]);
    when(this.mockStub.getAllConfigs(any()))
        .thenReturn(
            GetAllConfigsResponse.newBuilder()
                .addContextSpecificConfigs(
                    ContextSpecificConfig.newBuilder().setConfig(RULE_1_AS_VALUE))
                .addContextSpecificConfigs(
                    ContextSpecificConfig.newBuilder().setConfig(RULE_2_AS_VALUE))
                .build());
    assertEquals(List.of(RULE_1, RULE_2), this.store.getRules(this.mockRequestContext));

    verify(this.mockStub)
        .getAllConfigs(
            GetAllConfigsRequest.newBuilder()
                .setResourceName("data-handling-rule")
                .setResourceNamespace("data-handling")
                .build());
  }

  @Test
  void generatesConfigReadRequestForGet() {
    when(this.mockStub.getConfig(any()))
        .thenReturn(GetConfigResponse.newBuilder().setConfig(RULE_1_AS_VALUE).build());

    assertEquals(Optional.of(RULE_1), this.store.getRule(this.mockRequestContext, "id"));

    verify(this.mockStub, times(1))
        .getConfig(
            GetConfigRequest.newBuilder()
                .setResourceName("data-handling-rule")
                .setResourceNamespace("data-handling")
                .addContexts("id")
                .build());

    when(this.mockStub.getConfig(any())).thenThrow(Status.NOT_FOUND.asRuntimeException());

    assertEquals(Optional.empty(), this.store.getRule(this.mockRequestContext, "second-id"));

    verify(this.mockStub, times(1))
        .getConfig(
            GetConfigRequest.newBuilder()
                .setResourceName("data-handling-rule")
                .setResourceNamespace("data-handling")
                .addContexts("second-id")
                .build());
  }

  @Test
  void generatesConfigDeleteRequest() {
    assertDoesNotThrow(() -> this.store.deleteRule(mockRequestContext, "some-id"));

    verify(this.mockStub)
        .deleteConfig(
            DeleteConfigRequest.newBuilder()
                .setResourceName("data-handling-rule")
                .setResourceNamespace("data-handling")
                .setContext("some-id")
                .build());
  }

  @Test
  void generatesConfigUpsertRequest() {
    when(this.mockStub.upsertConfig(any()))
        .thenReturn(UpsertConfigResponse.newBuilder().setConfig(RULE_1_AS_VALUE).build());
    assertEquals(RULE_1, this.store.upsertRule(this.mockRequestContext, RULE_1));
    verify(this.mockStub, times(1))
        .upsertConfig(
            UpsertConfigRequest.newBuilder()
                .setResourceName("data-handling-rule")
                .setResourceNamespace("data-handling")
                .setContext("first-id")
                .setConfig(RULE_1_AS_VALUE)
                .build());
  }

  @Test
  void generatesUpsertRequestsForUpsertAll() {
    when(this.mockStub.upsertConfig(any()))
        .thenAnswer(
            invocation ->
                UpsertConfigResponse.newBuilder()
                    .setConfig(((UpsertConfigRequest) invocation.getArguments()[0]).getConfig())
                    .build());
    assertEquals(
        List.of(RULE_1, RULE_2),
        this.store.upsertAllRules(this.mockRequestContext, List.of(RULE_1, RULE_2)));
    verify(this.mockStub, times(1))
        .upsertConfig(
            UpsertConfigRequest.newBuilder()
                .setResourceName("data-handling-rule")
                .setResourceNamespace("data-handling")
                .setContext("first-id")
                .setConfig(RULE_1_AS_VALUE)
                .build());
    verify(this.mockStub, times(1))
        .upsertConfig(
            UpsertConfigRequest.newBuilder()
                .setResourceName("data-handling-rule")
                .setResourceNamespace("data-handling")
                .setContext("second-id")
                .setConfig(RULE_2_AS_VALUE)
                .build());
  }
}
