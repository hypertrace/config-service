package ai.traceable.userattribution.config.service.store;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.CustomUserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.HeaderLocation;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.RequestHeaderUserAttributionRuleData;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.util.List;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.config.service.v1.ContextSpecificConfig;
import org.hypertrace.config.service.v1.DeleteConfigRequest;
import org.hypertrace.config.service.v1.GetAllConfigsRequest;
import org.hypertrace.config.service.v1.GetAllConfigsResponse;
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
class UserAttributionRuleStoreTest {
  private static final UserAttributionRule RULE_1 =
      UserAttributionRule.newBuilder()
          .setId("first-id")
          .setName("first-name")
          .setData(
              UserAttributionRuleData.newBuilder()
                  .setCustomData(CustomUserAttributionRuleData.newBuilder().setYaml("some-yaml")))
          .build();

  private static final UserAttributionRule RULE_2 =
      UserAttributionRule.newBuilder()
          .setId("second-id")
          .setName("second-name")
          .setData(
              UserAttributionRuleData.newBuilder()
                  .setRequestHeaderData(
                      RequestHeaderUserAttributionRuleData.newBuilder()
                          .setUserIdLocation(
                              HeaderLocation.newBuilder().setHeaderName("second-header"))))
          .build();

  private static final Value RULE_1_AS_VALUE =
      Value.newBuilder()
          .setStructValue(
              Struct.newBuilder()
                  .putFields("id", Value.newBuilder().setStringValue("first-id").build())
                  .putFields("name", Value.newBuilder().setStringValue("first-name").build())
                  .putFields(
                      "data",
                      Value.newBuilder()
                          .setStructValue(
                              Struct.newBuilder()
                                  .putFields(
                                      "customData",
                                      Value.newBuilder()
                                          .setStructValue(
                                              Struct.newBuilder()
                                                  .putFields(
                                                      "yaml",
                                                      Value.newBuilder()
                                                          .setStringValue("some-yaml")
                                                          .build()))
                                          .build()))
                          .build())
                  .build())
          .build();
  private static final Value RULE_2_AS_VALUE =
      Value.newBuilder()
          .setStructValue(
              Struct.newBuilder()
                  .putFields("id", Value.newBuilder().setStringValue("second-id").build())
                  .putFields("name", Value.newBuilder().setStringValue("second-name").build())
                  .putFields(
                      "data",
                      Value.newBuilder()
                          .setStructValue(
                              Struct.newBuilder()
                                  .putFields(
                                      "requestHeaderData",
                                      Value.newBuilder()
                                          .setStructValue(
                                              Struct.newBuilder()
                                                  .putFields(
                                                      "userIdLocation",
                                                      Value.newBuilder()
                                                          .setStructValue(
                                                              Struct.newBuilder()
                                                                  .putFields(
                                                                      "headerName",
                                                                      Value.newBuilder()
                                                                          .setStringValue(
                                                                              "second-header")
                                                                          .build()))
                                                          .build()))
                                          .build()))
                          .build())
                  .build())
          .build();
  @Mock ConfigServiceBlockingStub mockStub;

  @Mock(answer = Answers.CALLS_REAL_METHODS)
  RequestContext mockRequestContext;

  UserAttributionRuleStore store;

  @BeforeEach
  void beforeEach() {
    this.mockStub = mock(ConfigServiceBlockingStub.class);
    this.store = new UserAttributionRuleStore(this.mockStub);
  }

  @Test
  void generatesConfigReadRequest() {
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
                .setResourceName("user-attribution-rule")
                .setResourceNamespace("user-attribution")
                .build());
  }

  @Test
  void generatesConfigDeleteRequest() {
    assertDoesNotThrow(() -> this.store.deleteRule(mockRequestContext, "some-id"));

    verify(this.mockStub)
        .deleteConfig(
            DeleteConfigRequest.newBuilder()
                .setResourceName("user-attribution-rule")
                .setResourceNamespace("user-attribution")
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
                .setResourceName("user-attribution-rule")
                .setResourceNamespace("user-attribution")
                .setContext("first-id")
                .setConfig(RULE_1_AS_VALUE)
                .build());

    when(this.mockStub.upsertConfig(any()))
        .thenReturn(UpsertConfigResponse.newBuilder().setConfig(RULE_2_AS_VALUE).build());
    assertEquals(RULE_2, this.store.upsertRule(this.mockRequestContext, RULE_2));
    verify(this.mockStub, times(1))
        .upsertConfig(
            UpsertConfigRequest.newBuilder()
                .setResourceName("user-attribution-rule")
                .setResourceNamespace("user-attribution")
                .setContext("second-id")
                .setConfig(RULE_2_AS_VALUE)
                .build());
  }
}
