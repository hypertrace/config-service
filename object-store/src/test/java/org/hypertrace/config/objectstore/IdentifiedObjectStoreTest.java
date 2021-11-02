package org.hypertrace.config.objectstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.util.Values;
import io.grpc.Status;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Builder;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.config.service.v1.ContextSpecificConfig;
import org.hypertrace.config.service.v1.DeleteConfigRequest;
import org.hypertrace.config.service.v1.DeleteConfigResponse;
import org.hypertrace.config.service.v1.GetAllConfigsRequest;
import org.hypertrace.config.service.v1.GetAllConfigsResponse;
import org.hypertrace.config.service.v1.GetConfigRequest;
import org.hypertrace.config.service.v1.GetConfigResponse;
import org.hypertrace.config.service.v1.UpsertAllConfigsRequest;
import org.hypertrace.config.service.v1.UpsertAllConfigsRequest.ConfigToUpsert;
import org.hypertrace.config.service.v1.UpsertAllConfigsResponse;
import org.hypertrace.config.service.v1.UpsertAllConfigsResponse.UpsertedConfig;
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
class IdentifiedObjectStoreTest {
  private static final String TEST_RESOURCE_NAMESPACE = "test-namespace";
  private static final String TEST_RESOURCE_NAME = "test-resource";
  private static final Instant TEST_CREATE_TIMESTAMP_1 = Instant.ofEpochMilli(20);
  private static final Instant TEST_CREATE_TIMESTAMP_2 = Instant.ofEpochMilli(30);
  private static final Instant TEST_UPDATE_TIMESTAMP = Instant.ofEpochMilli(40);

  private static final TestObject OBJECT_1 = TestObject.builder().id("first-id").rank(1).build();

  private static final TestObject OBJECT_2 = TestObject.builder().id("second-id").rank(2).build();

  private static final Value OBJECT_1_AS_VALUE =
      Value.newBuilder()
          .setStructValue(
              Struct.newBuilder()
                  .putFields("id", Values.of("first-id"))
                  .putFields("rank", Values.of(1)))
          .build();

  private static final Value OBJECT_2_AS_VALUE =
      Value.newBuilder()
          .setStructValue(
              Struct.newBuilder()
                  .putFields("id", Values.of("second-id"))
                  .putFields("rank", Values.of(2)))
          .build();

  @Mock ConfigServiceBlockingStub mockStub;

  @Mock ConfigChangeEventGenerator configChangeEventGenerator;

  @Mock(answer = Answers.CALLS_REAL_METHODS)
  RequestContext mockRequestContext;

  IdentifiedObjectStore<TestObject> store;

  @BeforeEach
  void beforeEach() {
    this.mockStub = mock(ConfigServiceBlockingStub.class);
    this.store = new TestObjectStore(this.mockStub, configChangeEventGenerator);
  }

  @Test
  void generatesConfigReadRequestForGetAll() {
    when(this.mockStub.getAllConfigs(any()))
        .thenReturn(
            GetAllConfigsResponse.newBuilder()
                .addContextSpecificConfigs(
                    ContextSpecificConfig.newBuilder()
                        .setConfig(OBJECT_2_AS_VALUE)
                        .setContext(OBJECT_2.getId())
                        .setCreationTimestamp(TEST_CREATE_TIMESTAMP_2.toEpochMilli())
                        .setUpdateTimestamp(TEST_UPDATE_TIMESTAMP.toEpochMilli()))
                .addContextSpecificConfigs(
                    ContextSpecificConfig.newBuilder()
                        .setConfig(OBJECT_1_AS_VALUE)
                        .setContext(OBJECT_1.getId())
                        .setCreationTimestamp(TEST_CREATE_TIMESTAMP_1.toEpochMilli())
                        .setUpdateTimestamp(TEST_UPDATE_TIMESTAMP.toEpochMilli()))
                .build());
    assertEquals(
        List.of(
            new ContextualConfigObjectImpl<>(
                OBJECT_1.getId(), OBJECT_1, TEST_CREATE_TIMESTAMP_1, TEST_UPDATE_TIMESTAMP),
            new ContextualConfigObjectImpl<>(
                OBJECT_2.getId(), OBJECT_2, TEST_CREATE_TIMESTAMP_2, TEST_UPDATE_TIMESTAMP)),
        this.store.getAllObjects(this.mockRequestContext));

    verify(this.mockStub)
        .getAllConfigs(
            GetAllConfigsRequest.newBuilder()
                .setResourceName(TEST_RESOURCE_NAME)
                .setResourceNamespace(TEST_RESOURCE_NAMESPACE)
                .build());
  }

  @Test
  void generatesConfigReadRequestForGet() {
    when(this.mockStub.getConfig(any()))
        .thenReturn(GetConfigResponse.newBuilder().setConfig(OBJECT_1_AS_VALUE).build());

    assertEquals(Optional.of(OBJECT_1), this.store.getData(this.mockRequestContext, "id"));

    verify(this.mockStub, times(1))
        .getConfig(
            GetConfigRequest.newBuilder()
                .setResourceName(TEST_RESOURCE_NAME)
                .setResourceNamespace(TEST_RESOURCE_NAMESPACE)
                .addContexts("id")
                .build());

    when(this.mockStub.getConfig(any())).thenThrow(Status.NOT_FOUND.asRuntimeException());

    assertEquals(Optional.empty(), this.store.getData(this.mockRequestContext, "second-id"));

    verify(this.mockStub, times(1))
        .getConfig(
            GetConfigRequest.newBuilder()
                .setResourceName(TEST_RESOURCE_NAME)
                .setResourceNamespace(TEST_RESOURCE_NAMESPACE)
                .addContexts("second-id")
                .build());
  }

  @Test
  void generatesConfigDeleteRequest() {
    when(this.mockStub.deleteConfig(any()))
        .thenReturn(
            DeleteConfigResponse.newBuilder()
                .setDeletedConfig(
                    ContextSpecificConfig.newBuilder()
                        .setConfig(OBJECT_1_AS_VALUE)
                        .setContext(OBJECT_1.getId())
                        .setCreationTimestamp(TEST_CREATE_TIMESTAMP_1.toEpochMilli())
                        .setUpdateTimestamp(TEST_UPDATE_TIMESTAMP.toEpochMilli()))
                .build());
    assertEquals(
        Optional.of(
            new ContextualConfigObjectImpl<>(
                OBJECT_1.getId(), OBJECT_1, TEST_CREATE_TIMESTAMP_1, TEST_UPDATE_TIMESTAMP)),
        this.store.deleteObject(mockRequestContext, "some-id"));

    verify(this.mockStub)
        .deleteConfig(
            DeleteConfigRequest.newBuilder()
                .setResourceName(TEST_RESOURCE_NAME)
                .setResourceNamespace(TEST_RESOURCE_NAMESPACE)
                .setContext("some-id")
                .build());

    when(this.mockStub.deleteConfig(any())).thenThrow(Status.NOT_FOUND.asRuntimeException());

    assertEquals(Optional.empty(), this.store.deleteObject(mockRequestContext, "some-id"));
  }

  @Test
  void generatesConfigUpsertRequest() {
    when(this.mockStub.upsertConfig(any()))
        .thenReturn(
            UpsertConfigResponse.newBuilder()
                .setConfig(OBJECT_1_AS_VALUE)
                .setCreationTimestamp(TEST_CREATE_TIMESTAMP_1.toEpochMilli())
                .setUpdateTimestamp(TEST_UPDATE_TIMESTAMP.toEpochMilli())
                .build());
    assertEquals(
        new ContextualConfigObjectImpl<>(
            OBJECT_1.getId(), OBJECT_1, TEST_CREATE_TIMESTAMP_1, TEST_UPDATE_TIMESTAMP),
        this.store.upsertObject(this.mockRequestContext, OBJECT_1));
    verify(this.mockStub, times(1))
        .upsertConfig(
            UpsertConfigRequest.newBuilder()
                .setResourceName(TEST_RESOURCE_NAME)
                .setResourceNamespace(TEST_RESOURCE_NAMESPACE)
                .setContext("first-id")
                .setConfig(OBJECT_1_AS_VALUE)
                .build());
  }

  @Test
  void generatesUpsertRequestsForUpsertAll() {
    when(this.mockStub.upsertAllConfigs(any()))
        .thenAnswer(
            invocation -> {
              List<UpsertedConfig> configs =
                  invocation.<UpsertAllConfigsRequest>getArgument(0).getConfigsList().stream()
                      .map(
                          requestedUpsert ->
                              UpsertedConfig.newBuilder()
                                  .setConfig(requestedUpsert.getConfig())
                                  .setContext(requestedUpsert.getContext())
                                  .setCreationTimestamp(TEST_CREATE_TIMESTAMP_1.toEpochMilli())
                                  .setUpdateTimestamp(TEST_UPDATE_TIMESTAMP.toEpochMilli()))
                      .map(UpsertedConfig.Builder::build)
                      .collect(Collectors.toUnmodifiableList());
              return UpsertAllConfigsResponse.newBuilder().addAllUpsertedConfigs(configs).build();
            });
    assertEquals(
        List.of(
            new ContextualConfigObjectImpl<>(
                OBJECT_1.getId(), OBJECT_1, TEST_CREATE_TIMESTAMP_1, TEST_UPDATE_TIMESTAMP),
            new ContextualConfigObjectImpl<>(
                OBJECT_2.getId(), OBJECT_2, TEST_CREATE_TIMESTAMP_1, TEST_UPDATE_TIMESTAMP)),
        this.store.upsertObjects(this.mockRequestContext, List.of(OBJECT_1, OBJECT_2)));
    verify(this.mockStub, times(1))
        .upsertAllConfigs(
            UpsertAllConfigsRequest.newBuilder()
                .addConfigs(
                    ConfigToUpsert.newBuilder()
                        .setResourceName(TEST_RESOURCE_NAME)
                        .setResourceNamespace(TEST_RESOURCE_NAMESPACE)
                        .setContext("first-id")
                        .setConfig(OBJECT_1_AS_VALUE))
                .addConfigs(
                    ConfigToUpsert.newBuilder()
                        .setResourceName(TEST_RESOURCE_NAME)
                        .setResourceNamespace(TEST_RESOURCE_NAMESPACE)
                        .setContext("second-id")
                        .setConfig(OBJECT_2_AS_VALUE))
                .build());
  }

  @lombok.Value
  @Builder
  private static class TestObject {
    String id;
    int rank;
  }

  @lombok.Value
  @Builder
  private static class MyObject {
    String id;
    int rank;
  }

  private static class TestObjectStore extends IdentifiedObjectStore<TestObject> {
    private TestObjectStore(
        ConfigServiceBlockingStub stub, ConfigChangeEventGenerator configChangeEventGenerator) {
      super(stub, TEST_RESOURCE_NAMESPACE, TEST_RESOURCE_NAME, configChangeEventGenerator);
    }

    @Override
    protected Optional<TestObject> buildDataFromValue(Value value) {
      return Optional.of(
          TestObject.builder()
              .rank((int) value.getStructValue().getFieldsOrThrow("rank").getNumberValue())
              .id(value.getStructValue().getFieldsOrThrow("id").getStringValue())
              .build());
    }

    @Override
    protected Value buildValueFromData(TestObject object) {
      return Value.newBuilder()
          .setStructValue(
              Struct.newBuilder()
                  .putFields("id", Values.of(object.getId()))
                  .putFields("rank", Values.of(object.getRank())))
          .build();
    }

    @Override
    protected String getContextFromData(TestObject object) {
      return object.getId();
    }

    @Override
    protected List<ContextualConfigObject<TestObject>> orderFetchedObjects(
        List<ContextualConfigObject<TestObject>> objects) {
      return objects.stream()
          .sorted(Comparator.comparing(object -> object.getData().getRank()))
          .collect(Collectors.toUnmodifiableList());
    }
  }
}
