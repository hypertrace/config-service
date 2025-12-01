package org.hypertrace.config.objectstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
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
import org.hypertrace.config.service.v1.DeleteConfigsRequest;
import org.hypertrace.config.service.v1.DeleteConfigsRequest.ConfigToDelete;
import org.hypertrace.config.service.v1.DeleteConfigsResponse;
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
  private static final String TEST_CREATED_BY = "test-created-by";
  private static final String TEST_LAST_MODIFIED_BY = "test-last-modified-by";

  private static final TestInternalObject OBJECT_1 =
      TestInternalObject.builder().id("first-id").rank(1).build();

  private static final TestInternalObject OBJECT_2 =
      TestInternalObject.builder().id("second-id").rank(2).build();

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

  @Mock(answer = Answers.RETURNS_SELF)
  ConfigServiceBlockingStub mockStub;

  @Mock ConfigChangeEventGenerator configChangeEventGenerator;

  @Mock(answer = Answers.CALLS_REAL_METHODS)
  RequestContext mockRequestContext;

  IdentifiedObjectStore<TestInternalObject> store;

  @BeforeEach
  void beforeEach() {
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
                        .setUpdateTimestamp(TEST_UPDATE_TIMESTAMP.toEpochMilli())
                        .setCreatedBy(TEST_CREATED_BY)
                        .setLastModifiedBy(TEST_LAST_MODIFIED_BY))
                .addContextSpecificConfigs(
                    ContextSpecificConfig.newBuilder()
                        .setConfig(OBJECT_1_AS_VALUE)
                        .setContext(OBJECT_1.getId())
                        .setCreationTimestamp(TEST_CREATE_TIMESTAMP_1.toEpochMilli())
                        .setUpdateTimestamp(TEST_UPDATE_TIMESTAMP.toEpochMilli())
                        .setCreatedBy(TEST_CREATED_BY)
                        .setLastModifiedBy(TEST_LAST_MODIFIED_BY))
                .build());
    assertEquals(
        List.of(
            new ContextualConfigObjectImpl<>(
                OBJECT_1.getId(),
                OBJECT_1,
                TEST_CREATE_TIMESTAMP_1,
                TEST_UPDATE_TIMESTAMP,
                TEST_CREATED_BY,
                TEST_LAST_MODIFIED_BY),
            new ContextualConfigObjectImpl<>(
                OBJECT_2.getId(),
                OBJECT_2,
                TEST_CREATE_TIMESTAMP_2,
                TEST_UPDATE_TIMESTAMP,
                TEST_CREATED_BY,
                TEST_LAST_MODIFIED_BY)),
        this.store.getAllObjects(this.mockRequestContext));

    verify(this.mockStub)
        .getAllConfigs(
            GetAllConfigsRequest.newBuilder()
                .setResourceName(TEST_RESOURCE_NAME)
                .setResourceNamespace(TEST_RESOURCE_NAMESPACE)
                .build());
  }

  @Test
  void generatesConfigReadRequestForGetObject() {
    when(this.mockStub.getConfig(any()))
        .thenReturn(
            GetConfigResponse.newBuilder()
                .setConfig(OBJECT_1_AS_VALUE)
                .setCreationTimestamp(TEST_CREATE_TIMESTAMP_1.toEpochMilli())
                .setUpdateTimestamp(TEST_UPDATE_TIMESTAMP.toEpochMilli())
                .setCreatedBy(TEST_CREATED_BY)
                .setLastModifiedBy(TEST_LAST_MODIFIED_BY)
                .build());

    assertEquals(
        Optional.of(
            new ContextualConfigObjectImpl<>(
                OBJECT_1.getId(),
                OBJECT_1,
                TEST_CREATE_TIMESTAMP_1,
                TEST_UPDATE_TIMESTAMP,
                TEST_CREATED_BY,
                TEST_LAST_MODIFIED_BY)),
        this.store.getObject(this.mockRequestContext, OBJECT_1.getId()));

    verify(this.mockStub, times(1))
        .getConfig(
            GetConfigRequest.newBuilder()
                .setResourceName(TEST_RESOURCE_NAME)
                .setResourceNamespace(TEST_RESOURCE_NAMESPACE)
                .addContexts(OBJECT_1.getId())
                .build());

    when(this.mockStub.getConfig(any())).thenThrow(Status.NOT_FOUND.asRuntimeException());

    assertEquals(Optional.empty(), this.store.getObject(this.mockRequestContext, OBJECT_2.getId()));

    verify(this.mockStub, times(1))
        .getConfig(
            GetConfigRequest.newBuilder()
                .setResourceName(TEST_RESOURCE_NAME)
                .setResourceNamespace(TEST_RESOURCE_NAMESPACE)
                .addContexts(OBJECT_2.getId())
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
            new DeletedContextualConfigObjectImpl<>(
                OBJECT_1.getId(), OBJECT_1, TEST_CREATE_TIMESTAMP_1, TEST_UPDATE_TIMESTAMP)),
        this.store.deleteObject(mockRequestContext, "first-id"));

    verify(this.mockStub)
        .deleteConfig(
            DeleteConfigRequest.newBuilder()
                .setResourceName(TEST_RESOURCE_NAME)
                .setResourceNamespace(TEST_RESOURCE_NAMESPACE)
                .setContext("first-id")
                .build());

    when(this.mockStub.deleteConfig(any())).thenThrow(Status.NOT_FOUND.asRuntimeException());

    assertEquals(Optional.empty(), this.store.deleteObject(mockRequestContext, "some-id"));

    verify(this.configChangeEventGenerator, times(1))
        .sendDeleteNotification(
            eq(this.mockRequestContext),
            eq(TestApiObject.class.getName()),
            eq("first-id"),
            eq(
                Value.newBuilder()
                    .setStructValue(
                        Struct.newBuilder()
                            .putFields("api_id", Values.of(OBJECT_1.getId()))
                            .putFields("api_rank", Values.of(OBJECT_1.getRank())))
                    .build()));
  }

  @Test
  void generatesConfigUpsertRequest() {
    when(this.mockStub.upsertConfig(any()))
        .thenReturn(
            UpsertConfigResponse.newBuilder()
                .setConfig(OBJECT_1_AS_VALUE)
                .setCreationTimestamp(TEST_CREATE_TIMESTAMP_1.toEpochMilli())
                .setUpdateTimestamp(TEST_UPDATE_TIMESTAMP.toEpochMilli())
                .setCreatedBy(TEST_CREATED_BY)
                .setLastModifiedBy(TEST_LAST_MODIFIED_BY)
                .build());
    ContextualConfigObject contextualConfigObject =
        new ContextualConfigObjectImpl<>(
            OBJECT_1.getId(),
            OBJECT_1,
            TEST_CREATE_TIMESTAMP_1,
            TEST_UPDATE_TIMESTAMP,
            TEST_CREATED_BY,
            TEST_LAST_MODIFIED_BY);
    assertEquals(
        contextualConfigObject, this.store.upsertObject(this.mockRequestContext, OBJECT_1));
    verify(this.mockStub, times(1))
        .upsertConfig(
            UpsertConfigRequest.newBuilder()
                .setResourceName(TEST_RESOURCE_NAME)
                .setResourceNamespace(TEST_RESOURCE_NAMESPACE)
                .setContext("first-id")
                .setConfig(OBJECT_1_AS_VALUE)
                .build());
    verify(this.configChangeEventGenerator, times(1))
        .sendCreateNotification(
            eq(this.mockRequestContext),
            eq(TestApiObject.class.getName()),
            eq(contextualConfigObject.getContext()),
            eq(
                Value.newBuilder()
                    .setStructValue(
                        Struct.newBuilder()
                            .putFields("api_id", Values.of(OBJECT_1.getId()))
                            .putFields("api_rank", Values.of(OBJECT_1.getRank())))
                    .build()));
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
                                  .setUpdateTimestamp(TEST_UPDATE_TIMESTAMP.toEpochMilli())
                                  .setCreatedBy(TEST_CREATED_BY)
                                  .setLastModifiedBy(TEST_LAST_MODIFIED_BY))
                      .map(UpsertedConfig.Builder::build)
                      .collect(Collectors.toUnmodifiableList());
              return UpsertAllConfigsResponse.newBuilder().addAllUpsertedConfigs(configs).build();
            });
    assertEquals(
        List.of(
            new ContextualConfigObjectImpl<>(
                OBJECT_1.getId(),
                OBJECT_1,
                TEST_CREATE_TIMESTAMP_1,
                TEST_UPDATE_TIMESTAMP,
                TEST_CREATED_BY,
                TEST_LAST_MODIFIED_BY),
            new ContextualConfigObjectImpl<>(
                OBJECT_2.getId(),
                OBJECT_2,
                TEST_CREATE_TIMESTAMP_1,
                TEST_UPDATE_TIMESTAMP,
                TEST_CREATED_BY,
                TEST_LAST_MODIFIED_BY)),
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

  @Test
  void generatesDeleteRequestsForDeleteConfigs() {
    when(this.mockStub.deleteConfigs(any()))
        .thenReturn(
            DeleteConfigsResponse.newBuilder()
                .addAllDeletedConfigs(
                    List.of(
                        ContextSpecificConfig.newBuilder()
                            .setConfig(OBJECT_1_AS_VALUE)
                            .setContext(OBJECT_1.getId())
                            .setCreationTimestamp(TEST_CREATE_TIMESTAMP_1.toEpochMilli())
                            .setUpdateTimestamp(TEST_UPDATE_TIMESTAMP.toEpochMilli())
                            .build(),
                        ContextSpecificConfig.newBuilder()
                            .setConfig(OBJECT_2_AS_VALUE)
                            .setContext(OBJECT_2.getId())
                            .setCreationTimestamp(TEST_CREATE_TIMESTAMP_1.toEpochMilli())
                            .setUpdateTimestamp(TEST_UPDATE_TIMESTAMP.toEpochMilli())
                            .build()))
                .build());
    assertEquals(
        List.of(
            new DeletedContextualConfigObjectImpl<>(
                OBJECT_1.getId(), OBJECT_1, TEST_CREATE_TIMESTAMP_1, TEST_UPDATE_TIMESTAMP),
            new DeletedContextualConfigObjectImpl<>(
                OBJECT_2.getId(), OBJECT_2, TEST_CREATE_TIMESTAMP_1, TEST_UPDATE_TIMESTAMP)),
        this.store.deleteObjects(
            this.mockRequestContext, List.of(OBJECT_1.getId(), OBJECT_2.getId())));
    verify(this.mockStub, times(1))
        .deleteConfigs(
            DeleteConfigsRequest.newBuilder()
                .addConfigs(
                    ConfigToDelete.newBuilder()
                        .setResourceName(TEST_RESOURCE_NAME)
                        .setResourceNamespace(TEST_RESOURCE_NAMESPACE)
                        .setContext("first-id"))
                .addConfigs(
                    ConfigToDelete.newBuilder()
                        .setResourceName(TEST_RESOURCE_NAME)
                        .setResourceNamespace(TEST_RESOURCE_NAMESPACE)
                        .setContext("second-id"))
                .build());
  }

  @Test
  void buildClassNameForChangeEvent_test() {
    assertEquals(TestApiObject.class.getName(), this.store.buildClassNameForChangeEvent(OBJECT_1));
  }

  @lombok.Value
  @Builder
  private static class TestInternalObject {
    String id;
    int rank;
  }

  @lombok.Value
  @Builder
  private static class TestApiObject {
    String api_id;
    int api_rank;
  }

  private static class TestObjectStore extends IdentifiedObjectStore<TestInternalObject> {
    private TestObjectStore(
        ConfigServiceBlockingStub stub, ConfigChangeEventGenerator configChangeEventGenerator) {
      super(stub, TEST_RESOURCE_NAMESPACE, TEST_RESOURCE_NAME, configChangeEventGenerator);
    }

    @Override
    protected Optional<TestInternalObject> buildDataFromValue(Value value) {
      return Optional.of(
          TestInternalObject.builder()
              .rank((int) value.getStructValue().getFieldsOrThrow("rank").getNumberValue())
              .id(value.getStructValue().getFieldsOrThrow("id").getStringValue())
              .build());
    }

    @Override
    protected Value buildValueFromData(TestInternalObject object) {
      return Value.newBuilder()
          .setStructValue(
              Struct.newBuilder()
                  .putFields("id", Values.of(object.getId()))
                  .putFields("rank", Values.of(object.getRank())))
          .build();
    }

    @Override
    protected Value buildValueForChangeEvent(TestInternalObject object) {
      return Value.newBuilder()
          .setStructValue(
              Struct.newBuilder()
                  .putFields("api_id", Values.of(object.getId()))
                  .putFields("api_rank", Values.of(object.getRank())))
          .build();
    }

    @Override
    protected String buildClassNameForChangeEvent(TestInternalObject object) {
      return TestApiObject.class.getName();
    }

    @Override
    protected String getContextFromData(TestInternalObject object) {
      return object.getId();
    }

    @Override
    protected List<ContextualConfigObject<TestInternalObject>> orderFetchedObjects(
        List<ContextualConfigObject<TestInternalObject>> objects) {
      return objects.stream()
          .sorted(Comparator.comparing(object -> object.getData().getRank()))
          .collect(Collectors.toUnmodifiableList());
    }
  }
}
