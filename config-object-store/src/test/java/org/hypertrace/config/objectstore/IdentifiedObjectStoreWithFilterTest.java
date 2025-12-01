package org.hypertrace.config.objectstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.util.Values;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Builder;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.config.service.v1.ContextSpecificConfig;
import org.hypertrace.config.service.v1.GetAllConfigsRequest;
import org.hypertrace.config.service.v1.GetAllConfigsResponse;
import org.hypertrace.config.service.v1.GetConfigRequest;
import org.hypertrace.config.service.v1.GetConfigResponse;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class IdentifiedObjectStoreWithFilterTest {
  private static final String TEST_RESOURCE_NAMESPACE = "test-namespace";
  private static final String TEST_RESOURCE_NAME = "test-resource";
  private static final Instant TEST_CREATE_TIMESTAMP_1 = Instant.ofEpochMilli(20);
  private static final Instant TEST_CREATE_TIMESTAMP_2 = Instant.ofEpochMilli(30);
  private static final Instant TEST_UPDATE_TIMESTAMP = Instant.ofEpochMilli(40);
  private static final String TEST_CREATED_BY = "test-created-by";
  private static final String TEST_LAST_MODIFIED_BY = "test-last-modified-by";

  private static final TestInternalObject OBJECT_1 =
      TestInternalObject.builder()
          .id("first-id")
          .rank(1)
          .nestedObjects(
              List.of(
                  TestInternalNestedObject.builder().id("id1a").points(1000).build(),
                  TestInternalNestedObject.builder().id("id1b").points(2000).build()))
          .build();

  private static final TestInternalObject OBJECT_1_FILTERED_BY_2 =
      TestInternalObject.builder()
          .id("first-id")
          .rank(1)
          .nestedObjects(
              List.of(TestInternalNestedObject.builder().id("id1b").points(2000).build()))
          .build();

  private static final TestInternalObject OBJECT_2 =
      TestInternalObject.builder()
          .id("second-id")
          .rank(2)
          .nestedObjects(
              List.of(
                  TestInternalNestedObject.builder().id("id2a").points(3000).build(),
                  TestInternalNestedObject.builder().id("id2b").points(4000).build()))
          .build();

  private static final TestInternalObject OBJECT_2_FILTERED_BY_3 =
      TestInternalObject.builder()
          .id("second-id")
          .rank(2)
          .nestedObjects(
              List.of(TestInternalNestedObject.builder().id("id2b").points(4000).build()))
          .build();

  private static final Value OBJECT_1_AS_VALUE = convertToValue(OBJECT_1);
  private static final Value OBJECT_2_AS_VALUE = convertToValue(OBJECT_2);

  // should filter out OBJECT_1 and accept OBJECT_2 as is
  private static final TestInternalFilter FILTER_1 =
      new TestInternalFilter(Optional.of(2), Optional.empty());

  // should partially filter OBJECT_1 and accept OBJECT_2 as is
  private static final TestInternalFilter FILTER_2 =
      new TestInternalFilter(Optional.empty(), Optional.of(1500));

  // should filter out OBJECT_1 and partially filter OBJECT_2
  private static final TestInternalFilter FILTER_3 =
      new TestInternalFilter(Optional.of(2), Optional.of(3500));

  private static Value convertToValue(TestInternalObject internalObject) {
    return Value.newBuilder()
        .setStructValue(
            Struct.newBuilder()
                .putFields("id", Values.of(internalObject.getId()))
                .putFields("rank", Values.of(internalObject.getRank()))
                .putFields(
                    "nestedObjects",
                    Values.of(
                        internalObject.getNestedObjects().stream()
                            .map(
                                nestedObject ->
                                    Value.newBuilder()
                                        .setStructValue(
                                            Struct.newBuilder()
                                                .putFields("id", Values.of(nestedObject.getId()))
                                                .putFields(
                                                    "points", Values.of(nestedObject.getPoints())))
                                        .build())
                            .collect(Collectors.toUnmodifiableList()))))
        .build();
  }

  @Mock(answer = Answers.RETURNS_SELF)
  ConfigServiceBlockingStub mockStub;

  @Mock ConfigChangeEventGenerator configChangeEventGenerator;

  @Mock(answer = Answers.CALLS_REAL_METHODS)
  RequestContext mockRequestContext;

  IdentifiedObjectStoreWithFilter<TestInternalObject, TestInternalFilter> store;

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
    assertEquals(List.of(OBJECT_1, OBJECT_2), this.store.getAllConfigData(this.mockRequestContext));

    assertEquals(
        List.of(
            new ContextualConfigObjectImpl<>(
                OBJECT_2.getId(),
                OBJECT_2,
                TEST_CREATE_TIMESTAMP_2,
                TEST_UPDATE_TIMESTAMP,
                TEST_CREATED_BY,
                TEST_LAST_MODIFIED_BY)),
        this.store.getAllObjects(this.mockRequestContext, FILTER_1));
    assertEquals(List.of(OBJECT_2), this.store.getAllConfigData(this.mockRequestContext, FILTER_1));

    assertEquals(
        List.of(
            new ContextualConfigObjectImpl<>(
                OBJECT_1.getId(),
                OBJECT_1_FILTERED_BY_2,
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
        this.store.getAllObjects(this.mockRequestContext, FILTER_2));
    assertEquals(
        List.of(OBJECT_1_FILTERED_BY_2, OBJECT_2),
        this.store.getAllConfigData(this.mockRequestContext, FILTER_2));

    assertEquals(
        List.of(
            new ContextualConfigObjectImpl<>(
                OBJECT_2.getId(),
                OBJECT_2_FILTERED_BY_3,
                TEST_CREATE_TIMESTAMP_2,
                TEST_UPDATE_TIMESTAMP,
                TEST_CREATED_BY,
                TEST_LAST_MODIFIED_BY)),
        this.store.getAllObjects(this.mockRequestContext, FILTER_3));
    assertEquals(
        List.of(OBJECT_2_FILTERED_BY_3),
        this.store.getAllConfigData(this.mockRequestContext, FILTER_3));

    verify(this.mockStub, atLeastOnce())
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
    assertEquals(
        Optional.of(OBJECT_1), this.store.getData(this.mockRequestContext, OBJECT_1.getId()));

    assertTrue(this.store.getObject(this.mockRequestContext, OBJECT_1.getId(), FILTER_1).isEmpty());
    assertTrue(this.store.getData(this.mockRequestContext, OBJECT_1.getId(), FILTER_1).isEmpty());

    assertEquals(
        Optional.of(
            new ContextualConfigObjectImpl<>(
                OBJECT_1.getId(),
                OBJECT_1_FILTERED_BY_2,
                TEST_CREATE_TIMESTAMP_1,
                TEST_UPDATE_TIMESTAMP,
                TEST_CREATED_BY,
                TEST_LAST_MODIFIED_BY)),
        this.store.getObject(this.mockRequestContext, OBJECT_1.getId(), FILTER_2));
    assertEquals(
        Optional.of(OBJECT_1_FILTERED_BY_2),
        this.store.getData(this.mockRequestContext, OBJECT_1.getId(), FILTER_2));

    assertTrue(this.store.getObject(this.mockRequestContext, OBJECT_1.getId(), FILTER_3).isEmpty());
    assertTrue(this.store.getData(this.mockRequestContext, OBJECT_1.getId(), FILTER_3).isEmpty());

    verify(this.mockStub, atLeastOnce())
        .getConfig(
            GetConfigRequest.newBuilder()
                .setResourceName(TEST_RESOURCE_NAME)
                .setResourceNamespace(TEST_RESOURCE_NAMESPACE)
                .addContexts(OBJECT_1.getId())
                .build());
  }

  @lombok.Value
  @Builder
  private static class TestInternalFilter {
    Optional<Integer> minRank;
    Optional<Integer> minPoints;
  }

  @lombok.Value
  @Builder
  private static class TestInternalObject {
    String id;
    int rank;
    List<TestInternalNestedObject> nestedObjects;
  }

  @lombok.Value
  @Builder
  private static class TestInternalNestedObject {
    String id;
    int points;
  }

  @lombok.Value
  @Builder
  private static class TestApiObject {
    String api_id;
    int api_rank;
  }

  private static class TestObjectStore
      extends IdentifiedObjectStoreWithFilter<TestInternalObject, TestInternalFilter> {
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
              .nestedObjects(
                  value
                      .getStructValue()
                      .getFieldsOrThrow("nestedObjects")
                      .getListValue()
                      .getValuesList()
                      .stream()
                      .map(
                          val ->
                              TestInternalNestedObject.builder()
                                  .id(val.getStructValue().getFieldsOrThrow("id").getStringValue())
                                  .points(
                                      (int)
                                          val.getStructValue()
                                              .getFieldsOrThrow("points")
                                              .getNumberValue())
                                  .build())
                      .collect(Collectors.toUnmodifiableList()))
              .build());
    }

    @Override
    protected Value buildValueFromData(TestInternalObject object) {
      return convertToValue(object);
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

    @Override
    protected Optional<TestInternalObject> filterConfigData(
        TestInternalObject data, TestInternalFilter filter) {
      if (filter.getMinRank().isPresent()) {
        if (data.getRank() < filter.getMinRank().get()) {
          return Optional.empty();
        }
      }
      if (filter.getMinPoints().isPresent()) {
        int minPoints = filter.getMinPoints().get();
        return Optional.of(
            new TestInternalObject(
                data.getId(),
                data.getRank(),
                data.getNestedObjects().stream()
                    .filter(obj -> obj.points >= minPoints)
                    .collect(Collectors.toUnmodifiableList())));
      }
      return Optional.of(data);
    }
  }
}
