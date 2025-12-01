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
import java.util.Optional;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.config.service.v1.ContextSpecificConfig;
import org.hypertrace.config.service.v1.DeleteConfigRequest;
import org.hypertrace.config.service.v1.DeleteConfigResponse;
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
class DefaultObjectStoreTest {
  private static final String TEST_RESOURCE_NAMESPACE = "test-namespace";
  private static final String TEST_RESOURCE_NAME = "test-resource";
  private static final Instant TEST_CREATE_TIMESTAMP = Instant.ofEpochMilli(20);
  private static final Instant TEST_UPDATE_TIMESTAMP = Instant.ofEpochMilli(40);
  private static final String TEST_CREATED_BY = "test-created-by";
  private static final String TEST_LAST_MODIFIED_BY = "test-last-modified-by";

  @Mock(answer = Answers.RETURNS_SELF)
  ConfigServiceBlockingStub mockStub;

  @Mock ConfigChangeEventGenerator configChangeEventGenerator;

  @Mock(answer = Answers.CALLS_REAL_METHODS)
  RequestContext mockRequestContext;

  DefaultObjectStore<TestInternalObject> store;

  @BeforeEach
  void beforeEach() {
    this.store = new TestObjectStore(this.mockStub, configChangeEventGenerator);
  }

  @Test
  void generatesConfigReadRequestForGetObject() {
    when(this.mockStub.getConfig(any()))
        .thenReturn(
            GetConfigResponse.newBuilder()
                .setConfig(Values.of("test"))
                .setCreationTimestamp(TEST_CREATE_TIMESTAMP.toEpochMilli())
                .setUpdateTimestamp(TEST_UPDATE_TIMESTAMP.toEpochMilli())
                .setCreatedBy(TEST_CREATED_BY)
                .setLastModifiedBy(TEST_LAST_MODIFIED_BY)
                .build());

    assertEquals(
        Optional.of(
            new ConfigObjectImpl<>(
                new TestInternalObject("test"),
                TEST_CREATE_TIMESTAMP,
                TEST_UPDATE_TIMESTAMP,
                TEST_CREATED_BY,
                TEST_LAST_MODIFIED_BY)),
        this.store.getObject(this.mockRequestContext));

    verify(this.mockStub, times(1))
        .getConfig(
            GetConfigRequest.newBuilder()
                .setResourceName(TEST_RESOURCE_NAME)
                .setResourceNamespace(TEST_RESOURCE_NAMESPACE)
                .build());

    when(this.mockStub.getConfig(any())).thenThrow(Status.NOT_FOUND.asRuntimeException());

    assertEquals(Optional.empty(), this.store.getObject(this.mockRequestContext));

    verify(this.mockStub, times(2))
        .getConfig(
            GetConfigRequest.newBuilder()
                .setResourceName(TEST_RESOURCE_NAME)
                .setResourceNamespace(TEST_RESOURCE_NAMESPACE)
                .build());
  }

  @Test
  void generatesConfigReadRequestForGet() {
    when(this.mockStub.getConfig(any()))
        .thenReturn(GetConfigResponse.newBuilder().setConfig(Values.of("test")).build());

    assertEquals(
        Optional.of(new TestInternalObject("test")), this.store.getData(this.mockRequestContext));

    verify(this.mockStub, times(1))
        .getConfig(
            GetConfigRequest.newBuilder()
                .setResourceName(TEST_RESOURCE_NAME)
                .setResourceNamespace(TEST_RESOURCE_NAMESPACE)
                .build());

    when(this.mockStub.getConfig(any())).thenThrow(Status.NOT_FOUND.asRuntimeException());

    assertEquals(Optional.empty(), this.store.getData(this.mockRequestContext));

    verify(this.mockStub, times(2))
        .getConfig(
            GetConfigRequest.newBuilder()
                .setResourceName(TEST_RESOURCE_NAME)
                .setResourceNamespace(TEST_RESOURCE_NAMESPACE)
                .build());
  }

  @Test
  void generatesConfigDeleteRequest() {
    when(this.mockStub.deleteConfig(any()))
        .thenReturn(
            DeleteConfigResponse.newBuilder()
                .setDeletedConfig(
                    ContextSpecificConfig.newBuilder()
                        .setConfig(Values.of("test"))
                        .setCreationTimestamp(TEST_CREATE_TIMESTAMP.toEpochMilli())
                        .setUpdateTimestamp(TEST_UPDATE_TIMESTAMP.toEpochMilli())
                        .setCreatedBy(TEST_CREATED_BY)
                        .setLastModifiedBy(TEST_LAST_MODIFIED_BY))
                .build());
    assertEquals(
        Optional.of(
            new ConfigObjectImpl<>(
                new TestInternalObject("test"),
                TEST_CREATE_TIMESTAMP,
                TEST_UPDATE_TIMESTAMP,
                TEST_CREATED_BY,
                TEST_LAST_MODIFIED_BY)),
        this.store.deleteObject(mockRequestContext));

    verify(this.mockStub)
        .deleteConfig(
            DeleteConfigRequest.newBuilder()
                .setResourceName(TEST_RESOURCE_NAME)
                .setResourceNamespace(TEST_RESOURCE_NAMESPACE)
                .build());

    when(this.mockStub.deleteConfig(any())).thenThrow(Status.NOT_FOUND.asRuntimeException());

    assertEquals(Optional.empty(), this.store.deleteObject(mockRequestContext));

    verify(this.configChangeEventGenerator, times(1))
        .sendDeleteNotification(
            eq(this.mockRequestContext),
            eq(TestApiObject.class.getName()),
            eq(
                Value.newBuilder()
                    .setStructValue(Struct.newBuilder().putFields("api_name", Values.of("test")))
                    .build()));
  }

  @Test
  void generatesConfigUpsertRequest() {
    when(this.mockStub.upsertConfig(any()))
        .thenReturn(
            UpsertConfigResponse.newBuilder()
                .setCreationTimestamp(TEST_CREATE_TIMESTAMP.toEpochMilli())
                .setUpdateTimestamp(TEST_UPDATE_TIMESTAMP.toEpochMilli())
                .setCreatedBy(TEST_CREATED_BY)
                .setLastModifiedBy(TEST_LAST_MODIFIED_BY)
                .setConfig(Values.of("updated"))
                .build());
    ConfigObject configObject =
        new ConfigObjectImpl<>(
            new TestInternalObject("updated"),
            TEST_CREATE_TIMESTAMP,
            TEST_UPDATE_TIMESTAMP,
            TEST_CREATED_BY,
            TEST_LAST_MODIFIED_BY);
    assertEquals(
        configObject,
        this.store.upsertObject(this.mockRequestContext, new TestInternalObject("updated")));
    verify(this.mockStub, times(1))
        .upsertConfig(
            UpsertConfigRequest.newBuilder()
                .setResourceName(TEST_RESOURCE_NAME)
                .setResourceNamespace(TEST_RESOURCE_NAMESPACE)
                .setConfig(Values.of("updated"))
                .build());
    verify(this.configChangeEventGenerator, times(1))
        .sendCreateNotification(
            eq(this.mockRequestContext),
            eq(TestApiObject.class.getName()),
            eq(
                Value.newBuilder()
                    .setStructValue(Struct.newBuilder().putFields("api_name", Values.of("updated")))
                    .build()));
  }

  @lombok.Value
  private static class TestInternalObject {
    String name;
  }

  @lombok.Value
  private static class TestApiObject {
    String api_name;
  }

  private static class TestObjectStore extends DefaultObjectStore<TestInternalObject> {
    private TestObjectStore(
        ConfigServiceBlockingStub stub, ConfigChangeEventGenerator configChangeEventGenerator) {
      super(stub, TEST_RESOURCE_NAMESPACE, TEST_RESOURCE_NAME, configChangeEventGenerator);
    }

    @Override
    protected Optional<TestInternalObject> buildDataFromValue(Value value) {
      return Optional.of(new TestInternalObject(value.getStringValue()));
    }

    @Override
    protected Value buildValueFromData(TestInternalObject object) {
      return Values.of(object.getName());
    }

    @Override
    protected Value buildValueForChangeEvent(TestInternalObject object) {
      return Value.newBuilder()
          .setStructValue(Struct.newBuilder().putFields("api_name", Values.of(object.getName())))
          .build();
    }

    @Override
    protected String buildClassNameForChangeEvent(TestInternalObject object) {
      return TestApiObject.class.getName();
    }
  }
}
