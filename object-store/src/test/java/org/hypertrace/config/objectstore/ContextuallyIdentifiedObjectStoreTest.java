package org.hypertrace.config.objectstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.Value;
import com.google.protobuf.util.Values;
import io.grpc.Status;
import java.time.Instant;
import java.util.Optional;
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
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ContextuallyIdentifiedObjectStoreTest {
  private static final String TEST_RESOURCE_NAMESPACE = "test-namespace";
  private static final String TEST_RESOURCE_NAME = "test-resource";
  private static final Instant TEST_CREATE_TIMESTAMP = Instant.ofEpochMilli(20);
  private static final Instant TEST_UPDATE_TIMESTAMP = Instant.ofEpochMilli(40);

  @Mock ConfigServiceBlockingStub mockStub;

  ContextuallyIdentifiedObjectStore<TestObject> store;

  @BeforeEach
  void beforeEach() {
    this.mockStub = mock(ConfigServiceBlockingStub.class);
    this.store = new TestObjectStore(this.mockStub);
  }

  @Test
  void generatesConfigReadRequestForGet() {
    when(this.mockStub.getConfig(any()))
        .thenReturn(GetConfigResponse.newBuilder().setConfig(Values.of("test")).build());
    assertEquals(
        Optional.of(new TestObject("test")),
        this.store.getData(RequestContext.forTenantId("my-tenant")));

    verify(this.mockStub, times(1))
        .getConfig(
            GetConfigRequest.newBuilder()
                .setResourceName(TEST_RESOURCE_NAME)
                .setResourceNamespace(TEST_RESOURCE_NAMESPACE)
                .addContexts("my-tenant")
                .build());

    when(this.mockStub.getConfig(any())).thenThrow(Status.NOT_FOUND.asRuntimeException());

    assertEquals(
        Optional.empty(), this.store.getData(RequestContext.forTenantId("my-other-tenant")));

    verify(this.mockStub, times(1))
        .getConfig(
            GetConfigRequest.newBuilder()
                .setResourceName(TEST_RESOURCE_NAME)
                .setResourceNamespace(TEST_RESOURCE_NAMESPACE)
                .addContexts("my-other-tenant")
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
                        .setContext("delete-tenant")
                        .setCreationTimestamp(TEST_CREATE_TIMESTAMP.toEpochMilli())
                        .setUpdateTimestamp(TEST_UPDATE_TIMESTAMP.toEpochMilli()))
                .build());
    assertEquals(
        Optional.of(
            new ContextualConfigObjectImpl<>(
                "delete-tenant",
                new TestObject("test"),
                TEST_CREATE_TIMESTAMP,
                TEST_UPDATE_TIMESTAMP)),
        this.store.deleteObject(RequestContext.forTenantId("delete-tenant")));

    verify(this.mockStub)
        .deleteConfig(
            DeleteConfigRequest.newBuilder()
                .setResourceName(TEST_RESOURCE_NAME)
                .setResourceNamespace(TEST_RESOURCE_NAMESPACE)
                .setContext("delete-tenant")
                .build());

    when(this.mockStub.deleteConfig(any())).thenThrow(Status.NOT_FOUND.asRuntimeException());

    assertEquals(
        Optional.empty(), this.store.deleteObject(RequestContext.forTenantId("delete-tenant")));
  }

  @Test
  void generatesConfigUpsertRequest() {
    when(this.mockStub.upsertConfig(any()))
        .thenReturn(
            UpsertConfigResponse.newBuilder()
                .setConfig(Values.of("updated"))
                .setCreationTimestamp(TEST_CREATE_TIMESTAMP.toEpochMilli())
                .setUpdateTimestamp(TEST_UPDATE_TIMESTAMP.toEpochMilli())
                .build());
    assertEquals(
        new ContextualConfigObjectImpl<>(
            "upsert-tenant",
            new TestObject("updated"),
            TEST_CREATE_TIMESTAMP,
            TEST_UPDATE_TIMESTAMP),
        this.store.upsertObject(
            RequestContext.forTenantId("upsert-tenant"), new TestObject("updated")));
    verify(this.mockStub, times(1))
        .upsertConfig(
            UpsertConfigRequest.newBuilder()
                .setResourceName(TEST_RESOURCE_NAME)
                .setResourceNamespace(TEST_RESOURCE_NAMESPACE)
                .setContext("upsert-tenant")
                .setConfig(Values.of("updated"))
                .build());
  }

  @lombok.Value
  private static class TestObject {
    String name;
  }

  private static class TestObjectStore extends ContextuallyIdentifiedObjectStore<TestObject> {
    private TestObjectStore(ConfigServiceBlockingStub stub) {
      super(stub, TEST_RESOURCE_NAMESPACE, TEST_RESOURCE_NAME);
    }

    @Override
    protected Optional<TestObject> buildDataFromValue(Value value) {
      return Optional.of(new TestObject(value.getStringValue()));
    }

    @Override
    protected Value buildValueFromData(TestObject object) {
      return Values.of(object.getName());
    }

    @Override
    protected Value buildValueForChangeEvent(TestObject object) {
      return Values.of(object.getName());
    }

    @Override
    protected String getConfigContextFromRequestContext(RequestContext requestContext) {
      return requestContext.getTenantId().orElseThrow();
    }
  }
}
