import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.Value;
import com.google.protobuf.util.Values;
import io.grpc.Status;
import java.util.Optional;
import org.hypertrace.config.objectstore.DefaultObjectStore;
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

  @Mock ConfigServiceBlockingStub mockStub;

  @Mock(answer = Answers.CALLS_REAL_METHODS)
  RequestContext mockRequestContext;

  DefaultObjectStore<TestObject> store;

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
        Optional.of(new TestObject("test")), this.store.getObject(this.mockRequestContext));

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
  void generatesConfigDeleteRequest() {

    when(this.mockStub.deleteConfig(any()))
        .thenReturn(
            DeleteConfigResponse.newBuilder()
                .setDeletedConfig(ContextSpecificConfig.newBuilder().setConfig(Values.of("test")))
                .build());
    assertEquals(new TestObject("test"), this.store.deleteObject(mockRequestContext));

    verify(this.mockStub)
        .deleteConfig(
            DeleteConfigRequest.newBuilder()
                .setResourceName(TEST_RESOURCE_NAME)
                .setResourceNamespace(TEST_RESOURCE_NAMESPACE)
                .build());
  }

  @Test
  void generatesConfigUpsertRequest() {
    when(this.mockStub.upsertConfig(any()))
        .thenReturn(UpsertConfigResponse.newBuilder().setConfig(Values.of("updated")).build());
    assertEquals(
        new TestObject("updated"),
        this.store.upsertObject(this.mockRequestContext, new TestObject("updated")));
    verify(this.mockStub, times(1))
        .upsertConfig(
            UpsertConfigRequest.newBuilder()
                .setResourceName(TEST_RESOURCE_NAME)
                .setResourceNamespace(TEST_RESOURCE_NAMESPACE)
                .setConfig(Values.of("updated"))
                .build());
  }

  @lombok.Value
  private static class TestObject {
    String name;
  }

  private static class TestObjectStore extends DefaultObjectStore<TestObject> {
    private TestObjectStore(ConfigServiceBlockingStub stub) {
      super(stub, TEST_RESOURCE_NAMESPACE, TEST_RESOURCE_NAME);
    }

    @Override
    protected Optional<TestObject> buildObjectFromValue(Value value) {
      return Optional.of(new TestObject(value.getStringValue()));
    }

    @Override
    protected Value buildValueFromObject(TestObject object) {
      return Values.of(object.getName());
    }
  }
}
