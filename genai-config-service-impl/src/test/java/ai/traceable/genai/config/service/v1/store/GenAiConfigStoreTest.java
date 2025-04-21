package ai.traceable.genai.config.service.v1.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.genai.config.service.v1.EnvironmentScope;
import ai.traceable.genai.config.service.v1.GenAiConfig;
import ai.traceable.genai.config.service.v1.GenAiScope;
import ai.traceable.genai.config.service.v1.IssuesSummaryFeatureConfig;
import com.google.protobuf.Value;
import java.util.Optional;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GenAiConfigStoreTest {

  @Mock private ConfigServiceGrpc.ConfigServiceBlockingStub stub;
  @Mock private ConfigChangeEventGenerator eventGenerator;
  @Mock private RequestContext requestContext;

  private GenAiConfigStore configStore;

  @BeforeEach
  void setUp() {
    GenAiConfigConverter converter = new GenAiConfigConverter();
    GenAiConfigIdGenerator idGenerator = new GenAiConfigIdGenerator(new UuidGenerator());
    configStore = new TestGenAiConfigStore(stub, eventGenerator, converter, idGenerator);
  }

  @Test
  void test_buildDataFromValue() {
    GenAiConfig config = createTestConfig();
    Value value = configStore.buildValueFromData(config);

    Optional<GenAiConfig> result = configStore.buildDataFromValue(value);

    assertTrue(result.isPresent());
    assertEquals(config, result.get());
  }

  @Test
  void test_buildValueFromData() {
    GenAiConfig config = createTestConfig();

    Value value = configStore.buildValueFromData(config);

    assertNotNull(value);
    assertTrue(value.hasStructValue());
    assertTrue(value.getStructValue().containsFields("scope"));
    assertTrue(value.getStructValue().containsFields("issuesSummaryFeatureConfig"));
  }

  @Test
  void test_getContextFromData() {
    GenAiConfig config = createTestConfig();

    String context = configStore.getContextFromData(config);

    assertNotNull(context);
    assertNotEquals("", context);

    String secondContext = configStore.getContextFromData(config);
    assertEquals(context, secondContext);
  }

  @Test
  void test_getGenAiConfigFromStore() {
    GenAiScope scope =
        GenAiScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().setEnvironmentId("test-env").build())
            .build();
    GenAiConfig config = createTestConfig().toBuilder().setScope(scope).build();

    ((TestGenAiConfigStore) configStore).setNextGetDataResult(config);

    Optional<GenAiConfig> result = configStore.getGenAiConfigFromStore(requestContext, scope);

    assertTrue(result.isPresent());
    assertEquals(config, result.get());
  }

  @Test
  void test_getGenAiConfigFromStore_withDefaultScope() {
    GenAiScope defaultScope = GenAiScope.getDefaultInstance();
    GenAiConfig config = createTestConfig().toBuilder().setScope(defaultScope).build();

    ((TestGenAiConfigStore) configStore).setNextGetDataResult(config);

    Optional<GenAiConfig> result =
        configStore.getGenAiConfigFromStore(requestContext, defaultScope);

    assertTrue(result.isPresent());
    assertEquals(config, result.get());
  }

  @Test
  void test_getGenAiConfigFromStore_nonExistentConfig() {
    GenAiScope scope =
        GenAiScope.newBuilder()
            .setEnvironmentScope(
                EnvironmentScope.newBuilder().setEnvironmentId("non-existent").build())
            .build();

    ((TestGenAiConfigStore) configStore).setNextGetDataResult(null);

    Optional<GenAiConfig> result = configStore.getGenAiConfigFromStore(requestContext, scope);

    assertFalse(result.isPresent());
  }

  private GenAiConfig createTestConfig() {
    return GenAiConfig.newBuilder()
        .setScope(GenAiScope.getDefaultInstance())
        .setIssuesSummaryFeatureConfig(
            IssuesSummaryFeatureConfig.newBuilder().setEnabled(true).build())
        .build();
  }

  private static class TestGenAiConfigStore extends GenAiConfigStore {
    private GenAiConfig nextGetDataResult = null;

    public TestGenAiConfigStore(
        ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
        ConfigChangeEventGenerator configChangeEventGenerator,
        GenAiConfigConverter configConverter,
        GenAiConfigIdGenerator configIdGenerator) {
      super(
          configServiceBlockingStub,
          configChangeEventGenerator,
          configConverter,
          configIdGenerator);
    }

    public void setNextGetDataResult(GenAiConfig result) {
      this.nextGetDataResult = result;
    }

    @Override
    public Optional<GenAiConfig> getData(RequestContext requestContext, String id) {
      return Optional.ofNullable(nextGetDataResult);
    }
  }
}
