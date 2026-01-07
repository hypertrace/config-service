package ai.traceable.data.obfuscation.config.service.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.data.obfuscation.config.service.v1.Argon2IdHash;
import ai.traceable.data.obfuscation.config.service.v1.CreateDataObfuscationStrategyRequest;
import ai.traceable.data.obfuscation.config.service.v1.HashStrategy;
import ai.traceable.data.obfuscation.config.service.v1.ObfuscationStrategy;
import ai.traceable.data.obfuscation.config.service.v1.ObfuscationStrategyInput;
import ai.traceable.data.obfuscation.config.service.v1.ScryptHash;
import ai.traceable.data.obfuscation.config.service.v1.UpdateDataObfuscationStrategyRequest;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ConfigManagerImplTest {

  private static final String TEST_TENANT_ID = "test-tenant-id";
  private static final String TEST_UUID = "test-uuid-123";
  private static final String TEST_STRATEGY_ID = "strategy-id-456";
  private static final String TEST_SALT = "test-salt";
  private static final HashStrategy STRATEGY_ARGON =
      HashStrategy.newBuilder()
          .setArgon2Id(
              Argon2IdHash.newBuilder()
                  .setMemory(1)
                  .setKeyLength(2)
                  .setThreads(3)
                  .setTime(4)
                  .build())
          .build();
  private static final HashStrategy STRATEGY_SCRYPT =
      HashStrategy.newBuilder()
          .setScrypt(
              ScryptHash.newBuilder()
                  .setBlockSize(1)
                  .setKeyLength(2)
                  .setThreads(3)
                  .setCost(4)
                  .build())
          .build();

  @Mock private DataObfuscationConfigStore dataObfuscationConfigStore;
  @Mock private UuidGenerator uuidGenerator;
  @Mock private RequestContext requestContext;
  @Mock private IdentifiedObjectStore<ObfuscationStrategy> upsertResult;

  private ConfigManagerImpl configManager;

  @BeforeEach
  void setUp() {
    configManager = new ConfigManagerImpl(dataObfuscationConfigStore, uuidGenerator);
  }

  @Test
  void testGetObfuscationStrategies_returnsAllStrategies() {
    ObfuscationStrategy strategy1 =
        ObfuscationStrategy.newBuilder()
            .setId("id1")
            .setHashStrategy(STRATEGY_ARGON)
            .setSalt("salt1")
            .build();
    ObfuscationStrategy strategy2 =
        ObfuscationStrategy.newBuilder()
            .setId("id2")
            .setHashStrategy(STRATEGY_SCRYPT)
            .setSalt("salt2")
            .build();
    List<ObfuscationStrategy> expectedStrategies = List.of(strategy1, strategy2);

    when(dataObfuscationConfigStore.getAllConfigData(requestContext))
        .thenReturn(expectedStrategies);

    List<ObfuscationStrategy> result = configManager.getObfuscationStrategies(requestContext);

    assertEquals(expectedStrategies, result);
    verify(dataObfuscationConfigStore).getAllConfigData(requestContext);
  }

  @Test
  void testGetObfuscationStrategies_returnsEmptyList() {
    when(dataObfuscationConfigStore.getAllConfigData(requestContext))
        .thenReturn(Collections.emptyList());

    List<ObfuscationStrategy> result = configManager.getObfuscationStrategies(requestContext);

    assertEquals(0, result.size());
    verify(dataObfuscationConfigStore).getAllConfigData(requestContext);
  }

  @Test
  void testCreateObfuscationStrategy_withCustomSalt_success() {
    when(dataObfuscationConfigStore.getAllConfigData(requestContext))
        .thenReturn(Collections.emptyList());
    when(uuidGenerator.generateRandomId()).thenReturn(TEST_UUID);

    ObfuscationStrategyInput input =
        ObfuscationStrategyInput.newBuilder()
            .setHashStrategy(STRATEGY_ARGON)
            .setSalt(TEST_SALT)
            .build();

    CreateDataObfuscationStrategyRequest request =
        CreateDataObfuscationStrategyRequest.newBuilder().setObfuscationStrategy(input).build();

    ObfuscationStrategy expectedStrategy =
        ObfuscationStrategy.newBuilder()
            .setId(TEST_UUID)
            .setHashStrategy(STRATEGY_ARGON)
            .setSalt(TEST_SALT)
            .build();

    when(dataObfuscationConfigStore.upsertObject(requestContext, expectedStrategy))
        .thenReturn(new MockContextualConfigObject(expectedStrategy));

    ObfuscationStrategy result = configManager.createObfuscationStrategy(requestContext, request);

    assertNotNull(result);
    assertEquals(TEST_UUID, result.getId());
    assertEquals(STRATEGY_ARGON, result.getHashStrategy());
    assertEquals(TEST_SALT, result.getSalt());
    verify(uuidGenerator).generateRandomId();
    verify(dataObfuscationConfigStore)
        .upsertObject(eq(requestContext), any(ObfuscationStrategy.class));
  }

  @Test
  void testCreateObfuscationStrategy_withEmptySalt_usesTenantId() {
    when(dataObfuscationConfigStore.getAllConfigData(requestContext))
        .thenReturn(Collections.emptyList());
    when(uuidGenerator.generateRandomId()).thenReturn(TEST_UUID);

    ObfuscationStrategyInput input =
        ObfuscationStrategyInput.newBuilder().setHashStrategy(STRATEGY_SCRYPT).setSalt("").build();

    CreateDataObfuscationStrategyRequest request =
        CreateDataObfuscationStrategyRequest.newBuilder().setObfuscationStrategy(input).build();

    ObfuscationStrategy expectedStrategy =
        ObfuscationStrategy.newBuilder()
            .setId(TEST_UUID)
            .setHashStrategy(STRATEGY_SCRYPT)
            .setSalt(TEST_TENANT_ID)
            .build();

    when(requestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    when(dataObfuscationConfigStore.upsertObject(requestContext, expectedStrategy))
        .thenReturn(new MockContextualConfigObject(expectedStrategy));

    ObfuscationStrategy result = configManager.createObfuscationStrategy(requestContext, request);

    assertNotNull(result);
    assertEquals(TEST_TENANT_ID, result.getSalt());
    verify(requestContext).getTenantId();
  }

  @Test
  void testCreateObfuscationStrategy_whenStrategyExists_throwsAlreadyExists() {
    ObfuscationStrategy existingStrategy =
        ObfuscationStrategy.newBuilder()
            .setId(TEST_STRATEGY_ID)
            .setHashStrategy(STRATEGY_ARGON)
            .setSalt(TEST_SALT)
            .build();

    when(dataObfuscationConfigStore.getAllConfigData(requestContext))
        .thenReturn(List.of(existingStrategy));

    ObfuscationStrategyInput input =
        ObfuscationStrategyInput.newBuilder()
            .setHashStrategy(STRATEGY_SCRYPT)
            .setSalt(TEST_SALT)
            .build();

    CreateDataObfuscationStrategyRequest request =
        CreateDataObfuscationStrategyRequest.newBuilder().setObfuscationStrategy(input).build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> configManager.createObfuscationStrategy(requestContext, request));

    assertEquals(Status.ALREADY_EXISTS.getCode(), exception.getStatus().getCode());
    assertEquals(
        "Data Obfuscation Strategy already exists", exception.getStatus().getDescription());
  }

  @Test
  void testUpdateObfuscationStrategy_withCustomSalt_success() {
    ObfuscationStrategy existingStrategy =
        ObfuscationStrategy.newBuilder()
            .setId(TEST_STRATEGY_ID)
            .setHashStrategy(STRATEGY_ARGON)
            .setSalt("old-salt")
            .build();

    when(dataObfuscationConfigStore.getAllConfigData(requestContext))
        .thenReturn(List.of(existingStrategy));

    ObfuscationStrategyInput input =
        ObfuscationStrategyInput.newBuilder()
            .setHashStrategy(STRATEGY_SCRYPT)
            .setSalt(TEST_SALT)
            .build();

    UpdateDataObfuscationStrategyRequest request =
        UpdateDataObfuscationStrategyRequest.newBuilder().setObfuscationStrategy(input).build();

    ObfuscationStrategy updatedStrategy =
        ObfuscationStrategy.newBuilder()
            .setId(TEST_STRATEGY_ID)
            .setHashStrategy(STRATEGY_SCRYPT)
            .setSalt(TEST_SALT)
            .build();

    when(dataObfuscationConfigStore.upsertObject(requestContext, updatedStrategy))
        .thenReturn(new MockContextualConfigObject(updatedStrategy));

    ObfuscationStrategy result = configManager.updateObfuscationStrategy(requestContext, request);

    assertNotNull(result);
    assertEquals(TEST_STRATEGY_ID, result.getId());
    assertEquals(STRATEGY_SCRYPT, result.getHashStrategy());
    assertEquals(TEST_SALT, result.getSalt());
    verify(dataObfuscationConfigStore)
        .upsertObject(eq(requestContext), any(ObfuscationStrategy.class));
  }

  @Test
  void testUpdateObfuscationStrategy_withEmptySalt_usesTenantId() {
    ObfuscationStrategy existingStrategy =
        ObfuscationStrategy.newBuilder()
            .setId(TEST_STRATEGY_ID)
            .setHashStrategy(STRATEGY_ARGON)
            .setSalt("old-salt")
            .build();

    when(dataObfuscationConfigStore.getAllConfigData(requestContext))
        .thenReturn(List.of(existingStrategy));

    ObfuscationStrategyInput input =
        ObfuscationStrategyInput.newBuilder().setHashStrategy(STRATEGY_SCRYPT).setSalt("").build();

    UpdateDataObfuscationStrategyRequest request =
        UpdateDataObfuscationStrategyRequest.newBuilder().setObfuscationStrategy(input).build();

    ObfuscationStrategy updatedStrategy =
        ObfuscationStrategy.newBuilder()
            .setId(TEST_STRATEGY_ID)
            .setHashStrategy(STRATEGY_SCRYPT)
            .setSalt(TEST_TENANT_ID)
            .build();

    when(requestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    when(dataObfuscationConfigStore.upsertObject(requestContext, updatedStrategy))
        .thenReturn(new MockContextualConfigObject(updatedStrategy));

    ObfuscationStrategy result = configManager.updateObfuscationStrategy(requestContext, request);

    assertNotNull(result);
    assertEquals(TEST_TENANT_ID, result.getSalt());
    verify(requestContext).getTenantId();
  }

  @Test
  void testUpdateObfuscationStrategy_whenStrategyNotFound_throwsNotFound() {
    when(dataObfuscationConfigStore.getAllConfigData(requestContext))
        .thenReturn(Collections.emptyList());

    ObfuscationStrategyInput input =
        ObfuscationStrategyInput.newBuilder()
            .setHashStrategy(STRATEGY_SCRYPT)
            .setSalt(TEST_SALT)
            .build();

    UpdateDataObfuscationStrategyRequest request =
        UpdateDataObfuscationStrategyRequest.newBuilder().setObfuscationStrategy(input).build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> configManager.updateObfuscationStrategy(requestContext, request));

    assertEquals(Status.NOT_FOUND.getCode(), exception.getStatus().getCode());
    assertEquals("Data Obfuscation Strategy not found", exception.getStatus().getDescription());
  }

  @Test
  void testDeleteObfuscationStrategy_success() {
    ObfuscationStrategy existingStrategy =
        ObfuscationStrategy.newBuilder()
            .setId(TEST_STRATEGY_ID)
            .setHashStrategy(STRATEGY_SCRYPT)
            .setSalt(TEST_SALT)
            .build();

    when(dataObfuscationConfigStore.getAllConfigData(requestContext))
        .thenReturn(List.of(existingStrategy));
    when(dataObfuscationConfigStore.deleteObject(requestContext, TEST_STRATEGY_ID))
        .thenReturn(Optional.of(new MockDeletedContextualConfigObject(existingStrategy)));

    configManager.deleteObfuscationStrategy(requestContext);

    verify(dataObfuscationConfigStore).deleteObject(requestContext, TEST_STRATEGY_ID);
  }

  @Test
  void testDeleteObfuscationStrategy_whenStrategyNotFound_throwsNotFound() {
    when(dataObfuscationConfigStore.getAllConfigData(requestContext))
        .thenReturn(Collections.emptyList());

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> configManager.deleteObfuscationStrategy(requestContext));

    assertEquals(Status.NOT_FOUND.getCode(), exception.getStatus().getCode());
  }

  @Test
  void testDeleteObfuscationStrategy_whenDeleteFails_throwsNotFound() {
    ObfuscationStrategy existingStrategy =
        ObfuscationStrategy.newBuilder()
            .setId(TEST_STRATEGY_ID)
            .setHashStrategy(STRATEGY_SCRYPT)
            .setSalt(TEST_SALT)
            .build();

    when(dataObfuscationConfigStore.getAllConfigData(requestContext))
        .thenReturn(List.of(existingStrategy));
    when(dataObfuscationConfigStore.deleteObject(requestContext, TEST_STRATEGY_ID))
        .thenReturn(Optional.empty());

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> configManager.deleteObfuscationStrategy(requestContext));

    assertEquals(Status.NOT_FOUND.getCode(), exception.getStatus().getCode());
    verify(dataObfuscationConfigStore).deleteObject(requestContext, TEST_STRATEGY_ID);
  }
}
