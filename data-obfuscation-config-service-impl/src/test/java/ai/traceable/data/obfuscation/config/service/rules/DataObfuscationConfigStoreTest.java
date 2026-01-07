package ai.traceable.data.obfuscation.config.service.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.data.obfuscation.config.service.v1.HashStrategy;
import ai.traceable.data.obfuscation.config.service.v1.ObfuscationStrategy;
import ai.traceable.data.obfuscation.config.service.v1.ScryptHash;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.Optional;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DataObfuscationConfigStoreTest {

  private static final String TEST_STRATEGY_ID = "test-strategy-id";
  private static final String TEST_SALT = "test-salt";
  private static final HashStrategy STRATEGY_SCRYPT =
      HashStrategy.newBuilder()
          .setScrypt(
              ScryptHash.newBuilder().setCost(1).setBlockSize(2).setThreads(3).setKeyLength(5))
          .build();

  @Mock private ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub;
  @Mock private ConfigChangeEventGenerator configChangeEventGenerator;

  private DataObfuscationConfigStore dataObfuscationConfigStore;

  @BeforeEach
  void setUp() {
    dataObfuscationConfigStore =
        new DataObfuscationConfigStore(configServiceBlockingStub, configChangeEventGenerator);
  }

  @Test
  void testFilterConfigData_returnsData() {
    ObfuscationStrategy strategy =
        ObfuscationStrategy.newBuilder()
            .setId(TEST_STRATEGY_ID)
            .setHashStrategy(STRATEGY_SCRYPT)
            .setSalt(TEST_SALT)
            .build();

    Object filter = new Object();

    Optional<ObfuscationStrategy> result =
        dataObfuscationConfigStore.filterConfigData(strategy, filter);

    assertTrue(result.isPresent());
    assertEquals(strategy, result.get());
  }

  @Test
  void testFilterConfigData_withNullFilter_returnsData() {
    ObfuscationStrategy strategy =
        ObfuscationStrategy.newBuilder()
            .setId(TEST_STRATEGY_ID)
            .setHashStrategy(STRATEGY_SCRYPT)
            .setSalt(TEST_SALT)
            .build();

    Optional<ObfuscationStrategy> result =
        dataObfuscationConfigStore.filterConfigData(strategy, null);

    assertTrue(result.isPresent());
    assertEquals(strategy, result.get());
  }

  @Test
  void testBuildDataFromValue_validValue_success() throws InvalidProtocolBufferException {
    ObfuscationStrategy strategy =
        ObfuscationStrategy.newBuilder()
            .setId(TEST_STRATEGY_ID)
            .setHashStrategy(STRATEGY_SCRYPT)
            .setSalt(TEST_SALT)
            .build();

    Value value = ConfigProtoConverter.convertToValue(strategy);

    Optional<ObfuscationStrategy> result = dataObfuscationConfigStore.buildDataFromValue(value);

    assertTrue(result.isPresent());
    assertEquals(TEST_STRATEGY_ID, result.get().getId());
    assertEquals(TEST_SALT, result.get().getSalt());
    assertEquals(strategy.getHashStrategy(), result.get().getHashStrategy());
  }

  @Test
  void testBuildDataFromValue_invalidValue_returnsEmpty() {
    Value invalidValue = Value.newBuilder().setStringValue("invalid-proto-data").build();

    Optional<ObfuscationStrategy> result =
        dataObfuscationConfigStore.buildDataFromValue(invalidValue);

    assertFalse(result.isPresent());
  }

  @Test
  void testBuildValueFromData_validData_success() throws InvalidProtocolBufferException {
    ObfuscationStrategy strategy =
        ObfuscationStrategy.newBuilder()
            .setId(TEST_STRATEGY_ID)
            .setHashStrategy(STRATEGY_SCRYPT)
            .setSalt(TEST_SALT)
            .build();

    Value result = dataObfuscationConfigStore.buildValueFromData(strategy);

    ObfuscationStrategy.Builder builder = ObfuscationStrategy.newBuilder();
    ConfigProtoConverter.mergeFromValue(result, builder);
    ObfuscationStrategy reconstructed = builder.build();

    assertEquals(TEST_STRATEGY_ID, reconstructed.getId());
    assertEquals(TEST_SALT, reconstructed.getSalt());
    assertEquals(strategy.getHashStrategy(), reconstructed.getHashStrategy());
  }

  @Test
  void testBuildValueFromData_emptyStrategy_returnsValue() {
    ObfuscationStrategy emptyStrategy = ObfuscationStrategy.newBuilder().build();

    Value result = dataObfuscationConfigStore.buildValueFromData(emptyStrategy);

    assertTrue(result.hasStructValue());
  }

  @Test
  void testGetContextFromData_returnsStrategyId() {
    ObfuscationStrategy strategy =
        ObfuscationStrategy.newBuilder()
            .setId(TEST_STRATEGY_ID)
            .setHashStrategy(STRATEGY_SCRYPT)
            .setSalt(TEST_SALT)
            .build();

    String context = dataObfuscationConfigStore.getContextFromData(strategy);

    assertEquals(TEST_STRATEGY_ID, context);
  }

  @Test
  void testGetContextFromData_emptyId_returnsEmptyString() {
    ObfuscationStrategy strategy = ObfuscationStrategy.newBuilder().setId("").build();

    String context = dataObfuscationConfigStore.getContextFromData(strategy);

    assertEquals("", context);
  }

  @Test
  void testConstants() {
    assertEquals("dataObfuscation", DataObfuscationConfigStore.DATA_OBFUSCATION_CONFIG_NAMESPACE);
    assertEquals(
        "dataObfuscationConfig", DataObfuscationConfigStore.DATA_OBFUSCATION_CONFIG_RESOURCE_NAME);
  }
}
