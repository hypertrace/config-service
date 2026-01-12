package ai.traceable.external.data.classification.config.service.obfuscation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.data.obfuscation.config.service.v1.DataObfuscationConfigServiceGrpc.DataObfuscationConfigServiceBlockingStub;
import ai.traceable.data.obfuscation.config.service.v1.GetDataObfuscationStrategyRequest;
import ai.traceable.data.obfuscation.config.service.v1.GetDataObfuscationStrategyResponse;
import ai.traceable.data.obfuscation.config.service.v1.HashStrategy;
import ai.traceable.external.data.classification.config.service.v1.ObfuscationStrategy;
import ai.traceable.external.data.classification.config.service.v1.Sha256Hash;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DataObfuscationRulesManagerTest {

  private static final String TEST_TENANT_ID = "test-tenant-id";
  private static final String TEST_SALT = "test-salt";

  @Mock private DataObfuscationConfigServiceBlockingStub dataObfuscationConfigServiceBlockingStub;

  private DataObfuscationRulesManager manager;
  private RequestContext requestContext;

  @BeforeEach
  void setUp() {
    manager = new DataObfuscationRulesManager(dataObfuscationConfigServiceBlockingStub);
    requestContext = RequestContext.forTenantId(TEST_TENANT_ID);
  }

  @Test
  void testGetObfuscationStrategy_emptyResponse_returnsSha256WithTenantId() {
    GetDataObfuscationStrategyResponse emptyResponse =
        GetDataObfuscationStrategyResponse.newBuilder().build();

    when(dataObfuscationConfigServiceBlockingStub.getDataObfuscationStrategy(
            any(GetDataObfuscationStrategyRequest.class)))
        .thenReturn(emptyResponse);

    ObfuscationStrategy result = manager.getObfuscationStrategy(requestContext);

    assertEquals(TEST_TENANT_ID, result.getSalt());
    assertTrue(result.hasSha256());
    assertEquals(ObfuscationStrategy.HashFunction.HASH_FUNCTION_SHA256, result.getHashFunction());
    assertEquals(Sha256Hash.getDefaultInstance(), result.getSha256());
    verify(dataObfuscationConfigServiceBlockingStub)
        .getDataObfuscationStrategy(any(GetDataObfuscationStrategyRequest.class));
  }

  @Test
  void testGetObfuscationStrategy_scryptHash_success() {
    ai.traceable.data.obfuscation.config.service.v1.ScryptHash scryptHash =
        ai.traceable.data.obfuscation.config.service.v1.ScryptHash.newBuilder()
            .setBlockSize(8)
            .setCost(16384)
            .setKeyLength(32)
            .setThreads(1)
            .build();

    ai.traceable.data.obfuscation.config.service.v1.ObfuscationStrategy configStrategy =
        ai.traceable.data.obfuscation.config.service.v1.ObfuscationStrategy.newBuilder()
            .setId("strategy-id")
            .setHashStrategy(HashStrategy.newBuilder().setScrypt(scryptHash).build())
            .setSalt(TEST_SALT)
            .build();

    GetDataObfuscationStrategyResponse response =
        GetDataObfuscationStrategyResponse.newBuilder()
            .addObfuscationStrategies(configStrategy)
            .build();

    when(dataObfuscationConfigServiceBlockingStub.getDataObfuscationStrategy(
            any(GetDataObfuscationStrategyRequest.class)))
        .thenReturn(response);

    ObfuscationStrategy result = manager.getObfuscationStrategy(requestContext);

    assertEquals(TEST_SALT, result.getSalt());
    assertTrue(result.hasScrypt());
    assertEquals(8, result.getScrypt().getBlockSize());
    assertEquals(16384, result.getScrypt().getCost());
    assertEquals(32, result.getScrypt().getKeyLength());
    assertEquals(1, result.getScrypt().getThreads());
    assertEquals(ObfuscationStrategy.HashFunction.HASH_FUNCTION_SHA256, result.getHashFunction());
  }

  @Test
  void testGetObfuscationStrategy_argon2IdHash_success() {
    ai.traceable.data.obfuscation.config.service.v1.Argon2IdHash argon2IdHash =
        ai.traceable.data.obfuscation.config.service.v1.Argon2IdHash.newBuilder()
            .setKeyLength(32)
            .setThreads(4)
            .setTime(3)
            .setMemory(65536)
            .build();

    ai.traceable.data.obfuscation.config.service.v1.ObfuscationStrategy configStrategy =
        ai.traceable.data.obfuscation.config.service.v1.ObfuscationStrategy.newBuilder()
            .setId("strategy-id")
            .setHashStrategy(HashStrategy.newBuilder().setArgon2Id(argon2IdHash).build())
            .setSalt(TEST_SALT)
            .build();

    GetDataObfuscationStrategyResponse response =
        GetDataObfuscationStrategyResponse.newBuilder()
            .addObfuscationStrategies(configStrategy)
            .build();

    when(dataObfuscationConfigServiceBlockingStub.getDataObfuscationStrategy(
            any(GetDataObfuscationStrategyRequest.class)))
        .thenReturn(response);

    ObfuscationStrategy result = manager.getObfuscationStrategy(requestContext);

    assertEquals(TEST_SALT, result.getSalt());
    assertTrue(result.hasArgon2Id());
    assertEquals(32, result.getArgon2Id().getKeyLength());
    assertEquals(4, result.getArgon2Id().getThreads());
    assertEquals(3, result.getArgon2Id().getTime());
    assertEquals(65536, result.getArgon2Id().getMemory());
    assertEquals(ObfuscationStrategy.HashFunction.HASH_FUNCTION_SHA256, result.getHashFunction());
  }

  @Test
  void testGetObfuscationStrategy_sha256Hash_success() {
    ai.traceable.data.obfuscation.config.service.v1.Sha256Hash sha256Hash =
        ai.traceable.data.obfuscation.config.service.v1.Sha256Hash.newBuilder().build();

    ai.traceable.data.obfuscation.config.service.v1.ObfuscationStrategy configStrategy =
        ai.traceable.data.obfuscation.config.service.v1.ObfuscationStrategy.newBuilder()
            .setId("strategy-id")
            .setHashStrategy(HashStrategy.newBuilder().setSha256(sha256Hash).build())
            .setSalt(TEST_SALT)
            .build();

    GetDataObfuscationStrategyResponse response =
        GetDataObfuscationStrategyResponse.newBuilder()
            .addObfuscationStrategies(configStrategy)
            .build();

    when(dataObfuscationConfigServiceBlockingStub.getDataObfuscationStrategy(
            any(GetDataObfuscationStrategyRequest.class)))
        .thenReturn(response);

    ObfuscationStrategy result = manager.getObfuscationStrategy(requestContext);

    assertEquals(TEST_SALT, result.getSalt());
    assertTrue(result.hasSha256());
    assertEquals(ObfuscationStrategy.HashFunction.HASH_FUNCTION_SHA256, result.getHashFunction());
  }

  @Test
  void testGetObfuscationStrategy_sha1Hash_success() {
    ai.traceable.data.obfuscation.config.service.v1.Sha1Hash sha1Hash =
        ai.traceable.data.obfuscation.config.service.v1.Sha1Hash.newBuilder().build();

    ai.traceable.data.obfuscation.config.service.v1.ObfuscationStrategy configStrategy =
        ai.traceable.data.obfuscation.config.service.v1.ObfuscationStrategy.newBuilder()
            .setId("strategy-id")
            .setHashStrategy(HashStrategy.newBuilder().setSha1(sha1Hash).build())
            .setSalt(TEST_SALT)
            .build();

    GetDataObfuscationStrategyResponse response =
        GetDataObfuscationStrategyResponse.newBuilder()
            .addObfuscationStrategies(configStrategy)
            .build();

    when(dataObfuscationConfigServiceBlockingStub.getDataObfuscationStrategy(
            any(GetDataObfuscationStrategyRequest.class)))
        .thenReturn(response);

    ObfuscationStrategy result = manager.getObfuscationStrategy(requestContext);

    assertEquals(TEST_SALT, result.getSalt());
    assertTrue(result.hasSha1());
    assertEquals(ObfuscationStrategy.HashFunction.HASH_FUNCTION_SHA256, result.getHashFunction());
  }

  @Test
  void testGetObfuscationStrategy_unknownHashStrategy_fallsBackToSha1() {
    ai.traceable.data.obfuscation.config.service.v1.ObfuscationStrategy configStrategy =
        ai.traceable.data.obfuscation.config.service.v1.ObfuscationStrategy.newBuilder()
            .setId("strategy-id")
            .setHashStrategy(HashStrategy.newBuilder().build())
            .setSalt(TEST_SALT)
            .build();

    GetDataObfuscationStrategyResponse response =
        GetDataObfuscationStrategyResponse.newBuilder()
            .addObfuscationStrategies(configStrategy)
            .build();

    when(dataObfuscationConfigServiceBlockingStub.getDataObfuscationStrategy(
            any(GetDataObfuscationStrategyRequest.class)))
        .thenReturn(response);

    ObfuscationStrategy result = manager.getObfuscationStrategy(requestContext);

    assertEquals(TEST_SALT, result.getSalt());
    assertTrue(result.hasSha1());
    assertEquals(ObfuscationStrategy.HashFunction.HASH_FUNCTION_SHA256, result.getHashFunction());
  }

  @Test
  void testGetObfuscationStrategy_backwardsCompatibility_setsHashFunctionAndSalt() {

    GetDataObfuscationStrategyResponse response =
        GetDataObfuscationStrategyResponse.newBuilder().build();

    when(dataObfuscationConfigServiceBlockingStub.getDataObfuscationStrategy(
            any(GetDataObfuscationStrategyRequest.class)))
        .thenReturn(response);

    ObfuscationStrategy result = manager.getObfuscationStrategy(requestContext);

    assertEquals(ObfuscationStrategy.HashFunction.HASH_FUNCTION_SHA256, result.getHashFunction());
    assertEquals(TEST_TENANT_ID, result.getSalt());
  }

  @Test
  void testGetObfuscationStrategy_multipleStrategies_usesFirst() {
    ai.traceable.data.obfuscation.config.service.v1.Sha256Hash sha256Hash =
        ai.traceable.data.obfuscation.config.service.v1.Sha256Hash.newBuilder().build();

    ai.traceable.data.obfuscation.config.service.v1.ObfuscationStrategy firstStrategy =
        ai.traceable.data.obfuscation.config.service.v1.ObfuscationStrategy.newBuilder()
            .setId("first-strategy-id")
            .setHashStrategy(HashStrategy.newBuilder().setSha256(sha256Hash).build())
            .setSalt("first-salt")
            .build();

    ai.traceable.data.obfuscation.config.service.v1.ObfuscationStrategy secondStrategy =
        ai.traceable.data.obfuscation.config.service.v1.ObfuscationStrategy.newBuilder()
            .setId("second-strategy-id")
            .setHashStrategy(HashStrategy.newBuilder().setSha256(sha256Hash).build())
            .setSalt("second-salt")
            .build();

    GetDataObfuscationStrategyResponse response =
        GetDataObfuscationStrategyResponse.newBuilder()
            .addObfuscationStrategies(firstStrategy)
            .addObfuscationStrategies(secondStrategy)
            .build();

    when(dataObfuscationConfigServiceBlockingStub.getDataObfuscationStrategy(
            any(GetDataObfuscationStrategyRequest.class)))
        .thenReturn(response);

    ObfuscationStrategy result = manager.getObfuscationStrategy(requestContext);

    assertEquals("first-salt", result.getSalt());
    assertTrue(result.hasSha256());
  }
}
