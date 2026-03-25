package ai.traceable.customsignature.config.service.rules.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.customsignature.config.service.rules.converter.expression.CustomSignatureExpressionConverter;
import ai.traceable.customsignature.config.service.rules.converter.expression.ExpressionConverterModule;
import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.ClauseGroup;
import ai.traceable.customsignature.config.service.v1.ClauseOperator;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.EventType;
import ai.traceable.customsignature.config.service.v1.ExpiryDetails;
import ai.traceable.customsignature.config.service.v1.IpAddressExpression;
import ai.traceable.customsignature.config.service.v1.RuleDefinition;
import ai.traceable.customsignature.config.service.v1.RuleEffect;
import ai.traceable.customsignature.config.service.v1.RuleEvaluationPoint;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRule;
import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider;
import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider.ApiIdentifierEntity;
import ai.traceable.entity.fetcher.cache.CachedServiceMappingProvider;
import ai.traceable.entity.fetcher.cache.CachedServiceMappingProvider.ServiceIdentifierEntity;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class CustomSignatureEdgeDecisionConverterTest {

  private static final String INPUT_DIR = "rules/input";
  private static final String EXPECTED_OUTPUT_DIR = "rules/expected-output";
  private static final JsonFormat.Parser parser = JsonFormat.parser().ignoringUnknownFields();

  private CustomSignatureEdgeDecisionConverter converter;
  private ManagedChannel channel;
  private CachedApiMappingProvider apiMappingProvider;
  private CachedServiceMappingProvider serviceMappingProvider;
  private RequestContext previousContext;

  @BeforeEach
  void setup() {
    channel = InProcessChannelBuilder.forName("test").build();
    GrpcChannelRegistry grpcChannelRegistry = mock(GrpcChannelRegistry.class);
    when(grpcChannelRegistry.forPlaintextAddress(anyString(), anyInt())).thenReturn(channel);

    apiMappingProvider = mock(CachedApiMappingProvider.class);
    ApiIdentifierEntity apiEntity =
        new ApiIdentifierEntity(
            "api-id-1", "api-name", "/api-path", List.of("/api/path/.*"), List.of(), null, null);
    when(apiMappingProvider.getApiIdentifierEntities(any(RequestContext.class), anySet()))
        .thenReturn(Map.of("api-id-1", Optional.of(apiEntity)));

    Set<ApiIdentifierEntity> apiEntities = new HashSet<>();
    apiEntities.add(apiEntity);
    when(apiMappingProvider.getApiIdentifierEntitiesHavingLabels(
            any(RequestContext.class), anySet()))
        .thenReturn(Map.of("api-id-1", apiEntities));

    serviceMappingProvider = mock(CachedServiceMappingProvider.class);
    ServiceIdentifierEntity serviceEntity =
        new ServiceIdentifierEntity("service-name", Optional.empty());
    when(serviceMappingProvider.getServiceIdentifierEntities(any(RequestContext.class), anySet()))
        .thenReturn(Map.of("service-id-1", Optional.of(serviceEntity)));

    Injector injector =
        Guice.createInjector(
            new ExpressionConverterModule(),
            new AbstractModule() {
              @Override
              protected void configure() {
                bind(CachedApiMappingProvider.class).toInstance(apiMappingProvider);
                bind(CachedServiceMappingProvider.class).toInstance(serviceMappingProvider);
                bind(Clock.class).toInstance(Clock.systemUTC());
              }
            });
    Set<CustomSignatureExpressionConverter> conditionConverters =
        injector.getInstance(Key.get(new TypeLiteral<>() {}));
    converter = new CustomSignatureEdgeDecisionConverter(conditionConverters);

    previousContext = RequestContext.CURRENT.get();
    RequestContext testContext = RequestContext.forTenantId("test-tenant");
    Context.current().withValue(RequestContext.CURRENT, testContext).run(() -> {});
  }

  @AfterEach
  void tearDown() throws InterruptedException {
    if (previousContext != null) {
      Context.current().withValue(RequestContext.CURRENT, previousContext).run(() -> {});
    }

    if (channel != null) {
      channel.shutdown();
      if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
        channel.shutdownNow();
      }
    }
  }

  @ParameterizedTest
  @MethodSource("getInputFileNames")
  void testEvaluateRule(String fileName) {
    RequestContext testContext = RequestContext.forTenantId("test-tenant");
    Context.current()
        .withValue(RequestContext.CURRENT, testContext)
        .run(
            () -> {
              try {
                String inputFileStr = readResourceFileAsString(INPUT_DIR, fileName);
                String expectedOutputFileStr =
                    readResourceFileAsString(EXPECTED_OUTPUT_DIR, fileName);
                CustomSignatureRule.Builder builder = CustomSignatureRule.newBuilder();
                parser.merge(inputFileStr, builder);
                EdgeDecisionEngineConfig output = converter.convert(List.of(builder.build()));
                EdgeDecisionEngineConfig.Builder expectedOutputBuilder =
                    EdgeDecisionEngineConfig.newBuilder();
                parser.merge(expectedOutputFileStr, expectedOutputBuilder);

                assertEquals(expectedOutputBuilder.build(), output);
              } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
              }
            });
  }

  static List<String> getInputFileNames() {
    String folderName =
        Objects.requireNonNull(
                CustomSignatureEdgeDecisionConverterTest.class
                    .getClassLoader()
                    .getResource(INPUT_DIR))
            .getFile();
    File queriesFolder = new File(folderName);

    return Arrays.stream(Objects.requireNonNull(queriesFolder.listFiles()))
        .map(File::getName)
        .collect(Collectors.toUnmodifiableList());
  }

  private String readResourceFileAsString(String dirName, String fileName) {
    try {
      File file =
          new File(
              Objects.requireNonNull(
                      this.getClass().getClassLoader().getResource(dirName + "/" + fileName))
                  .getFile());
      return FileUtils.readFileToString(file, StandardCharsets.UTF_8);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private CustomSignatureRule buildMinimalEdgeRule(String id) {
    return CustomSignatureRule.newBuilder()
        .setId(id)
        .setName("test-rule-" + id)
        .setEffect(
            RuleEffect.newBuilder()
                .setEventType(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING)
                .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                .build())
        .setDefinition(
            RuleDefinition.newBuilder()
                .setClauseGroup(
                    ClauseGroup.newBuilder()
                        .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                        .addClauses(
                            Clause.newBuilder()
                                .setIpAddressExpression(
                                    IpAddressExpression.newBuilder().addIpAddresses("1.2.3.4")))
                        .build())
                .build())
        .build();
  }

  @Test
  void testBuildRuleStatus_WithExpiry_PopulatesConfigTtl() {
    RequestContext testContext = RequestContext.forTenantId("test-tenant");
    Context.current()
        .withValue(RequestContext.CURRENT, testContext)
        .run(
            () -> {
              long expiryMillis = 1700000000_123L;
              CustomSignatureRule rule =
                  buildMinimalEdgeRule("r1").toBuilder()
                      .setBlockingExpiryDetails(
                          ExpiryDetails.newBuilder().setExpiryTimestampMillis(expiryMillis).build())
                      .build();
              EdgeDecisionEngineConfig config = converter.convert(List.of(rule));

              assertEquals(1, config.getDecisionRulesCount());
              EdgeDecisionRule edgeRule = config.getDecisionRules(0);
              assertTrue(edgeRule.getRuleStatus().hasTtl());
              assertTrue(edgeRule.getRuleStatus().getTtl().hasExpiresAt());
              assertEquals(
                  1700000000L, edgeRule.getRuleStatus().getTtl().getExpiresAt().getSeconds());
              assertEquals(
                  123_000_000, edgeRule.getRuleStatus().getTtl().getExpiresAt().getNanos());
            });
  }

  @Test
  void testBuildRuleStatus_WithoutExpiry_NoConfigTtl() {
    RequestContext testContext = RequestContext.forTenantId("test-tenant");
    Context.current()
        .withValue(RequestContext.CURRENT, testContext)
        .run(
            () -> {
              CustomSignatureRule rule = buildMinimalEdgeRule("r2");
              EdgeDecisionEngineConfig config = converter.convert(List.of(rule));

              assertEquals(1, config.getDecisionRulesCount());
              EdgeDecisionRule edgeRule = config.getDecisionRules(0);
              assertFalse(edgeRule.getRuleStatus().hasTtl());
            });
  }

  @Test
  void testBuildRuleStatus_DefaultExpiryDetails_NoConfigTtl() {
    RequestContext testContext = RequestContext.forTenantId("test-tenant");
    Context.current()
        .withValue(RequestContext.CURRENT, testContext)
        .run(
            () -> {
              CustomSignatureRule rule =
                  buildMinimalEdgeRule("r3").toBuilder()
                      .setBlockingExpiryDetails(ExpiryDetails.getDefaultInstance())
                      .build();
              EdgeDecisionEngineConfig config = converter.convert(List.of(rule));

              assertEquals(1, config.getDecisionRulesCount());
              assertFalse(config.getDecisionRules(0).getRuleStatus().hasTtl());
            });
  }

  @Test
  void testBuildRuleStatus_DurationOnlyNoTimestamp_NoConfigTtl() {
    // In practice, CustomSignatureRulesManager.updateExpiryDetails() derives
    // expiryTimestampMillis from expiryDuration at rule creation time, so
    // the converter should never see duration-only. But if it does, no ConfigTtl
    // is set (we don't duplicate the derivation logic).
    RequestContext testContext = RequestContext.forTenantId("test-tenant");
    Context.current()
        .withValue(RequestContext.CURRENT, testContext)
        .run(
            () -> {
              CustomSignatureRule rule =
                  buildMinimalEdgeRule("r4").toBuilder()
                      .setBlockingExpiryDetails(
                          ExpiryDetails.newBuilder().setExpiryDuration("PT10M").build())
                      .build();
              EdgeDecisionEngineConfig config = converter.convert(List.of(rule));

              assertEquals(1, config.getDecisionRulesCount());
              assertFalse(config.getDecisionRules(0).getRuleStatus().hasTtl());
            });
  }
}
