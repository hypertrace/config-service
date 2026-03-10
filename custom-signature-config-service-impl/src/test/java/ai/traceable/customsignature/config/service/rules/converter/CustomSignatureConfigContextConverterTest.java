package ai.traceable.customsignature.config.service.rules.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.customsignature.config.service.rules.converter.evaluator.CustomSignatureRuleDefinitionConverter;
import ai.traceable.customsignature.config.service.rules.converter.evaluator.EvaluatorConverterModule;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureEvaluationConfigContextRequest;
import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider;
import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider.ApiIdentifierEntity;
import ai.traceable.entity.fetcher.cache.CachedServiceMappingProvider;
import ai.traceable.entity.fetcher.cache.CachedServiceMappingProvider.ServiceIdentifierEntity;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureConditionExpression;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureConfigContext;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureRuleConfig;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureRuleDefinition;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureRuleDefinitionGroup;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureRulesContext;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureSecRule;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class CustomSignatureConfigContextConverterTest {

  private static final String INPUT_DIR = "rules/input";
  private static final String PROTECTION_ENGINE_ONLY_INPUT_DIR =
      "rules/protection-engine-only-input";
  private static final String EXPECTED_OUTPUT_DIR = "rules/expected-output-protection-engine";
  private static final JsonFormat.Parser parser = JsonFormat.parser().ignoringUnknownFields();

  private CustomSignatureConfigContextConverter converter;
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
            "api-id-1", "api-name", "/api-path", List.of("/api/path/.*"), List.of());
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
            new EvaluatorConverterModule(),
            new AbstractModule() {
              @Override
              protected void configure() {
                bind(CachedApiMappingProvider.class).toInstance(apiMappingProvider);
                bind(CachedServiceMappingProvider.class).toInstance(serviceMappingProvider);
                bind(Clock.class).toInstance(Clock.systemUTC());
              }
            });
    Set<CustomSignatureRuleDefinitionConverter> ruleDefinitionConverters =
        injector.getInstance(Key.get(new TypeLiteral<>() {}));
    converter = new CustomSignatureConfigContextConverter(ruleDefinitionConverters);

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

                GetCustomSignatureEvaluationConfigContextRequest request =
                    GetCustomSignatureEvaluationConfigContextRequest.newBuilder().build();

                CustomSignatureConfigContext output =
                    converter.convert(List.of(builder.build()), request);
                CustomSignatureConfigContext.Builder expectedOutputBuilder =
                    CustomSignatureConfigContext.newBuilder();
                parser.merge(expectedOutputFileStr, expectedOutputBuilder);

                assertEquals(
                    copyWithoutEvaluationIdentifier(expectedOutputBuilder.build()),
                    copyWithoutEvaluationIdentifier(output));
              } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
              }
            });
  }

  static List<String> getInputFileNames() {
    String folderName =
        Objects.requireNonNull(
                CustomSignatureConfigContextConverterTest.class
                    .getClassLoader()
                    .getResource(INPUT_DIR))
            .getFile();
    File queriesFolder = new File(folderName);

    return Arrays.stream(Objects.requireNonNull(queriesFolder.listFiles()))
        .map(File::getName)
        .filter(fileName -> !fileName.equals("rule-ip-address-expression-3.json"))
        .filter(fileName -> !fileName.equals("rule-scope-expression-2.json"))
        .collect(Collectors.toUnmodifiableList());
  }

  static List<String> getProtectionEngineOnlyInputFileNames() {
    String folderName =
        Objects.requireNonNull(
                CustomSignatureConfigContextConverterTest.class
                    .getClassLoader()
                    .getResource(PROTECTION_ENGINE_ONLY_INPUT_DIR))
            .getFile();
    File queriesFolder = new File(folderName);

    return Arrays.stream(Objects.requireNonNull(queriesFolder.listFiles()))
        .map(File::getName)
        .collect(Collectors.toUnmodifiableList());
  }

  @ParameterizedTest
  @MethodSource("getProtectionEngineOnlyInputFileNames")
  void testEvaluateRuleProtectionEngineOnly(String fileName) {
    RequestContext testContext = RequestContext.forTenantId("test-tenant");
    Context.current()
        .withValue(RequestContext.CURRENT, testContext)
        .run(
            () -> {
              try {
                String inputFileStr =
                    readResourceFileAsString(PROTECTION_ENGINE_ONLY_INPUT_DIR, fileName);
                String expectedOutputFileStr =
                    readResourceFileAsString(EXPECTED_OUTPUT_DIR, fileName);
                CustomSignatureRule.Builder builder = CustomSignatureRule.newBuilder();
                parser.merge(inputFileStr, builder);

                GetCustomSignatureEvaluationConfigContextRequest request =
                    GetCustomSignatureEvaluationConfigContextRequest.newBuilder().build();

                CustomSignatureConfigContext output =
                    converter.convert(List.of(builder.build()), request);
                CustomSignatureConfigContext.Builder expectedOutputBuilder =
                    CustomSignatureConfigContext.newBuilder();
                parser.merge(expectedOutputFileStr, expectedOutputBuilder);

                assertEquals(
                    copyWithoutEvaluationIdentifier(expectedOutputBuilder.build()),
                    copyWithoutEvaluationIdentifier(output));
              } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
              }
            });
  }

  private CustomSignatureConfigContext copyWithoutEvaluationIdentifier(
      CustomSignatureConfigContext original) {
    CustomSignatureConfigContext.Builder builder = CustomSignatureConfigContext.newBuilder();

    for (CustomSignatureRulesContext ruleContext : original.getRuleContextsList()) {
      CustomSignatureRulesContext.Builder ruleContextBuilder =
          CustomSignatureRulesContext.newBuilder().setScopeContext(ruleContext.getScopeContext());

      for (CustomSignatureRuleConfig ruleConfig : ruleContext.getRuleConfigsList()) {
        CustomSignatureRuleConfig.Builder ruleConfigBuilder =
            CustomSignatureRuleConfig.newBuilder(ruleConfig);

        if (ruleConfig.hasRuleDefinitionGroup()) {
          ruleConfigBuilder.setRuleDefinitionGroup(
              clearEvaluationIdentifiersFromRuleDefinitionGroup(
                  ruleConfig.getRuleDefinitionGroup()));
        }

        ruleContextBuilder.addRuleConfigs(ruleConfigBuilder.build());
      }

      builder.addRuleContexts(ruleContextBuilder.build());
    }

    return builder.build();
  }

  private CustomSignatureRuleDefinitionGroup clearEvaluationIdentifiersFromRuleDefinitionGroup(
      CustomSignatureRuleDefinitionGroup originalGroup) {
    CustomSignatureRuleDefinitionGroup.Builder groupBuilder =
        CustomSignatureRuleDefinitionGroup.newBuilder(originalGroup);
    groupBuilder.clearRuleDefinitions();

    for (CustomSignatureRuleDefinition ruleDef : originalGroup.getRuleDefinitionsList()) {
      groupBuilder.addRuleDefinitions(clearEvaluationIdentifiersFromRuleDefinition(ruleDef));
    }

    return groupBuilder.build();
  }

  private CustomSignatureRuleDefinition clearEvaluationIdentifiersFromRuleDefinition(
      CustomSignatureRuleDefinition originalRuleDef) {
    CustomSignatureRuleDefinition.Builder ruleDefBuilder =
        CustomSignatureRuleDefinition.newBuilder();

    switch (originalRuleDef.getEvaluatorCase()) {
      case CUSTOM_SIGNATURE_CONDITION_EXPRESSION:
        CustomSignatureConditionExpression conditionExpression =
            clearEvaluationIdentifierFromConditionExpression(
                originalRuleDef.getCustomSignatureConditionExpression());
        ruleDefBuilder.setCustomSignatureConditionExpression(conditionExpression);
        break;

      case CUSTOM_SIGNATURE_SEC_RULE:
        CustomSignatureSecRule secRule =
            clearEvaluationIdentifierFromSecRule(originalRuleDef.getCustomSignatureSecRule());
        ruleDefBuilder.setCustomSignatureSecRule(secRule);
        break;

      case CUSTOM_SIGNATURE_RULE_DEFINITION_GROUP:
        CustomSignatureRuleDefinitionGroup nestedGroup =
            clearEvaluationIdentifiersFromRuleDefinitionGroup(
                originalRuleDef.getCustomSignatureRuleDefinitionGroup());
        ruleDefBuilder.setCustomSignatureRuleDefinitionGroup(nestedGroup);
        break;

      case EVALUATOR_NOT_SET:
        // Handle case where no evaluator is set
        break;

      default:
        throw new IllegalArgumentException(
            "Unknown evaluator case: " + originalRuleDef.getEvaluatorCase());
    }

    return ruleDefBuilder.build();
  }

  private CustomSignatureConditionExpression clearEvaluationIdentifierFromConditionExpression(
      CustomSignatureConditionExpression originalCondition) {
    return CustomSignatureConditionExpression.newBuilder(originalCondition)
        .clearConditionExpressionEvaluationIdentifier()
        .build();
  }

  private CustomSignatureSecRule clearEvaluationIdentifierFromSecRule(
      CustomSignatureSecRule originalSecRule) {
    return CustomSignatureSecRule.newBuilder(originalSecRule)
        .clearSecRuleEvaluationIdentifier()
        .build();
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
}
