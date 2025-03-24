package ai.traceable.bot.categorized.config.service.v1;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.datamodel.data.transformation.config.v1.AttributeDerivationMapping;
import ai.traceable.datamodel.data.transformation.config.v1.BinaryOperator;
import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.FieldType;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.datamodel.data.transformation.config.v1.LogicalMatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.LogicalMatchOperator;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.StructuredMatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.VariableDerivationMapping;
import com.google.protobuf.Value;
import java.util.List;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class CategorizedBotConfigServiceTest {

  private static CategorizedBotConfigServiceGrpc.CategorizedBotConfigServiceBlockingStub stub;
  private static MockGenericConfigService mockGenericConfigService;

  @BeforeAll
  static void before() {
    mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    mockGenericConfigService.addService(new CategorizedBotConfigService()).start();
    stub =
        CategorizedBotConfigServiceGrpc.newBlockingStub(mockGenericConfigService.channel())
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @AfterAll
  static void afterAll() {
    mockGenericConfigService.shutdown();
  }

  @Test
  void testGetAllBots() {
    final RequestContext requestContext = RequestContext.forTenantId("t1");
    assertEquals(
        0,
        requestContext
            .call(
                () ->
                    stub.getCategorizedBotConfigs(
                        GetCategorizedBotConfigsRequest.newBuilder()
                            .setBotRequestFilter(
                                CategorizedBotRequestFilter.newBuilder().addBotIds("1").build())
                            .build()))
            .getBotConfigsCount());

    final List<CategorizedBotConfig> botConfigsList =
        requestContext
            .call(
                () ->
                    stub.getCategorizedBotConfigs(
                        GetCategorizedBotConfigsRequest.newBuilder().build()))
            .getBotConfigsList();
    final CategorizedBotConfig expectedConfig =
        CategorizedBotConfig.newBuilder()
            .setId("550e8400-e29b-41d4-a716-446655440000")
            .setCategorizedBotDetails(
                CategorizedBotDetails.newBuilder()
                    .setName("Bing bot")
                    .setDescription(
                        "Bingbot is Microsoft's web crawler responsible for indexing content for Bing Search.")
                    .setCategorizedBotSignatureRule(
                        CategorizedBotSignatureRule.newBuilder()
                            .setMatchCondition(
                                ai.traceable.bot.categorized.config.service.v1.MatchCondition
                                    .newBuilder()
                                    .setLogicalMatchCondition(
                                        ai.traceable.bot.categorized.config.service.v1
                                            .LogicalMatchCondition.newBuilder()
                                            .setOperator(
                                                ai.traceable.bot.categorized.config.service.v1
                                                    .LogicalMatchOperator
                                                    .LOGICAL_MATCH_OPERATOR_AND)
                                            .setRightOperand(
                                                ai.traceable.bot.categorized.config.service.v1
                                                    .MatchCondition.newBuilder()
                                                    .setGenericMatchCondition(
                                                        GenericMatchCondition.newBuilder()
                                                            .setKeyCondition(
                                                                KeyCondition.newBuilder()
                                                                    .setKeyType(
                                                                        KeyType.KEY_TYPE_HEADER)
                                                                    .setKeyMatchOperator(
                                                                        MatchOperatorCondition
                                                                            .newBuilder()
                                                                            .setMatchOperator(
                                                                                MatchOperator
                                                                                    .MATCH_OPERATOR_EQUALS)
                                                                            .setMatchValue(
                                                                                Value.newBuilder()
                                                                                    .setStringValue(
                                                                                        "User-Agent"))))
                                                            .setValueMatchCondition(
                                                                MatchOperatorCondition.newBuilder()
                                                                    .setMatchOperator(
                                                                        MatchOperator
                                                                            .MATCH_OPERATOR_EQUALS)
                                                                    .setMatchValue(
                                                                        Value.newBuilder()
                                                                            .setStringValue(
                                                                                "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)")))
                                                            .build())
                                                    .build())
                                            .setLeftOperand(
                                                ai.traceable.bot.categorized.config.service.v1
                                                    .MatchCondition.newBuilder()
                                                    .setIpMetadataMatchCondition(
                                                        IpMetadataMatchConditon.newBuilder()
                                                            .setIpAddressCondition(
                                                                IpAddressCondition.newBuilder()
                                                                    .addIpv4CidrIpRanges(
                                                                        "13.66.139.0/24")
                                                                    .addIpv4CidrIpRanges(
                                                                        "40.77.167.0/24")
                                                                    .build())
                                                            .build())
                                                    .build())
                                            .build())
                                    .build()))
                    .setBotCategory("Crawlers")
                    .setBotSubCategory("Search bots")
                    .build())
            .build();
    assertEquals(
        expectedConfig,
        botConfigsList.stream()
            .filter(
                categorizedBotConfig ->
                    categorizedBotConfig
                        .getCategorizedBotDetails()
                        .getName()
                        .toLowerCase()
                        .contains("bing"))
            .findFirst()
            .get());
  }

  @Test
  void testGetInvalidIdFilterBots() {
    final RequestContext requestContext = RequestContext.forTenantId("t1");
    assertEquals(
        0,
        requestContext
            .call(
                () ->
                    stub.getCategorizedBotConfigs(
                        GetCategorizedBotConfigsRequest.newBuilder()
                            .setBotRequestFilter(
                                CategorizedBotRequestFilter.newBuilder().addBotIds("1").build())
                            .build()))
            .getBotConfigsCount());
  }

  @Test
  void testGetValidIdFilterBots() {
    final RequestContext requestContext = RequestContext.forTenantId("t1");
    assertEquals(
        1,
        requestContext
            .call(
                () ->
                    stub.getCategorizedBotConfigs(
                        GetCategorizedBotConfigsRequest.newBuilder()
                            .setBotRequestFilter(
                                CategorizedBotRequestFilter.newBuilder()
                                    .addBotIds("550e8400-e29b-41d4-a716-446655440000")
                                    .build())
                            .build()))
            .getBotConfigsCount());
  }

  @Test
  void testGetInvalidCategoryFilterBots() {
    final RequestContext requestContext = RequestContext.forTenantId("t1");
    assertEquals(
        0,
        requestContext
            .call(
                () ->
                    stub.getCategorizedBotConfigs(
                        GetCategorizedBotConfigsRequest.newBuilder()
                            .setBotRequestFilter(
                                CategorizedBotRequestFilter.newBuilder()
                                    .addCategory("DUMMY")
                                    .build())
                            .build()))
            .getBotConfigsCount());
  }

  @Test
  void testGetValidCategoryFilterBots() {
    final RequestContext requestContext = RequestContext.forTenantId("t1");
    assertEquals(
        10,
        requestContext
            .call(
                () ->
                    stub.getCategorizedBotConfigs(
                        GetCategorizedBotConfigsRequest.newBuilder()
                            .setBotRequestFilter(
                                CategorizedBotRequestFilter.newBuilder()
                                    .addCategory("Crawlers")
                                    .build())
                            .build()))
            .getBotConfigsCount());
  }

  @Test
  void testGetInvalidSubCategoryFilterBots() {
    final RequestContext requestContext = RequestContext.forTenantId("t1");
    assertEquals(
        0,
        requestContext
            .call(
                () ->
                    stub.getCategorizedBotConfigs(
                        GetCategorizedBotConfigsRequest.newBuilder()
                            .setBotRequestFilter(
                                CategorizedBotRequestFilter.newBuilder()
                                    .addSubCategory("DUMMY")
                                    .build())
                            .build()))
            .getBotConfigsCount());
  }

  @Test
  void testGetValidSubCategoryFilterBots() {
    final RequestContext requestContext = RequestContext.forTenantId("t1");
    assertEquals(
        3,
        requestContext
            .call(
                () ->
                    stub.getCategorizedBotConfigs(
                        GetCategorizedBotConfigsRequest.newBuilder()
                            .setBotRequestFilter(
                                CategorizedBotRequestFilter.newBuilder()
                                    .addSubCategory("Search bots")
                                    .build())
                            .build()))
            .getBotConfigsCount());
  }

  @Test
  void testGetCategorizedBotConfigEdgeDecisionVariables() {
    final VariableDerivationMapping expectedVariableDerivation =
        VariableDerivationMapping.newBuilder()
            .setName("TRACEABLEAI_BOT_bing_bot_550e8400-e29b-41d4-a716-446655440000")
            .addRules(
                DerivationRule.newBuilder()
                    .setTransformationConfig(
                        DataTransformationConfig.newBuilder()
                            .setStaticValue(Value.newBuilder().setBoolValue(true))
                            .setOutputType(FieldType.FIELD_TYPE_BOOL))
                    .setMatchCondition(
                        MatchCondition.newBuilder()
                            .setLogicalMatchCondition(
                                LogicalMatchCondition.newBuilder()
                                    .setOperator(LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_AND)
                                    .addConditions(
                                        MatchCondition.newBuilder()
                                            .setLogicalMatchCondition(
                                                LogicalMatchCondition.newBuilder()
                                                    .setOperator(
                                                        LogicalMatchOperator
                                                            .LOGICAL_MATCH_OPERATOR_OR)
                                                    .addConditions(
                                                        MatchCondition.newBuilder()
                                                            .setGenericMatchCondition(
                                                                ai.traceable.datamodel.data
                                                                    .transformation.config.v1
                                                                    .GenericMatchCondition
                                                                    .newBuilder()
                                                                    .setJexlExpression(
                                                                        JexlExpressionConfig
                                                                            .newBuilder()
                                                                            .setJexlExpression(
                                                                                "ipValidation:isIpAddressInRange('13.66.139.0/24', $s.getIpAddress())")
                                                                            .build()))
                                                            .build())
                                                    .addConditions(
                                                        MatchCondition.newBuilder()
                                                            .setGenericMatchCondition(
                                                                ai.traceable.datamodel.data
                                                                    .transformation.config.v1
                                                                    .GenericMatchCondition
                                                                    .newBuilder()
                                                                    .setJexlExpression(
                                                                        JexlExpressionConfig
                                                                            .newBuilder()
                                                                            .setJexlExpression(
                                                                                "ipValidation:isIpAddressInRange('40.77.167.0/24', $s.getIpAddress())")
                                                                            .build())
                                                                    .build()))
                                                    .build())
                                            .build())
                                    .addConditions(
                                        MatchCondition.newBuilder()
                                            .setStructuredMatchCondition(
                                                StructuredMatchCondition.newBuilder()
                                                    .setLhs(
                                                        AttributeDerivationMapping.newBuilder()
                                                            .setName("lhs")
                                                            .setType(FieldType.FIELD_TYPE_STR)
                                                            .addRules(
                                                                DerivationRule.newBuilder()
                                                                    .setTransformationConfig(
                                                                        DataTransformationConfig
                                                                            .newBuilder()
                                                                            .setOutputType(
                                                                                FieldType
                                                                                    .FIELD_TYPE_STR)
                                                                            .setJexlExpression(
                                                                                JexlExpressionConfig
                                                                                    .newBuilder()
                                                                                    .setJexlExpression(
                                                                                        "request.headers['user-agent']")))))
                                                    .setBinaryOperator(
                                                        BinaryOperator.newBuilder()
                                                            .setStringValue(
                                                                "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)")
                                                            .setMatchOperator(
                                                                ai.traceable.datamodel.data
                                                                    .transformation.config.v1
                                                                    .MatchOperator
                                                                    .MATCH_OPERATOR_EQ)))))
                            .build())
                    .build())
            .build();
    final RequestContext requestContext = RequestContext.forTenantId("t1");
    assertEquals(
        expectedVariableDerivation,
        requestContext.call(
            () ->
                stub
                    .getCategorizedBotConfigEdgeDecisionVariables(
                        GetCategorizedBotConfigEdgeDecisionVariablesRequest.getDefaultInstance())
                    .getVariableDerivationMappingsList()
                    .stream()
                    .filter(variableDerivation -> variableDerivation.getName().contains("bing_bot"))
                    .findFirst()
                    .get()));
  }
}
