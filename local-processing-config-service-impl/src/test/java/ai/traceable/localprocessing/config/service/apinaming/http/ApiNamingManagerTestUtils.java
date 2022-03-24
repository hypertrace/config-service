package ai.traceable.localprocessing.config.service.apinaming.http;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyServiceScope;
import ai.traceable.anomaly.config.service.v1.StringList;
import ai.traceable.anomaly.config.service.v1.trainer.ApiNamingTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.GetAllScopedTrainingConfigsResponse;
import ai.traceable.anomaly.config.service.v1.trainer.ScopedTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.ThresholdRegexConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrieModelTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.UrlFilterConfig;
import ai.traceable.localprocessing.config.service.utils.UuidGenerator;
import ai.traceable.localprocessing.config.service.v1.DiffTrie;
import ai.traceable.localprocessing.config.service.v1.FullTrie;
import ai.traceable.localprocessing.config.service.v1.HttpApiNamingConfig;
import ai.traceable.localprocessing.config.service.v1.HttpApiNamingCustomRule;
import ai.traceable.localprocessing.config.service.v1.Node;
import ai.traceable.localprocessing.config.service.v1.TrieNodePath;
import ai.traceable.localprocessing.config.service.v1.Wildcard;
import ai.traceable.localprocessing.config.service.v1.WildcardConfig;
import ai.traceable.localprocessing.config.service.v1.WildcardType;
import ai.traceable.platform.apientity.Addition;
import ai.traceable.platform.apientity.Deletion;
import ai.traceable.platform.apientity.Segment;
import ai.traceable.platform.apientity.TrieDiffLog;
import ai.traceable.platform.apientity.TrieNodeType;
import ai.traceable.platform.apientity.http.model.TrieNodeConfig;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.hypertrace.span.processing.config.service.v1.ApiNamingRule;
import org.hypertrace.span.processing.config.service.v1.ApiNamingRuleConfig;
import org.hypertrace.span.processing.config.service.v1.ApiNamingRuleDetails;
import org.hypertrace.span.processing.config.service.v1.ApiNamingRuleInfo;
import org.hypertrace.span.processing.config.service.v1.Field;
import org.hypertrace.span.processing.config.service.v1.GetAllApiNamingRulesResponse;
import org.hypertrace.span.processing.config.service.v1.ListValue;
import org.hypertrace.span.processing.config.service.v1.RelationalOperator;
import org.hypertrace.span.processing.config.service.v1.RelationalSpanFilterExpression;
import org.hypertrace.span.processing.config.service.v1.SegmentMatchingBasedConfig;
import org.hypertrace.span.processing.config.service.v1.SpanFilter;
import org.hypertrace.span.processing.config.service.v1.SpanFilterValue;

public class ApiNamingManagerTestUtils {
  private static final String SERVICE_ID1 = "serviceId1";
  private static final String SERVICE_ID2 = "serviceId2";

  public static HttpApiNamingConfig buildApiNamingConfig() {
    UuidGenerator uuidGenerator = new UuidGenerator();
    HttpApiNamingConfig.Builder httpApiNamingConfigBuilder = HttpApiNamingConfig.newBuilder();
    httpApiNamingConfigBuilder
        .addExtensions("extension")
        .addSegmentWhitelistRegexes("allowRegex")
        .addApiNamingCustomRules(
            HttpApiNamingCustomRule.newBuilder()
                .setRegexPattern("regex")
                .setUrlPattern("urlPattern")
                .build())
        .addWildcardConfigs(
            WildcardConfig.newBuilder()
                .setWildcardType(WildcardType.WILDCARD_TYPE_ID)
                .addIdentificationRegexes("regexId")
                .build())
        .addWildcardConfigs(
            WildcardConfig.newBuilder()
                .setWildcardType(WildcardType.WILDCARD_TYPE_LOW_CARDINALITY)
                .addIdentificationRegexes("regexLow")
                .build())
        .addWildcardConfigs(
            WildcardConfig.newBuilder()
                .setWildcardType(WildcardType.WILDCARD_TYPE_HIGH_CARDINALITY)
                .addIdentificationRegexes("regexHigh")
                .build())
        .addWildcardConfigs(
            WildcardConfig.newBuilder()
                .setWildcardType(WildcardType.WILDCARD_TYPE_MEDIUM_CARDINALITY)
                .addIdentificationRegexes("regexMedium")
                .build())
        .addAllFallbackWildcardRegexes(
            List.of(
                "(\\{){0,1}[0-9a-fA-F]{8}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{12}(\\}){0,1}",
                "\\d+"));
    String hash = uuidGenerator.generateId(httpApiNamingConfigBuilder.build());
    return httpApiNamingConfigBuilder.setHash(hash).build();
  }

  public static GetAllScopedTrainingConfigsResponse buildGetAllScopedTrainingConfigsResponse() {
    return GetAllScopedTrainingConfigsResponse.newBuilder()
        .addAllScopedTrainingConfigs(
            List.of(
                buildScopedTrainingConfig(
                    AnomalyConfigScope.newBuilder()
                        .setServiceScope(
                            AnomalyServiceScope.newBuilder().setId(SERVICE_ID1).build())
                        .build()),
                buildScopedTrainingConfig(
                    AnomalyConfigScope.newBuilder()
                        .setServiceScope(
                            AnomalyServiceScope.newBuilder().setId(SERVICE_ID2).build())
                        .build()),
                buildScopedTrainingConfig(
                    AnomalyConfigScope.newBuilder()
                        .setCustomerScope(AnomalyCustomerScope.newBuilder().build())
                        .build())))
        .build();
  }

  private static ScopedTrainingConfig buildScopedTrainingConfig(
      AnomalyConfigScope anomalyConfigScope) {
    return ScopedTrainingConfig.newBuilder()
        .setConfigScope(anomalyConfigScope)
        .addTrainingConfigs(
            TrainingConfig.newBuilder()
                .setApiNamingTrainingConfig(
                    ApiNamingTrainingConfig.newBuilder()
                        .setUrlFilterConfig(
                            UrlFilterConfig.newBuilder()
                                .setUrlRejectRegexPatterns(
                                    StringList.newBuilder().addValues("urlReject").build())
                                .build())))
        .addTrainingConfigs(
            TrainingConfig.newBuilder()
                .setApiNamingTrainingConfig(
                    ApiNamingTrainingConfig.newBuilder()
                        .setTrieModelTrainingConfig(
                            TrieModelTrainingConfig.newBuilder()
                                .setEmbryonicThreshold(123)
                                .setAllowRegexList(
                                    StringList.newBuilder().addValues("allowRegex").build())
                                .setExtensions(
                                    StringList.newBuilder().addValues("extension").build())
                                .setIds(
                                    ThresholdRegexConfig.newBuilder()
                                        .setRegexList(
                                            StringList.newBuilder().addValues("regexId").build())
                                        .setThreshold(1)
                                        .build())
                                .setLowCardinality(
                                    ThresholdRegexConfig.newBuilder()
                                        .setRegexList(
                                            StringList.newBuilder().addValues("regexLow").build())
                                        .setThreshold(1)
                                        .build())
                                .setMediumCardinality(
                                    ThresholdRegexConfig.newBuilder()
                                        .setRegexList(
                                            StringList.newBuilder()
                                                .addValues("regexMedium")
                                                .build())
                                        .setThreshold(1)
                                        .build())
                                .setHighCardinality(
                                    ThresholdRegexConfig.newBuilder()
                                        .setRegexList(
                                            StringList.newBuilder().addValues("regexHigh").build())
                                        .setThreshold(1)
                                        .build())
                                .build())
                        .build())
                .build())
        .build();
  }

  public static GetAllApiNamingRulesResponse buildGetAllApiNamingRuleResponse() {
    return GetAllApiNamingRulesResponse.newBuilder()
        .addRuleDetails(
            ApiNamingRuleDetails.newBuilder()
                .setRule(
                    ApiNamingRule.newBuilder()
                        .setRuleInfo(
                            ApiNamingRuleInfo.newBuilder()
                                .setDisabled(false)
                                .setFilter(
                                    SpanFilter.newBuilder()
                                        .setRelationalSpanFilter(
                                            RelationalSpanFilterExpression.newBuilder()
                                                .setField(Field.FIELD_ENVIRONMENT_NAME)
                                                .setOperator(
                                                    RelationalOperator.RELATIONAL_OPERATOR_IN)
                                                .setRightOperand(
                                                    SpanFilterValue.newBuilder()
                                                        .setListValue(
                                                            ListValue.newBuilder()
                                                                .addValues(
                                                                    SpanFilterValue.newBuilder()
                                                                        .setStringValue(
                                                                            "environment")
                                                                        .build())
                                                                .build())
                                                        .build())
                                                .build())
                                        .build())
                                .setRuleConfig(
                                    ApiNamingRuleConfig.newBuilder()
                                        .setSegmentMatchingBasedConfig(
                                            SegmentMatchingBasedConfig.newBuilder()
                                                .addRegexes("urlPattern")
                                                .addValues("regex")
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build())
        .build();
  }

  public static Set<List<Segment>> buildNonEmbryonicPaths() {
    return new HashSet<>(
        Set.of(
            List.of(
                Segment.newBuilder().setName("3").build(),
                Segment.newBuilder().setName("GET").build(),
                Segment.newBuilder().setName("a").build(),
                Segment.newBuilder().setName("b").build()),
            List.of(
                Segment.newBuilder().setName("3").build(),
                Segment.newBuilder().setName("POST").build(),
                Segment.newBuilder().setName("a").build(),
                Segment.newBuilder()
                    .setName(
                        ai.traceable.platform.apientity.Wildcard.newBuilder()
                            .setWildcardType(TrieNodeType.ID)
                            .setExtension("e")
                            .build())
                    .build())));
  }

  public static TrieNodeConfig buildTrieNodeConfig() {
    return new TrieNodeConfig(
        List.of("allowRegex"),
        List.of("regexId"),
        List.of("regexLow"),
        List.of("regexHigh"),
        new HashSet<>(List.of("extension")),
        123);
  }

  public static FullTrie buildExpectedFullTrie() {
    Node rootNode =
        Node.newBuilder()
            .setValue(
                ai.traceable.localprocessing.config.service.v1.Value.newBuilder()
                    .setName("3")
                    .build())
            .addChildren(
                Node.newBuilder()
                    .setValue(
                        ai.traceable.localprocessing.config.service.v1.Value.newBuilder()
                            .setName("GET")
                            .build())
                    .addChildren(
                        Node.newBuilder()
                            .setValue(
                                ai.traceable.localprocessing.config.service.v1.Value.newBuilder()
                                    .setName("a")
                                    .build())
                            .addChildren(
                                Node.newBuilder()
                                    .setValue(
                                        ai.traceable.localprocessing.config.service.v1.Value
                                            .newBuilder()
                                            .setName("b")
                                            .build())))
                    .build())
            .addChildren(
                Node.newBuilder()
                    .setValue(
                        ai.traceable.localprocessing.config.service.v1.Value.newBuilder()
                            .setName("POST")
                            .build())
                    .addChildren(
                        Node.newBuilder()
                            .setValue(
                                ai.traceable.localprocessing.config.service.v1.Value.newBuilder()
                                    .setName("a")
                                    .build())
                            .addChildren(
                                Node.newBuilder()
                                    .setValue(
                                        ai.traceable.localprocessing.config.service.v1.Value
                                            .newBuilder()
                                            .setWildcard(
                                                Wildcard.newBuilder()
                                                    .setExtension("e")
                                                    .setWildcardType(WildcardType.WILDCARD_TYPE_ID)
                                                    .build())
                                            .build())))
                    .build())
            .build();
    return FullTrie.newBuilder().addAllRoots(List.of(rootNode)).build();
  }

  public static TrieDiffLog buildTrieDiffLog() {
    return TrieDiffLog.newBuilder()
        .setPathAdditions(
            List.of(
                Addition.newBuilder()
                    .setSegments(
                        List.of(
                            Segment.newBuilder().setName("GET").build(),
                            Segment.newBuilder()
                                .setName(
                                    ai.traceable.platform.apientity.Wildcard.newBuilder()
                                        .setWildcardType(TrieNodeType.ID)
                                        .setExtension("e")
                                        .build())
                                .build()))
                    .build()))
        .setNodeDeletions(
            List.of(
                Deletion.newBuilder()
                    .setSegments(List.of(Segment.newBuilder().setName("GET").build()))
                    .build(),
                Deletion.newBuilder()
                    .setSegments(
                        List.of(
                            Segment.newBuilder()
                                .setName(
                                    ai.traceable.platform.apientity.Wildcard.newBuilder()
                                        .setWildcardType(TrieNodeType.ID)
                                        .setExtension("ex")
                                        .build())
                                .build()))
                    .build()))
        .build();
  }

  public static DiffTrie buildExpectedDiffTrie() {
    return DiffTrie.newBuilder()
        .addTrieDiffLogs(
            ai.traceable.localprocessing.config.service.v1.TrieDiffLog.newBuilder()
                .setPathAddition(
                    TrieNodePath.newBuilder()
                        .addValues(
                            ai.traceable.localprocessing.config.service.v1.Value.newBuilder()
                                .setName("GET")
                                .build())
                        .addValues(
                            ai.traceable.localprocessing.config.service.v1.Value.newBuilder()
                                .setWildcard(
                                    Wildcard.newBuilder()
                                        .setWildcardType(WildcardType.WILDCARD_TYPE_ID)
                                        .setExtension("e")
                                        .build())
                                .build())
                        .build())
                .build())
        .addTrieDiffLogs(
            ai.traceable.localprocessing.config.service.v1.TrieDiffLog.newBuilder()
                .setNodeRemoval(
                    TrieNodePath.newBuilder()
                        .addValues(
                            ai.traceable.localprocessing.config.service.v1.Value.newBuilder()
                                .setName("GET")
                                .build())
                        .build())
                .build())
        .addTrieDiffLogs(
            ai.traceable.localprocessing.config.service.v1.TrieDiffLog.newBuilder()
                .setNodeRemoval(
                    TrieNodePath.newBuilder()
                        .addValues(
                            ai.traceable.localprocessing.config.service.v1.Value.newBuilder()
                                .setWildcard(
                                    Wildcard.newBuilder()
                                        .setExtension("ex")
                                        .setWildcardType(WildcardType.WILDCARD_TYPE_ID)
                                        .build())
                                .build())
                        .build())
                .build())
        .build();
  }

  public static Config buildFullTrieReloadConfig(boolean disabled, String tenantScopedTimestamp) {
    return ConfigFactory.parseMap(
        Map.of(
            "default",
            Map.of("timestamp", "1970-01-01T00:00:00.000Z", "disabled", disabled),
            "tenantId",
            Map.of("default", Map.of("timestamp", tenantScopedTimestamp, "disabled", disabled))));
  }

  public static Config buildEntityFetcherConfig() {
    return ConfigFactory.parseMap(Map.of());
  }
}
