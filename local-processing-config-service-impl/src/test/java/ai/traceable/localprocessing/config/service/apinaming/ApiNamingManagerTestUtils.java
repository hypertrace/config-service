package ai.traceable.localprocessing.config.service.apinaming;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyServiceScope;
import ai.traceable.anomaly.config.service.v1.StringList;
import ai.traceable.anomaly.config.service.v1.trainer.ApiNamingTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.CustomRuleConfig;
import ai.traceable.anomaly.config.service.v1.trainer.CustomRulesListConfig;
import ai.traceable.anomaly.config.service.v1.trainer.GetAllScopedTrainingConfigsResponse;
import ai.traceable.anomaly.config.service.v1.trainer.ScopedTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.ThresholdRegexConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrieModelTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.UrlFilterConfig;
import ai.traceable.localprocessing.config.service.utils.UuidGenerator;
import ai.traceable.localprocessing.config.service.v1.FullTrie;
import ai.traceable.localprocessing.config.service.v1.HttpApiNamingConfig;
import ai.traceable.localprocessing.config.service.v1.HttpApiNamingCustomRule;
import ai.traceable.localprocessing.config.service.v1.Node;
import ai.traceable.localprocessing.config.service.v1.Wildcard;
import ai.traceable.localprocessing.config.service.v1.WildcardConfig;
import ai.traceable.localprocessing.config.service.v1.WildcardType;
import ai.traceable.platform.apientity.Segment;
import ai.traceable.platform.apientity.TrieNodeType;
import ai.traceable.platform.apientity.http.model.TrieNodeConfig;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.hypertrace.entity.constants.v1.CommonAttribute;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.ByTypeAndIdentifyingAttributes;
import org.hypertrace.entity.data.service.v1.Value;
import org.hypertrace.entity.service.constants.EntityConstants;
import org.hypertrace.entity.v1.entitytype.EntityType;

public class ApiNamingManagerTestUtils {
  private static final String SERVICE_ID1 = "serviceId1";
  private static final String SERVICE_ID2 = "serviceId2";

  public static HttpApiNamingConfig buildApiNamingConfig() {
    UuidGenerator uuidGenerator = new UuidGenerator();
    HttpApiNamingConfig.Builder httpApiNamingConfigBuilder = HttpApiNamingConfig.newBuilder();
    httpApiNamingConfigBuilder
        .addExtensions("extension")
        .addSegmentWhitelistRegexes("allowRegex")
        .addUrlRejectRegexes("urlReject")
        .addApiNamingCustomRules(
            HttpApiNamingCustomRule.newBuilder()
                .setRegexPattern("regex")
                .setUrlPattern("urlPattern")
                .build())
        .addWildcardConfigs(
            WildcardConfig.newBuilder()
                .setWildcardType(WildcardType.WILDCARD_TYPE_ID)
                .setPriority(4)
                .addIdentificationRegexes("regexId")
                .build())
        .addWildcardConfigs(
            WildcardConfig.newBuilder()
                .setWildcardType(WildcardType.WILDCARD_TYPE_LOW_CARDINALITY)
                .setPriority(3)
                .addIdentificationRegexes("regexLow")
                .build())
        .addWildcardConfigs(
            WildcardConfig.newBuilder()
                .setWildcardType(WildcardType.WILDCARD_TYPE_MEDIUM_CARDINALITY)
                .setPriority(1)
                .addIdentificationRegexes("regexMedium")
                .build())
        .addWildcardConfigs(
            WildcardConfig.newBuilder()
                .setWildcardType(WildcardType.WILDCARD_TYPE_HIGH_CARDINALITY)
                .setPriority(2)
                .addIdentificationRegexes("regexHigh")
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

  public static ByTypeAndIdentifyingAttributes buildGetEntityByTypeAndIdentifyingAttributesRequest(
      String serviceName) {
    ByTypeAndIdentifyingAttributes.Builder byTypeAndIdentifyingAttributesBuilder =
        ByTypeAndIdentifyingAttributes.newBuilder()
            .setEntityType(EntityType.SERVICE.name())
            .putIdentifyingAttributes(
                EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_FQN),
                AttributeValue.newBuilder()
                    .setValue(Value.newBuilder().setString(serviceName).build())
                    .build())
            .putIdentifyingAttributes(
                "ENVIRONMENT",
                AttributeValue.newBuilder()
                    .setValue(Value.newBuilder().setString(serviceName).build())
                    .build());
    return byTypeAndIdentifyingAttributesBuilder.build();
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
                        .setCustomRulesListConfig(
                            CustomRulesListConfig.newBuilder()
                                .addCustomRulesConfig(
                                    CustomRuleConfig.newBuilder()
                                        .setUrlPattern("urlPattern")
                                        .setRegex("regex")
                                        .build())
                                .build())
                        .build())
                .build())
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
}
