package ai.traceable.edge.config.service.supplier.api.resolution.url.pattern.trie;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.edge.config.service.TraceableEdgeConfigSupplier;
import ai.traceable.edge.config.service.config.TraceableEdgeConfig;
import ai.traceable.edge.config.service.v1.AgentCapabilities;
import ai.traceable.edge.config.service.v1.ApiIdResolverData;
import ai.traceable.edge.config.service.v1.ApiType;
import ai.traceable.edge.config.service.v1.ConfigPayloads;
import ai.traceable.edge.config.service.v1.ConfigRequestElement;
import ai.traceable.edge.config.service.v1.ConfigResponseElement;
import ai.traceable.edge.config.service.v1.HttpApiIdResolverData;
import ai.traceable.edge.config.service.v1.HttpDetail;
import ai.traceable.edge.config.service.v1.SegmentType;
import ai.traceable.edge.config.service.v1.UrlSegmentTrieNode;
import ai.traceable.entity.fetcher.cache.StreamingApiMappingProvider;
import ai.traceable.entity.fetcher.cache.StreamingApiMappingProvider.HttpApiDetails;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import io.grpc.Status;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = @Inject)
public class ApiIdResolverConfigSupplier implements TraceableEdgeConfigSupplier {
  private static final String CONFIG_TYPE = "ApiIdResolverConfig";
  private static final Pattern REGEX_SEGMENT =
      Pattern.compile(
          ".*(?:"
              + "[*+?]|\\{\\d+(?:,\\d*)?\\}" // quantifiers
              + "|\\^|\\$" // anchors
              + "|\\|" // alternation
              + "|\\[.*?\\]" // character classes
              + "|\\\\[dDsSwWbBAZz]" // common escapes
              + ").*");
  private static final String SERVICE_NAME = "serviceName";
  private static final String API_TYPE = "apiType";
  private static final String FORWARD_SLASH = "/";
  private static final String ENVIRONMENT = "environment";

  private final ObjectMapper objectMapper;
  private final StreamingApiMappingProvider apiMappingProvider;
  private final TraceableEdgeConfig config;
  private final UuidGenerator uuidGenerator;

  @Override
  public String getConfigType() {
    return CONFIG_TYPE;
  }

  @SneakyThrows
  @Override
  public ConfigResponseElement getConfigs(
      RequestContext requestContext,
      String environment,
      ConfigRequestElement requestElement,
      AgentCapabilities agentCapabilities) {

    // NOTE:
    // GRAPHQL, GRPC APIs do not have resolvedUrlPatterns

    Map<String, String> additionalFields = agentCapabilities.getAdditionalFieldsMap();

    if (!additionalFields.containsKey(SERVICE_NAME)) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Missing serviceName in agent capabilities")
          .asRuntimeException();
    }

    String serviceName = additionalFields.get(SERVICE_NAME);
    String apiTypeStr = additionalFields.get(API_TYPE);
    ApiType apiType = apiTypeStr != null ? ApiType.valueOf(apiTypeStr) : ApiType.API_TYPE_HTTP;
    ConfigPayloads configPayloads =
        buildConfigPayloads(requestContext, apiType, serviceName, environment);
    return ConfigResponseElement.newBuilder()
        .setConfigType(getConfigType())
        .setConfigPayloads(configPayloads)
        .addSupportedAgentCapabilities(agentCapabilities)
        .setRefreshAfterDuration(config.getAgentPollingFrequency(getConfigType()))
        .setHash(uuidGenerator.generateId(configPayloads))
        .build();
  }

  private ConfigPayloads buildConfigPayloads(
      RequestContext requestContext, ApiType apiType, String serviceName, String environment) {

    // assert that the most crucial information -serviceName and environment are not null or empty
    assertNonNullOrEmpty(serviceName, SERVICE_NAME);
    assertNonNullOrEmpty(environment, ENVIRONMENT);

    switch (apiType) {
      case API_TYPE_HTTP:
      case API_TYPE_SOAP:
      case API_TYPE_XML_RPC:
        Stream<HttpApiDetails> allHttpApis =
            apiMappingProvider.getAllHttpApiDetails(requestContext, serviceName, environment);
        BiConsumer<UrlSegmentTrieNode.Builder, Map.Entry<String, String>> apiDetailsUpdater =
            (nodeBuilder, entry) -> {
              // Ensure uniqueness by HTTP method: update existing if present, else add new
              var httpDetailsBuilder = nodeBuilder.getApiDetailsBuilder().getHttpDetailsBuilder();
              boolean updated = false;
              for (int i = 0; i < httpDetailsBuilder.getHttpDetailsCount(); i++) {
                if (httpDetailsBuilder.getHttpDetails(i).getHttpMethod().equals(entry.getKey())) {
                  httpDetailsBuilder.setHttpDetails(
                      i,
                      HttpDetail.newBuilder()
                          .setHttpMethod(entry.getKey())
                          .setApiId(entry.getValue())
                          .build());
                  updated = true;
                  break;
                }
              }
              if (!updated) {
                httpDetailsBuilder.addHttpDetails(
                    HttpDetail.newBuilder()
                        .setHttpMethod(entry.getKey())
                        .setApiId(entry.getValue())
                        .build());
              }
            };
        return serializeTrie(buildUrlSegmentTrie(allHttpApis, apiDetailsUpdater));
      case API_TYPE_GRPC:
      case API_TYPE_GRAPHQL:
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Unsupported API type: " + apiType)
            .asRuntimeException();
    }
  }

  private static void assertNonNullOrEmpty(String serviceName, String fieldName) {
    if (serviceName == null || serviceName.isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(String.format("%s is null or empty", fieldName))
          .asRuntimeException();
    }
  }

  @NonNull
  @SneakyThrows
  private ConfigPayloads serializeTrie(UrlSegmentTrieNode root) {
    if (root.toByteString().isEmpty()) {
      return ConfigPayloads.getDefaultInstance();
    }
    return ConfigPayloads.newBuilder()
        .addConfigBytes(
            ApiIdResolverData.newBuilder()
                .setHttpApiIdResolverData(
                    HttpApiIdResolverData.newBuilder().addAllTrie(root.getChildrenList()))
                .build()
                .toByteString())
        .build();
  }

  @NonNull
  private UrlSegmentTrieNode buildUrlSegmentTrie(
      Stream<HttpApiDetails> apiDetails,
      BiConsumer<UrlSegmentTrieNode.Builder, Map.Entry<String, String>> apiDetailsUpdater) {
    UrlSegmentTrieNode.Builder rootBuilder =
        UrlSegmentTrieNode.newBuilder().setKey("").setType(SegmentType.SEGMENT_TYPE_UNSPECIFIED);
    apiDetails.forEach(
        apiDetail ->
            buildTrieForPattern(
                rootBuilder,
                apiDetail.getResolvedUrlPatterns(),
                apiDetail.getApiId(),
                apiDetail.getHttpMethod(),
                apiDetailsUpdater));
    return rootBuilder.build();
  }

  private void buildTrieForPattern(
      UrlSegmentTrieNode.Builder root,
      List<String> resolvedUrlPatterns,
      String apiId,
      String httpMethod,
      BiConsumer<UrlSegmentTrieNode.Builder, Map.Entry<String, String>> apiDetailsUpdater) {
    for (String resolvedUrlPattern : resolvedUrlPatterns) {
      // Remove leading slash and split by '/'
      String cleanPattern =
          resolvedUrlPattern.startsWith(FORWARD_SLASH)
              ? resolvedUrlPattern.substring(1)
              : resolvedUrlPattern;

      // Handle root path case - create a "/" child node
      // this also means that an empty pattern is a root path
      if (cleanPattern.isEmpty()) {
        UrlSegmentTrieNode.Builder childBuilder =
            UrlSegmentTrieNode.newBuilder()
                .setKey(FORWARD_SLASH)
                .setType(SegmentType.SEGMENT_TYPE_LITERAL);
        apiDetailsUpdater.accept(childBuilder, Map.entry(httpMethod, apiId));
        root.addChildren(childBuilder);
        return;
      }

      String[] segments = cleanPattern.split(FORWARD_SLASH);
      UrlSegmentTrieNode.Builder currentNode = root;

      for (String segment : segments) {
        // Determine if the segment is literal or pattern using the proto enum
        SegmentType segTypeEnum =
            isRegexPattern(segment)
                ? SegmentType.SEGMENT_TYPE_PATTERN
                : SegmentType.SEGMENT_TYPE_LITERAL;

        // Find or create child node
        UrlSegmentTrieNode.Builder childNode = findChildNode(currentNode, segment, segTypeEnum);
        if (childNode == null) {
          childNode = currentNode.addChildrenBuilder().setKey(segment).setType(segTypeEnum);
        }

        currentNode = childNode;
      }

      // This is the last segment, upsert method -> apiId mapping
      apiDetailsUpdater.accept(currentNode, Map.entry(httpMethod, apiId));
    }
  }

  private UrlSegmentTrieNode.Builder findChildNode(
      UrlSegmentTrieNode.Builder parent, String key, SegmentType type) {
    for (UrlSegmentTrieNode.Builder childBuilder : parent.getChildrenBuilderList()) {
      if (childBuilder.getKey().equals(key) && childBuilder.getType() == type) {
        return childBuilder;
      }
    }
    return null;
  }

  private boolean isRegexPattern(String segment) {
    return REGEX_SEGMENT.matcher(segment).matches();
  }
}
