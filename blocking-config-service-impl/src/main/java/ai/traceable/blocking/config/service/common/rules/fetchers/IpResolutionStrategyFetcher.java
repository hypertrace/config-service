package ai.traceable.blocking.config.service.common.rules.fetchers;

import ai.traceable.blocking.config.service.v2.IpResolutionStrategy;
import ai.traceable.ipresolutionstrategy.config.service.v1.GetIpResolutionStrategyConfigsRequest;
import ai.traceable.ipresolutionstrategy.config.service.v1.GetIpResolutionStrategyConfigsResponse;
import ai.traceable.ipresolutionstrategy.config.service.v1.IpResolutionStrategyConfig;
import ai.traceable.ipresolutionstrategy.config.service.v1.IpResolutionStrategyConfigData;
import ai.traceable.ipresolutionstrategy.config.service.v1.IpResolutionStrategyConfigServiceGrpc.IpResolutionStrategyConfigServiceBlockingStub;
import ai.traceable.ipresolutionstrategy.config.service.v1.IpResolutionStrategyFilter;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class IpResolutionStrategyFetcher implements RulesFetcher {
  private final IpResolutionStrategyConfigServiceBlockingStub stub;
  private final ClientConfig clientConfig;

  @Inject
  public IpResolutionStrategyFetcher(
      IpResolutionStrategyConfigServiceBlockingStub stub, ClientConfig clientConfig) {
    this.stub = stub;
    this.clientConfig = clientConfig;
  }

  public Map<String, IpResolutionStrategy> fetchStrategies(
      RequestContext context, Optional<String> environmentId, Set<String> serviceNames) {
    if (serviceNames == null || serviceNames.isEmpty()) {
      return Map.of();
    }

    IpResolutionStrategyFilter.Builder filter = IpResolutionStrategyFilter.newBuilder();
    filter.setDisabled(false);
    environmentId.ifPresent(filter::addEnvironmentNames);
    // Set service_names to fetch only service-scoped strategies (no env-wide fallback)
    filter.addAllServiceNames(serviceNames);

    GetIpResolutionStrategyConfigsRequest request =
        GetIpResolutionStrategyConfigsRequest.newBuilder().setFilter(filter.build()).build();

    GetIpResolutionStrategyConfigsResponse response =
        context.call(
            () ->
                stub.withDeadlineAfter(clientConfig.getTimeout().toMillis(), TimeUnit.MILLISECONDS)
                    .getIpResolutionStrategyConfigs(request));

    // Build selection per service
    Map<String, IpResolutionStrategy> result = new HashMap<>();
    for (String serviceName : serviceNames) {
      Optional<IpResolutionStrategy> match =
          selectBestStrategyForService(response, environmentId, serviceName);
      match.ifPresent(strategy -> result.put(serviceName, strategy));
    }
    return result;
  }

  public Map<String, List<IpResolutionStrategy>> fetchStrategyLists(
      RequestContext context, Optional<String> environmentId, Set<String> serviceNames) {
    if (serviceNames == null || serviceNames.isEmpty()) {
      return Map.of();
    }

    IpResolutionStrategyFilter.Builder filter = IpResolutionStrategyFilter.newBuilder();
    filter.setDisabled(false);
    environmentId.ifPresent(filter::addEnvironmentNames);
    filter.addAllServiceNames(serviceNames);

    GetIpResolutionStrategyConfigsRequest request =
        GetIpResolutionStrategyConfigsRequest.newBuilder().setFilter(filter.build()).build();

    GetIpResolutionStrategyConfigsResponse response =
        context.call(
            () ->
                stub.withDeadlineAfter(clientConfig.getTimeout().toMillis(), TimeUnit.MILLISECONDS)
                    .getIpResolutionStrategyConfigs(request));

    Map<String, List<IpResolutionStrategy>> result = new HashMap<>();
    for (String serviceName : serviceNames) {
      List<IpResolutionStrategy> list =
          selectStrategyListForService(response, environmentId, serviceName);
      if (!list.isEmpty()) {
        result.put(serviceName, list);
      }
    }
    return result;
  }

  // Preferred precedence to find best strategy:
  // 1.) Env + Service match exactly
  // 2.) Service match only
  // 3.) Environment match only
  // 4.) Global(if a user configures a strategy without env or service)
  private Optional<IpResolutionStrategy> selectBestStrategyForService(
      GetIpResolutionStrategyConfigsResponse response,
      Optional<String> environmentId,
      String serviceName) {
    IpResolutionStrategyConfigData best = null;
    int bestRank = -1;

    for (IpResolutionStrategyConfig cfg : response.getConfigsList()) {
      IpResolutionStrategyConfigData data = cfg.getData();
      int rank = rankForServiceAndEnv(data, environmentId, serviceName);
      if (rank > bestRank) {
        bestRank = rank;
        best = data;
      }
    }

    if (best == null) {
      return Optional.empty();
    }

    IpResolutionStrategy converted = convert(best.getStrategy());
    if (converted.equals(IpResolutionStrategy.getDefaultInstance())) {
      return Optional.empty();
    }
    return Optional.of(converted);
  }

  private List<IpResolutionStrategy> selectStrategyListForService(
      GetIpResolutionStrategyConfigsResponse response,
      Optional<String> environmentId,
      String serviceName) {
    @SuppressWarnings("unchecked")
    List<IpResolutionStrategyConfigData>[] buckets = new List[4];
    for (int i = 0; i < buckets.length; i++) {
      buckets[i] = new ArrayList<>();
    }

    for (IpResolutionStrategyConfig cfg : response.getConfigsList()) {
      IpResolutionStrategyConfigData data = cfg.getData();
      int bucket = bucketForServiceAndEnv(data, environmentId, serviceName);
      if (bucket >= 0) {
        buckets[bucket].add(data);
      }
    }

    List<IpResolutionStrategy> out = new ArrayList<>();
    for (List<IpResolutionStrategyConfigData> bucket : buckets) {
      for (IpResolutionStrategyConfigData data : bucket) {
        IpResolutionStrategy converted = convert(data.getStrategy());
        if (!converted.equals(IpResolutionStrategy.getDefaultInstance())) {
          out.add(converted);
        }
      }
    }
    return out;
  }

  private static int bucketForServiceAndEnv(
      IpResolutionStrategyConfigData data, Optional<String> environmentId, String serviceName) {
    var serviceList = data.getScope().getServiceScope().getServiceNamesList();
    boolean serviceSpecific = serviceList.contains(serviceName);
    boolean serviceGlobal = serviceList.isEmpty();
    if (!serviceSpecific && !serviceGlobal) {
      return -1;
    }

    if (environmentId.isEmpty()) {
      if (serviceSpecific) {
        return 0;
      }

      var envList = data.getScope().getEnvironmentScope().getEnvironmentNamesList();
      boolean envGlobal = envList.isEmpty();
      return envGlobal ? 3 : 2;
    }

    var envList = data.getScope().getEnvironmentScope().getEnvironmentNamesList();
    String env = environmentId.get();
    boolean envSpecific = envList.contains(env);
    boolean envGlobal = envList.isEmpty();
    if (!envSpecific && !envGlobal) {
      return -1;
    }

    if (serviceSpecific && envSpecific) {
      return 0;
    }
    if (serviceSpecific && envGlobal) {
      return 1;
    }
    if (serviceGlobal && envSpecific) {
      return 2;
    }
    return 3;
  }

  private static int rankForServiceAndEnv(
      IpResolutionStrategyConfigData data, Optional<String> environmentId, String serviceName) {
    var serviceList = data.getScope().getServiceScope().getServiceNamesList();
    boolean serviceSpecific = serviceList.contains(serviceName);
    boolean serviceGlobal = serviceList.isEmpty();
    if (!serviceSpecific && !serviceGlobal) {
      return -1;
    }

    if (environmentId.isEmpty()) {
      return serviceSpecific ? 1 : 0;
    }

    var envList = data.getScope().getEnvironmentScope().getEnvironmentNamesList();
    String env = environmentId.get();
    boolean envSpecific = envList.contains(env);
    boolean envGlobal = envList.isEmpty();
    if (!envSpecific && !envGlobal) {
      return -1;
    }

    int servicePart = serviceSpecific ? 2 : 0;
    int envPart = envSpecific ? 1 : 0;
    return servicePart + envPart;
  }

  private static IpResolutionStrategy convert(
      ai.traceable.ipresolutionstrategy.config.service.v1.IpResolutionStrategy producer) {
    if (producer == null
        || producer.equals(
            ai.traceable.ipresolutionstrategy.config.service.v1.IpResolutionStrategy
                .getDefaultInstance())) {
      return IpResolutionStrategy.getDefaultInstance();
    }

    IpResolutionStrategy.Builder agg = IpResolutionStrategy.newBuilder();
    agg.setEvaluateAllIpsForBlocking(producer.getEvaluateAllIpsForBlocking());
    for (ai.traceable.ipresolutionstrategy.config.service.v1.IpSource src :
        producer.getSourcesList()) {
      agg.addSources(convert(src));
    }
    return agg.build();
  }

  private static ai.traceable.blocking.config.service.v2.IpSource convert(
      ai.traceable.ipresolutionstrategy.config.service.v1.IpSource src) {
    ai.traceable.blocking.config.service.v2.IpSource.Builder agg =
        ai.traceable.blocking.config.service.v2.IpSource.newBuilder();
    agg.setSourceAttributeName(src.getSourceAttributeName());
    if (src.hasParsingStrategy()) {
      agg.setParsingStrategy(convert(src.getParsingStrategy()));
    }
    return agg.build();
  }

  private static ai.traceable.blocking.config.service.v2.ParsingStrategy convert(
      ai.traceable.ipresolutionstrategy.config.service.v1.ParsingStrategy ps) {
    ai.traceable.blocking.config.service.v2.ParsingStrategy.Builder agg =
        ai.traceable.blocking.config.service.v2.ParsingStrategy.newBuilder();
    agg.setSplitDelimiter(ps.getSplitDelimiter());
    agg.setRemoveInternalIps(ps.getRemoveInternalIps());
    agg.setPreferredIndex(ps.getPreferredIndex());

    switch (ps.getExtractionRuleCase()) {
      case KEYWORD_BASED:
        agg.setKeywordBased(
            ai.traceable.blocking.config.service.v2.KeywordBasedExtraction.newBuilder()
                .setPrefix(ps.getKeywordBased().getPrefix())
                .setSuffix(ps.getKeywordBased().getSuffix())
                .build());
        break;
      case RAW_IP:
        agg.setRawIp(ai.traceable.blocking.config.service.v2.RawIpExtraction.newBuilder().build());
        break;
      case REGEX_BASED:
        agg.setRegexBased(
            ai.traceable.blocking.config.service.v2.RegexBasedExtraction.newBuilder()
                .setPattern(ps.getRegexBased().getPattern())
                .setCaptureGroup(ps.getRegexBased().getCaptureGroup())
                .setCaseInsensitive(ps.getRegexBased().getCaseInsensitive())
                .build());
        break;
      case EXTRACTIONRULE_NOT_SET:
      default:
        // leave unset
        break;
    }
    return agg.build();
  }
}
