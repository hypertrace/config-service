package ai.traceable.blocking.config.service.common.iptype;

import ai.traceable.config.utils.refresh.FileRefreshConfig;
import ai.traceable.malicioussources.config.service.v1.IpLocationType;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRule;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class IpTypeRuleAggregatorBase<T> {
  Supplier<List<IpTypeRuleInfo>> ipTypeRules;
  private final BlockingIpTypesClient blockingIpTypesClient;
  private final GenericIpTypeRuleConverter<T> ipTypeConverter;

  @Inject
  public IpTypeRuleAggregatorBase(
      FileRefreshConfig ipTypeBlockingManagerConfig,
      IpTypeRulesLoader ipTypeRulesLoader,
      BlockingIpTypesClient blockingIpTypesClient,
      GenericIpTypeRuleConverter<T> ipTypeConverter) {
    this.ipTypeRules = ipTypeRulesLoader.getLatestDataSupplier(ipTypeBlockingManagerConfig);
    this.blockingIpTypesClient = blockingIpTypesClient;
    this.ipTypeConverter = ipTypeConverter;
  }

  public List<T> getEnabledBlockingRules(
      RequestContext requestContext, Optional<String> environmentId) {
    List<MaliciousSourcesRule> maliciousSourcesRules =
        blockingIpTypesClient.fetchMaliciousSourceRules(requestContext, environmentId);

    List<IpLocationType> blockingIpTypes =
        maliciousSourcesRules.stream()
            .flatMap(
                maliciousSourcesRule ->
                    BlockingIpTypesClient.getBlockingIpTypes(maliciousSourcesRule).stream())
            .distinct()
            .collect(Collectors.toUnmodifiableList());

    return ipTypeRules.get().stream()
        .filter(ipTypeRule -> blockingIpTypes.contains(ipTypeRule.getIpLocationType()))
        .map(ipTypeConverter::convert)
        .collect(Collectors.toUnmodifiableList());
  }

  public interface GenericIpTypeRuleConverter<T> {
    T convert(IpTypeRuleInfo ipTypeRule);
  }
}
