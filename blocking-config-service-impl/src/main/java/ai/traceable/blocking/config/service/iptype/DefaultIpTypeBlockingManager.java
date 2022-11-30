package ai.traceable.blocking.config.service.iptype;

import ai.traceable.blocking.config.service.v1.IpType;
import ai.traceable.blocking.config.service.v1.IpTypeBlockingRules;
import ai.traceable.blocking.config.service.v1.IpTypeRule;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.config.utils.refresh.FileRefreshConfig;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesConfigServiceGrpc.MaliciousSourcesConfigServiceBlockingStub;
import com.google.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class DefaultIpTypeBlockingManager implements IpTypeBlockingManager {

  Supplier<List<IpTypeRule>> ipTypeRules;
  private final MaliciousSourcesConfigServiceBlockingStub maliciousSourcesConfigServiceBlockingStub;
  private final UuidGenerator uuidGenerator;
  private final BlockingIpTypesClient blockingIpTypesClient;

  @Inject
  public DefaultIpTypeBlockingManager(
      FileRefreshConfig ipTypeBlockingManagerConfig,
      IpTypeRulesLoader ipTypeRulesLoader,
      MaliciousSourcesConfigServiceBlockingStub maliciousSourcesConfigServiceBlockingStub,
      UuidGenerator uuidGenerator,
      BlockingIpTypesClient blockingIpTypesClient) {
    this.ipTypeRules = ipTypeRulesLoader.getLatestDataSupplier(ipTypeBlockingManagerConfig);
    this.maliciousSourcesConfigServiceBlockingStub = maliciousSourcesConfigServiceBlockingStub;
    this.uuidGenerator = uuidGenerator;
    this.blockingIpTypesClient = blockingIpTypesClient;
  }

  @Override
  public IpTypeBlockingRules getEnabledBlockingRules(
      RequestContext requestContext, String requestHash, Optional<String> environmentId) {

    List<IpType> blockingIpTypes =
        blockingIpTypesClient.getBlockingIpTypes(
            maliciousSourcesConfigServiceBlockingStub, requestContext, environmentId);

    List<IpTypeRule> ipTypeBlockingRules =
        ipTypeRules.get().stream()
            .filter(ipTypeRule -> blockingIpTypes.contains(ipTypeRule.getIpType()))
            .collect(Collectors.toUnmodifiableList());
    String responseHash = uuidGenerator.generateId(ipTypeBlockingRules);
    IpTypeBlockingRules.Builder ipTypeBlockingRulesBuilder =
        IpTypeBlockingRules.newBuilder().setHash(responseHash);
    if (!responseHash.equals(requestHash)) {
      ipTypeBlockingRulesBuilder.addAllIpTypeRuleList(ipTypeBlockingRules);
    }
    return ipTypeBlockingRulesBuilder.build();
  }
}
