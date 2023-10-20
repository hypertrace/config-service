package ai.traceable.blocking.config.service.v1;

import ai.traceable.blocking.config.service.common.entity.EntityFetcher;
import ai.traceable.blocking.config.service.common.rules.BlockingRulesSupplier;
import ai.traceable.blocking.config.service.common.rules.BlockingRulesSupplierContext;
import ai.traceable.blocking.config.service.v1.BlockingConfigServiceGrpc.BlockingConfigServiceImplBase;
import ai.traceable.blocking.config.service.v1.blockingmodsec.ModsecBlockingManager;
import ai.traceable.blocking.config.service.v1.blockingpolicy.BlockingPolicyConfigurationManager;
import ai.traceable.blocking.config.service.v1.customsignature.CustomModsecBlockingManager;
import ai.traceable.blocking.config.service.v1.iptype.IpTypeBlockingManager;
import ai.traceable.blocking.config.service.v1.regions.RegionBlockingManager;
import com.google.inject.Inject;
import io.grpc.stub.StreamObserver;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class BlockingConfigServiceImpl extends BlockingConfigServiceImplBase {
  private final RegionBlockingManager regionBlockingManager;
  private final CustomModsecBlockingManager customModsecBlockingManager;
  private final ModsecBlockingManager modsecBlockingManager;
  private final IpTypeBlockingManager ipTypeBlockingManager;
  private final BlockingPolicyConfigurationManager blockingPolicyConfigurationManager;
  private final BlockingRulesSupplierContext blockingRulesSupplierContext;
  private final EntityFetcher entityFetcher;

  @Inject
  public BlockingConfigServiceImpl(
      RegionBlockingManager regionBlockingManager,
      CustomModsecBlockingManager customModsecBlockingManager,
      ModsecBlockingManager modsecBlockingManager,
      IpTypeBlockingManager ipTypeBlockingManager,
      BlockingPolicyConfigurationManager blockingPolicyConfigurationManager,
      BlockingRulesSupplierContext blockingRulesSupplierContext,
      EntityFetcher entityFetcher) {
    this.regionBlockingManager = regionBlockingManager;
    this.customModsecBlockingManager = customModsecBlockingManager;
    this.modsecBlockingManager = modsecBlockingManager;
    this.ipTypeBlockingManager = ipTypeBlockingManager;
    this.blockingPolicyConfigurationManager = blockingPolicyConfigurationManager;
    this.blockingRulesSupplierContext = blockingRulesSupplierContext;
    this.entityFetcher = entityFetcher;
  }

  @Override
  public void getBlockingRules(
      GetBlockingRulesRequest request, StreamObserver<GetBlockingRulesResponse> responseObserver) {

    GetBlockingRulesResponse.Builder responseBuilder = GetBlockingRulesResponse.newBuilder();
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();

      Optional<String> environmentId =
          entityFetcher.getEnvironmentId(requestContext, request.getEnvironment());

      BlockingRulesSupplier blockingRulesSupplier =
          new BlockingRulesSupplier(blockingRulesSupplierContext, requestContext, environmentId);

      if (request.getFilter().getBlockingConfigDataOption()
          != BlockingConfigDataOption.BLOCKING_CONFIG_DATA_OPTION_POLICY_ONLY) {
        responseBuilder.setRegionBlockingRules(
            regionBlockingManager.getEnabledBlockingRules(
                requestContext, request.getRegionBlockingRulesHash(), environmentId));

        responseBuilder.setCustomModsecBlockingRules(
            customModsecBlockingManager.getEnabledBlockingRules(
                requestContext, request.getCustomModsecBlockingRulesHash(), environmentId));

        responseBuilder.setSafeCrsBlockingRules(
            modsecBlockingManager.getEnabledBlockingRules(
                requestContext, request.getSafeCrsBlockingRulesHash(), environmentId));

        responseBuilder.setIpTypeBlockingRules(
            ipTypeBlockingManager.getEnabledBlockingRules(
                requestContext, request.getIpTypeBlockingRulesHash(), environmentId));
      }

      responseBuilder.setBlockingPolicyConfiguration(
          blockingPolicyConfigurationManager.getBlockingPolicyConfiguration(
              requestContext,
              request.getBlockingPolicyConfigurationHash(),
              environmentId,
              blockingRulesSupplier));

      responseObserver.onNext(responseBuilder.build());
      responseObserver.onCompleted();
    } catch (RuntimeException | ExecutionException e) {
      log.error("Get Blocking Rules RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }
}
