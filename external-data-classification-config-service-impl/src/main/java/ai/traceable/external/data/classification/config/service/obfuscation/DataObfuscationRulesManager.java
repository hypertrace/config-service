package ai.traceable.external.data.classification.config.service.obfuscation;

import ai.traceable.data.obfuscation.config.service.v1.DataObfuscationConfigServiceGrpc.DataObfuscationConfigServiceBlockingStub;
import ai.traceable.data.obfuscation.config.service.v1.GetDataObfuscationStrategyRequest;
import ai.traceable.data.obfuscation.config.service.v1.GetDataObfuscationStrategyResponse;
import ai.traceable.data.obfuscation.config.service.v1.HashStrategy;
import ai.traceable.external.data.classification.config.service.v1.Argon2IdHash;
import ai.traceable.external.data.classification.config.service.v1.ObfuscationStrategy;
import ai.traceable.external.data.classification.config.service.v1.ScryptHash;
import ai.traceable.external.data.classification.config.service.v1.Sha1Hash;
import ai.traceable.external.data.classification.config.service.v1.Sha256Hash;
import com.google.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class DataObfuscationRulesManager {
  private final DataObfuscationConfigServiceBlockingStub dataObfuscationConfigServiceBlockingStub;

  @Inject
  public DataObfuscationRulesManager(
      DataObfuscationConfigServiceBlockingStub dataObfuscationConfigServiceBlockingStub) {
    this.dataObfuscationConfigServiceBlockingStub = dataObfuscationConfigServiceBlockingStub;
  }

  public ObfuscationStrategy getObfuscationStrategy(RequestContext requestContext) {
    GetDataObfuscationStrategyRequest request =
        GetDataObfuscationStrategyRequest.newBuilder().build();
    GetDataObfuscationStrategyResponse response =
        requestContext.call(
            () -> dataObfuscationConfigServiceBlockingStub.getDataObfuscationStrategy(request));

    ObfuscationStrategy.Builder builder = ObfuscationStrategy.newBuilder();
    makeObfuscationStrategyBackwardsCompatible(builder, requestContext);

    if (response.getObfuscationStrategiesCount() == 0) {
      // fallback to existing logic of SHA 256 hash and using tenant id as salt
      builder
          .setSha256(Sha256Hash.newBuilder())
          .setSalt(requestContext.getTenantId().orElseThrow());
      return builder.build();
    }
    ai.traceable.data.obfuscation.config.service.v1.ObfuscationStrategy configuredStrategy =
        response.getObfuscationStrategiesList().get(0);
    builder.setSalt(configuredStrategy.getSalt());

    HashStrategy hashStrategy = configuredStrategy.getHashStrategy();
    switch (hashStrategy.getHashStrategyCase()) {
      case SCRYPT:
        ai.traceable.data.obfuscation.config.service.v1.ScryptHash scryptHash =
            hashStrategy.getScrypt();
        builder.setScrypt(
            ScryptHash.newBuilder()
                .setBlockSize(scryptHash.getBlockSize())
                .setCost(scryptHash.getCost())
                .setKeyLength(scryptHash.getKeyLength())
                .setThreads(scryptHash.getThreads()));
        break;
      case ARGON2ID:
        ai.traceable.data.obfuscation.config.service.v1.Argon2IdHash argon2IdHash =
            hashStrategy.getArgon2Id();
        builder.setArgon2Id(
            Argon2IdHash.newBuilder()
                .setKeyLength(argon2IdHash.getKeyLength())
                .setThreads(argon2IdHash.getThreads())
                .setTime(argon2IdHash.getTime())
                .setMemory(argon2IdHash.getMemory()));
        break;
      case SHA256:
        builder.setSha256(Sha256Hash.newBuilder());
        break;
      case SHA1:
        builder.setSha1(Sha1Hash.newBuilder());
        break;
      default:
        log.warn("Unknown hash strategy: " + hashStrategy.getHashStrategyCase());
        builder.setSha1(Sha1Hash.newBuilder());
    }

    return builder.build();
  }

  // this is so that older TPA's not having obfuscation strategy config will still work
  private void makeObfuscationStrategyBackwardsCompatible(
      ObfuscationStrategy.Builder strategy, RequestContext requestContext) {
    strategy
        .setHashFunction(ObfuscationStrategy.HashFunction.HASH_FUNCTION_SHA256)
        .setSalt(requestContext.getTenantId().orElseThrow());
  }
}
