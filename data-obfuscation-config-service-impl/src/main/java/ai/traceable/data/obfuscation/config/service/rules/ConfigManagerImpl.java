package ai.traceable.data.obfuscation.config.service.rules;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.data.obfuscation.config.service.v1.CreateDataObfuscationStrategyRequest;
import ai.traceable.data.obfuscation.config.service.v1.ObfuscationStrategy;
import ai.traceable.data.obfuscation.config.service.v1.ObfuscationStrategyInput;
import ai.traceable.data.obfuscation.config.service.v1.UpdateDataObfuscationStrategyRequest;
import com.google.inject.Inject;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class ConfigManagerImpl implements ConfigManager {
  private final DataObfuscationConfigStore dataObfuscationConfigStore;
  private final UuidGenerator uuidGenerator;

  @Inject
  public ConfigManagerImpl(
      DataObfuscationConfigStore dataObfuscationConfigStore, UuidGenerator uuidGenerator) {
    this.dataObfuscationConfigStore = dataObfuscationConfigStore;
    this.uuidGenerator = uuidGenerator;
  }

  @Override
  public List<ObfuscationStrategy> getObfuscationStrategies(RequestContext requestContext) {
    return dataObfuscationConfigStore.getAllConfigData(requestContext);
  }

  @Override
  public ObfuscationStrategy createObfuscationStrategy(
      RequestContext requestContext, CreateDataObfuscationStrategyRequest request) {
    Optional<ObfuscationStrategy> currentStrategy = getCurrentStrategy(requestContext);
    if (currentStrategy.isPresent()) {
      throw Status.ALREADY_EXISTS
          .withDescription("Data Obfuscation Strategy already exists")
          .asRuntimeException(requestContext.buildTrailers());
    }

    String id = uuidGenerator.generateRandomId();
    ObfuscationStrategyInput input = request.getObfuscationStrategy();

    String salt = input.getSalt();
    if (salt.isEmpty()) {
      salt = requestContext.getTenantId().orElseThrow();
    }

    ObfuscationStrategy.Builder strategy =
        ObfuscationStrategy.newBuilder()
            .setId(id)
            .setHashStrategy(input.getHashStrategy())
            .setSalt(salt);

    return dataObfuscationConfigStore.upsertObject(requestContext, strategy.build()).getData();
  }

  @Override
  public ObfuscationStrategy updateObfuscationStrategy(
      RequestContext requestContext, UpdateDataObfuscationStrategyRequest request) {
    Optional<ObfuscationStrategy> currentStrategy = getCurrentStrategy(requestContext);
    if (currentStrategy.isEmpty()) {
      throw Status.NOT_FOUND
          .withDescription("Data Obfuscation Strategy not found")
          .asRuntimeException(requestContext.buildTrailers());
    }

    ObfuscationStrategyInput input = request.getObfuscationStrategy();
    String salt = input.getSalt();
    if (salt.isEmpty()) {
      salt = requestContext.getTenantId().orElseThrow();
    }
    ObfuscationStrategy.Builder strategy =
        ObfuscationStrategy.newBuilder()
            .setId(currentStrategy.get().getId())
            .setHashStrategy(input.getHashStrategy())
            .setSalt(salt);

    return dataObfuscationConfigStore.upsertObject(requestContext, strategy.build()).getData();
  }

  @Override
  public void deleteObfuscationStrategy(RequestContext requestContext)
      throws StatusRuntimeException {
    Optional<ObfuscationStrategy> currentStrategy = getCurrentStrategy(requestContext);
    if (currentStrategy.isEmpty()
        || dataObfuscationConfigStore
            .deleteObject(requestContext, currentStrategy.get().getId())
            .isEmpty()) {
      throw Status.NOT_FOUND.asRuntimeException(requestContext.buildTrailers());
    }
  }

  private Optional<ObfuscationStrategy> getCurrentStrategy(RequestContext requestContext) {
    return dataObfuscationConfigStore.getAllConfigData(requestContext).stream().findAny();
  }
}
