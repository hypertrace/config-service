package ai.traceable.data.obfuscation.config.service.rules;

import ai.traceable.data.obfuscation.config.service.v1.CreateDataObfuscationStrategyRequest;
import ai.traceable.data.obfuscation.config.service.v1.ObfuscationStrategy;
import ai.traceable.data.obfuscation.config.service.v1.UpdateDataObfuscationStrategyRequest;
import io.grpc.StatusRuntimeException;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface ConfigManager {
  List<ObfuscationStrategy> getObfuscationStrategies(RequestContext requestContext);

  ObfuscationStrategy createObfuscationStrategy(
      RequestContext requestContext, CreateDataObfuscationStrategyRequest request);

  ObfuscationStrategy updateObfuscationStrategy(
      RequestContext requestContext, UpdateDataObfuscationStrategyRequest request);

  void deleteObfuscationStrategy(RequestContext requestContext) throws StatusRuntimeException;
}
