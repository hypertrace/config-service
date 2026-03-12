package ai.traceable.customsignature.config.service.rules.provider;

import ai.traceable.customsignature.config.service.v1.GetCustomSignatureEvaluationConfigContextRequest;
import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@EqualsAndHashCode
public class CustomSignatureConfigContextKey {
  GetCustomSignatureEvaluationConfigContextRequest request;

  static CustomSignatureConfigContextKey from(
      GetCustomSignatureEvaluationConfigContextRequest request) {
    return new CustomSignatureConfigContextKey(request);
  }
}
