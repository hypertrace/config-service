package ai.traceable.blocking.config.service.common.customsignature;

import ai.traceable.customsignature.config.service.v1.CustomModsecRuleVersion;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface CustomSignatureBlobFetcher {
  String getEnabledCustomSignatureRulesBlob(
      RequestContext requestContext,
      CustomModsecRuleVersion customModsecRuleVersion,
      Optional<String> environmentId);
}
