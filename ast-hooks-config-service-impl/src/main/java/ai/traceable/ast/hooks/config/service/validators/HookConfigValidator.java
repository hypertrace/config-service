package ai.traceable.ast.hooks.config.service.validators;

import ai.traceable.ast.hooks.config.service.v1.AwsSignatureV4;
import ai.traceable.ast.hooks.config.service.v1.BasicAuthConfig;
import ai.traceable.ast.hooks.config.service.v1.Bearer;
import ai.traceable.ast.hooks.config.service.v1.ContentSignature;
import ai.traceable.ast.hooks.config.service.v1.DynamicBearer;
import ai.traceable.ast.hooks.config.service.v1.DynamicJwt;
import ai.traceable.ast.hooks.config.service.v1.Hmac;
import ai.traceable.ast.hooks.config.service.v1.HookConfig;
import ai.traceable.ast.hooks.config.service.v1.JwtConfig;
import ai.traceable.ast.hooks.config.service.v1.KeyGenAlgo;
import ai.traceable.ast.hooks.config.service.v1.MutualTls;
import ai.traceable.ast.hooks.config.service.v1.Oauth2;
import ai.traceable.ast.hooks.config.service.v1.PopTokenSignature;
import ai.traceable.ast.hooks.config.service.v1.RequestTokenInfo;
import io.grpc.Status;
import javax.inject.Inject;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class HookConfigValidator extends ValidatorBase {
  public void validate(HookConfig hookConfig) {
    switch (hookConfig.getHookConfigCase()) {
      case HMAC:
        validateHmac(hookConfig.getHmac());
      case BEARER:
        validateBearer(hookConfig.getBearer());
      case OAUTH2:
        validateOauth2(hookConfig.getOauth2());
      case BASIC_AUTH_CONFIG:
        validateBasicAuth(hookConfig.getBasicAuthConfig());
      case MUTUAL_TLS:
        validateMutualTls(hookConfig.getMutualTls());
      case JWT_CONFIG:
        validateJwt(hookConfig.getJwtConfig());
      case POP_TOKEN_SIGNATURE:
        validatePopTokenSignature(hookConfig.getPopTokenSignature());
      case AWS_SIGNATURE_V4:
        validateAwsSignature(hookConfig.getAwsSignatureV4());
      case CONTENT_SIGNATURE:
        validateContentSignature(hookConfig.getContentSignature());
      case POSTMAN_HOOK_CONFIG:
        throw Status.UNIMPLEMENTED
            .withDescription("Postman hook not supported yet")
            .asRuntimeException();
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("unknown config type, found " + hookConfig.getHookConfigCase())
            .asRuntimeException();
    }
  }

  private void validateContentSignature(ContentSignature contentSignature) {
    validateStringNotBlank(contentSignature.getHeaderKey(), "content sig header key not found");
    validateStringNotBlank(contentSignature.getPrivateKey(), "content sig private key not found");
  }

  private void validateAwsSignature(AwsSignatureV4 awsSignatureV4) {
    validateStringNotBlank(awsSignatureV4.getAwsRegion(), "Aws region key not found");
    validateStringNotBlank(awsSignatureV4.getAccessIdKey(), "Aws access id key not found");
    validateStringNotBlank(awsSignatureV4.getSecretAccessKey(), "Aws secret access key not found");
    validateStringNotBlank(awsSignatureV4.getServiceName(), "Aws service name not found");
  }

  private void validatePopTokenSignature(PopTokenSignature popTokenSignature) {
    validateStringNotBlank(popTokenSignature.getPopPrivateKey(), "pop sig private key not found");
    validateStringNotBlank(popTokenSignature.getPopHeaderKey(), "pop sig header key not found");
    validateStringNotBlank(
        popTokenSignature.getPopHeaderPrefix(), "pop sig header prefix not found");
  }

  private void validateJwt(JwtConfig jwtConfig) {
    validateRequestTokenInfo(jwtConfig.getTokenInfo());
    switch (jwtConfig.getJwtInfoCase()) {
      case JWT_TOKEN:
        validateStringNotBlank(jwtConfig.getJwtToken(), "Manual jwt token not found");
      case DYNAMIC_JWT:
        validateDynamicJwt(jwtConfig.getDynamicJwt());
    }
  }

  private void validateDynamicJwt(DynamicJwt dynamicJwt) {
    validateStringNotBlank(dynamicJwt.getSecretKey(), "jwt secret key not found");
    validateKeyGenAlgo(dynamicJwt.getKeyGenAlgo());
  }

  private void validateMutualTls(MutualTls mutualTls) {
    validateStringNotBlank(mutualTls.getClientCert(), "MutualTls client certificate not found");
    validateStringNotBlank(mutualTls.getClientKey(), "MutualTls client key not found");
  }

  private void validateBasicAuth(BasicAuthConfig basicAuthConfig) {
    validateStringNotBlank(basicAuthConfig.getPassword(), "expected password");
    validateStringNotBlank(basicAuthConfig.getUsername(), "expected username");
  }

  private void validateBearer(Bearer bearer) {
    switch (bearer.getGenerationInfoCase()) {
      case BEARER_TOKEN:
        validateStringNotBlank(bearer.getBearerToken(), "Bearer token expected, not found");
      case DYNAMIC_BEARER:
        validateDynamicBearer(bearer.getDynamicBearer());
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Generation info not found for Bearer")
            .asRuntimeException();
    }
  }

  private void validateOauth2(Oauth2 oauth2) {
    validateStringNotBlank(oauth2.getToken(), "Token not found for oauth");
    validateRequestTokenInfo(oauth2.getTokenInfo());
  }

  private void validateRequestTokenInfo(RequestTokenInfo requestTokenInfo) {
    switch (requestTokenInfo.getTokenPlacement()) {
      case TOKEN_PLACEMENT_QUERY:
      case TOKEN_PLACEMENT_COOKIE:
      case TOKEN_PLACEMENT_HEADER:
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(
                "Token placement not found, received " + requestTokenInfo.getTokenPlacement())
            .asRuntimeException();
    }
    validateStringNotBlank(requestTokenInfo.getTokenKey(), "Request token key not found");
  }

  private void validateDynamicBearer(DynamicBearer dynamicBearer) {
    switch (dynamicBearer.getBearerAuthTypeCase()) {
      case OAUTH2:
        validateOauth2(dynamicBearer.getOauth2());
      case BASIC_AUTH:
        validateBasicAuth(dynamicBearer.getBasicAuth());
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Auth not specified for dynamic bearer")
            .asRuntimeException();
    }
  }

  private void validateHmac(Hmac hmac) {
    validateStringNotBlank(hmac.getSignatureHeader(), "Hmac signature header not found");
  }

  private void validateKeyGenAlgo(KeyGenAlgo keyGenAlgo) {
    switch (keyGenAlgo) {
      case KEY_GEN_ALGO_HMAC_SHA1:
      case KEY_GEN_ALGO_HMAC_SHA256:
        break;
      default:
        throw Status.INVALID_ARGUMENT.withDescription("Unknown key gen algo").asRuntimeException();
    }
  }
}
