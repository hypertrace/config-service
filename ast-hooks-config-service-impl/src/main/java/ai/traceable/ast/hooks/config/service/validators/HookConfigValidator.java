package ai.traceable.ast.hooks.config.service.validators;

import ai.traceable.ast.hooks.config.service.v1.ApiKey;
import ai.traceable.ast.hooks.config.service.v1.AwsSignatureV4;
import ai.traceable.ast.hooks.config.service.v1.BasicAuthConfig;
import ai.traceable.ast.hooks.config.service.v1.Bearer;
import ai.traceable.ast.hooks.config.service.v1.ContentSignature;
import ai.traceable.ast.hooks.config.service.v1.DynamicBearer;
import ai.traceable.ast.hooks.config.service.v1.DynamicJwt;
import ai.traceable.ast.hooks.config.service.v1.EncryptedText;
import ai.traceable.ast.hooks.config.service.v1.Hmac;
import ai.traceable.ast.hooks.config.service.v1.HmacHash;
import ai.traceable.ast.hooks.config.service.v1.HookConfig;
import ai.traceable.ast.hooks.config.service.v1.JwtConfig;
import ai.traceable.ast.hooks.config.service.v1.KeyGenAlgo;
import ai.traceable.ast.hooks.config.service.v1.MutualTls;
import ai.traceable.ast.hooks.config.service.v1.Oauth2;
import ai.traceable.ast.hooks.config.service.v1.OauthAuthorizationCodeFlow;
import ai.traceable.ast.hooks.config.service.v1.OauthClientAuthenticationType;
import ai.traceable.ast.hooks.config.service.v1.OauthClientCredentialsFlow;
import ai.traceable.ast.hooks.config.service.v1.OauthImplicitFlow;
import ai.traceable.ast.hooks.config.service.v1.OauthPasswordFlow;
import ai.traceable.ast.hooks.config.service.v1.OauthPkceFlow;
import ai.traceable.ast.hooks.config.service.v1.PopTokenSignature;
import ai.traceable.ast.hooks.config.service.v1.RequestTokenInfo;
import io.grpc.Status;
import javax.inject.Inject;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class HookConfigValidator extends ValidatorBase {
  public void validate(HookConfig hookConfig) {
    validateStringNotBlank(hookConfig.getAuthEndpoint(), "auth endpoint not found");
    switch (hookConfig.getHookConfigCase()) {
      case HMAC:
        validateHmac(hookConfig.getHmac());
        break;
      case BEARER:
        validateBearer(hookConfig.getBearer());
        break;
      case OAUTH2:
        validateOauth2(hookConfig.getOauth2());
        break;
      case BASIC_AUTH_CONFIG:
        validateBasicAuth(hookConfig.getBasicAuthConfig());
        break;
      case MUTUAL_TLS:
        validateMutualTls(hookConfig.getMutualTls());
        break;
      case JWT_CONFIG:
        validateJwt(hookConfig.getJwtConfig());
        break;
      case POP_TOKEN_SIGNATURE:
        validatePopTokenSignature(hookConfig.getPopTokenSignature());
        break;
      case AWS_SIGNATURE_V4:
        validateAwsSignature(hookConfig.getAwsSignatureV4());
        break;
      case CONTENT_SIGNATURE:
        validateContentSignature(hookConfig.getContentSignature());
        break;
      case API_KEY:
        validateApiKey(hookConfig.getApiKey());
        break;
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
    validateEncryptedText(contentSignature.getPrivateKey(), "private key");
  }

  private void validateApiKey(ApiKey apiKey) {
    validateRequestTokenInfo(apiKey.getTokenInfo());
    validateEncryptedText(apiKey.getApiKeyValue(), "api key");
  }

  private void validateEncryptedText(EncryptedText encryptedText, String fieldName) {
    String description = "encrypted value not found for " + fieldName;
    validateStringNotBlank(encryptedText.getKeyId(), description);
    validateStringNotBlank(encryptedText.getValue(), description);
  }

  private void validateAwsSignature(AwsSignatureV4 awsSignatureV4) {
    validateStringNotBlank(awsSignatureV4.getAwsRegion(), "Aws region key not found");
    validateEncryptedText(awsSignatureV4.getAccessIdKey(), "access id key");
    validateEncryptedText(awsSignatureV4.getSecretAccessKey(), "secret access key");
    validateStringNotBlank(awsSignatureV4.getServiceName(), "Aws service name not found");
  }

  private void validatePopTokenSignature(PopTokenSignature popTokenSignature) {
    validateEncryptedText(popTokenSignature.getPopPrivateKey(), "pop private key");
    validateStringNotBlank(popTokenSignature.getPopHeaderKey(), "pop sig header key not found");
    validateStringNotBlank(
        popTokenSignature.getPopHeaderPrefix(), "pop sig header prefix not found");
  }

  private void validateJwt(JwtConfig jwtConfig) {
    validateRequestTokenInfo(jwtConfig.getTokenInfo());
    switch (jwtConfig.getJwtInfoCase()) {
      case JWT_TOKEN:
        validateEncryptedText(jwtConfig.getJwtToken(), "jwt token");
        break;
      case DYNAMIC_JWT:
        validateDynamicJwt(jwtConfig.getDynamicJwt());
        break;
    }
  }

  private void validateDynamicJwt(DynamicJwt dynamicJwt) {
    validateEncryptedText(dynamicJwt.getSecretKey(), "secret key");
    validateKeyGenAlgo(dynamicJwt.getKeyGenAlgo());
  }

  private void validateMutualTls(MutualTls mutualTls) {
    validateStringNotBlank(mutualTls.getClientCert(), "MutualTls client certificate not found");
    validateEncryptedText(mutualTls.getClientKey(), "client key");
  }

  private void validateBasicAuth(BasicAuthConfig basicAuthConfig) {
    validateEncryptedText(basicAuthConfig.getPassword(), "password");
    validateStringNotBlank(basicAuthConfig.getUsername(), "expected username");
  }

  private void validateBearer(Bearer bearer) {
    switch (bearer.getGenerationInfoCase()) {
      case BEARER_TOKEN:
        if (bearer.hasTokenInfo()) {
          validateRequestTokenInfo(bearer.getTokenInfo());
          validateEncryptedText(bearer.getBearerToken(), "bearer token");
        } else {
          throw Status.INVALID_ARGUMENT
              .withDescription("Token info not found for explicit token bearer")
              .asRuntimeException();
        }
        break;
      case DYNAMIC_BEARER:
        validateDynamicBearer(bearer.getDynamicBearer());
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Generation info not found for Bearer")
            .asRuntimeException();
    }
  }

  private void validateOauth2(Oauth2 oauth2) {
    if (oauth2.hasToken()) {
      validateEncryptedText(oauth2.getToken(), "token");
    }
    validateEncryptedText(oauth2.getClientId(), "client id");
    validateStringNotBlank(oauth2.getAccessTokenUrl(), "access token url not found for oauth");
    validateRequestTokenInfo(oauth2.getTokenInfo());
    validateStringNotBlank(oauth2.getAccessTokenUrl(), "access token url can not be blank");
    if (oauth2.hasAuthorizationCodeFlow()) {
      validateOauthAuthorizationCodeFlow(oauth2.getAuthorizationCodeFlow());
    } else if (oauth2.hasPkceFlow()) {
      validateOauthPkceFlow(oauth2.getPkceFlow());
    } else if (oauth2.hasImplicitFlow()) {
      validateOauthImplicitFlow(oauth2.getImplicitFlow());
    } else if (oauth2.hasClientCredentialsFlow()) {
      validateOauthClientCredentialsFlow(oauth2.getClientCredentialsFlow());
    } else if (oauth2.hasPasswordFlow()) {
      validateOauthPasswordFlow(oauth2.getPasswordFlow());
    }
  }

  private void validateOauthPasswordFlow(OauthPasswordFlow passwordFlow) {
    validateStringNotBlank(passwordFlow.getUsername(), "username can not be blank");
    validateStringNotBlank(passwordFlow.getPassword(), "password can not be blank");
    validateEncryptedText(passwordFlow.getClientSecret(), "client secret");
  }

  private void validateOauthClientCredentialsFlow(
      OauthClientCredentialsFlow clientCredentialsFlow) {
    validateEncryptedText(clientCredentialsFlow.getClientSecret(), "client secret");
  }

  private void validateOauthImplicitFlow(OauthImplicitFlow implicitFlow) {
    validateStringNotBlank(implicitFlow.getState(), "state can not be blank");
  }

  private void validateOauthPkceFlow(OauthPkceFlow pkceFlow) {
    validateStringNotBlank(pkceFlow.getState(), "state can not be blank");
    validateStringNotBlank(pkceFlow.getCodeVerifier(), "code verifier can not be blank");
    validateEncryptedText(pkceFlow.getClientSecret(), "client secret");
  }

  private void validateOauthAuthorizationCodeFlow(
      OauthAuthorizationCodeFlow authorizationCodeFlow) {
    validateStringNotBlank(authorizationCodeFlow.getState(), "state can not be blank");
    validateOauthClientAuthenticationType(authorizationCodeFlow.getClientAuthenticationType());
    validateEncryptedText(authorizationCodeFlow.getClientSecret(), "client secret");
  }

  private void validateOauthClientAuthenticationType(
      OauthClientAuthenticationType clientAuthenticationType) {
    switch (clientAuthenticationType) {
      case OAUTH_CLIENT_AUTHENTICATION_TYPE_REQUEST_BODY:
      case OAUTH_CLIENT_AUTHENTICATION_TYPE_BASIC_AUTH_HEADER:
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(
                "Oauth client authentication type not found, received " + clientAuthenticationType)
            .asRuntimeException();
    }
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
        break;
      case BASIC_AUTH:
        validateBasicAuth(dynamicBearer.getBasicAuth());
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Auth not specified for dynamic bearer")
            .asRuntimeException();
    }
  }

  private void validateHmac(Hmac hmac) {
    validateStringNotBlank(hmac.getSignatureHeader(), "Hmac signature header not found");
    validateEncryptedText(hmac.getAccessKey(), "access key");
    validateEncryptedText(hmac.getSecretKey(), "secret key");
    validateHmacHash(hmac.getHmacHashAlgo());
  }

  private void validateHmacHash(HmacHash hmacHash) {
    switch (hmacHash) {
      case HMAC_HASH_HMAC_SHA1:
      case HMAC_HASH_HMAC_SHA256:
      case HMAC_HASH_HMAC_SHA512:
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Unknown hmac hash " + hmacHash)
            .asRuntimeException();
    }
  }

  private void validateKeyGenAlgo(KeyGenAlgo keyGenAlgo) {
    switch (keyGenAlgo) {
      case KEY_GEN_ALGO_SHA1:
      case KEY_GEN_ALGO_SHA256:
        break;
      default:
        throw Status.INVALID_ARGUMENT.withDescription("Unknown key gen algo").asRuntimeException();
    }
  }
}
