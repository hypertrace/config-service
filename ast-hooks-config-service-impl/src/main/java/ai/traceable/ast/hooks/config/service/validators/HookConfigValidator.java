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

  private static final EncryptedText EMPTY_ENCRYPTED_TEXT = EncryptedText.newBuilder().build();
  private static final String ACCESS_TOKEN_URL_NOT_FOUND = "access token url not found for oauth";

  public void validate(HookConfig hookConfig, boolean isUpdateRequest) {
    switch (hookConfig.getHookConfigCase()) {
      case HMAC:
        validateHmac(hookConfig.getHmac(), isUpdateRequest);
        break;
      case BEARER:
        validateBearer(hookConfig.getBearer(), isUpdateRequest);
        break;
      case OAUTH2:
        validateOauth2(hookConfig.getOauth2(), isUpdateRequest);
        break;
      case BASIC_AUTH_CONFIG:
        validateBasicAuth(hookConfig.getBasicAuthConfig(), isUpdateRequest);
        break;
      case MUTUAL_TLS:
        validateMutualTls(hookConfig.getMutualTls(), isUpdateRequest);
        break;
      case JWT_CONFIG:
        validateJwt(hookConfig.getJwtConfig(), isUpdateRequest);
        break;
      case POP_TOKEN_SIGNATURE:
        validatePopTokenSignature(hookConfig.getPopTokenSignature(), isUpdateRequest);
        break;
      case AWS_SIGNATURE_V4:
        validateAwsSignature(hookConfig.getAwsSignatureV4(), isUpdateRequest);
        break;
      case CONTENT_SIGNATURE:
        validateContentSignature(hookConfig.getContentSignature(), isUpdateRequest);
        break;
      case API_KEY:
        validateApiKey(hookConfig.getApiKey(), isUpdateRequest);
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

  private void validateContentSignature(
      ContentSignature contentSignature, boolean isUpdateRequest) {
    validateStringNotBlank(contentSignature.getHeaderKey(), "content sig header key not found");
    validateEncryptedText(contentSignature.getPrivateKey(), "private key", isUpdateRequest);
  }

  private void validateApiKey(ApiKey apiKey, boolean isUpdateRequest) {
    validateRequestTokenInfo(apiKey.getTokenInfo());
    validateEncryptedText(apiKey.getApiKeyValue(), "api key", isUpdateRequest);
  }

  private void validateEncryptedText(
      EncryptedText encryptedText, String fieldName, boolean isUpdateRequest) {
    // In case of update empty encrypted text is allowed
    if (encryptedText.equals(EMPTY_ENCRYPTED_TEXT) && isUpdateRequest) {
      return;
    }
    String description = "encrypted value not found for " + fieldName;
    validateStringNotBlank(encryptedText.getKeyId(), description);
    validateStringNotBlank(encryptedText.getValue(), description);
    validateStringNotBlank(encryptedText.getSymmetricKey(), description);
  }

  private void validateAwsSignature(AwsSignatureV4 awsSignatureV4, boolean isUpdateRequest) {
    validateStringNotBlank(awsSignatureV4.getAwsRegion(), "Aws region key not found");
    validateEncryptedText(awsSignatureV4.getAccessIdKey(), "access id key", isUpdateRequest);
    validateEncryptedText(
        awsSignatureV4.getSecretAccessKey(), "secret access key", isUpdateRequest);
    validateStringNotBlank(awsSignatureV4.getServiceName(), "Aws service name not found");
  }

  private void validatePopTokenSignature(
      PopTokenSignature popTokenSignature, boolean isUpdateRequest) {
    validateEncryptedText(popTokenSignature.getPopPrivateKey(), "pop private key", isUpdateRequest);
    validateStringNotBlank(popTokenSignature.getPopHeaderKey(), "pop sig header key not found");
  }

  private void validateJwt(JwtConfig jwtConfig, boolean isUpdateRequest) {
    validateRequestTokenInfo(jwtConfig.getTokenInfo());
    switch (jwtConfig.getJwtInfoCase()) {
      case JWT_TOKEN:
        validateEncryptedText(jwtConfig.getJwtToken(), "jwt token", isUpdateRequest);
        break;
      case DYNAMIC_JWT:
        validateDynamicJwt(jwtConfig.getDynamicJwt(), isUpdateRequest);
        break;
    }
  }

  private void validateDynamicJwt(DynamicJwt dynamicJwt, boolean isUpdateRequest) {
    validateEncryptedText(dynamicJwt.getSecretKey(), "secret key", isUpdateRequest);
    validateKeyGenAlgo(dynamicJwt.getKeyGenAlgo());
  }

  private void validateMutualTls(MutualTls mutualTls, boolean isUpdateRequest) {
    validateStringNotBlank(mutualTls.getClientCert(), "MutualTls client certificate not found");
    validateEncryptedText(mutualTls.getClientKey(), "client key", isUpdateRequest);
  }

  private void validateBasicAuth(BasicAuthConfig basicAuthConfig, boolean isUpdateRequest) {
    validateEncryptedText(basicAuthConfig.getPassword(), "password", isUpdateRequest);
    validateStringNotBlank(basicAuthConfig.getUsername(), "expected username");
  }

  private void validateBearer(Bearer bearer, boolean isUpdateRequest) {
    switch (bearer.getGenerationInfoCase()) {
      case BEARER_TOKEN:
        if (bearer.hasTokenInfo()) {
          validateRequestTokenInfo(bearer.getTokenInfo());
          validateEncryptedText(bearer.getBearerToken(), "bearer token", isUpdateRequest);
        } else {
          throw Status.INVALID_ARGUMENT
              .withDescription("Token info not found for explicit token bearer")
              .asRuntimeException();
        }
        break;
      case DYNAMIC_BEARER:
        validateDynamicBearer(bearer.getDynamicBearer(), isUpdateRequest);
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Generation info not found for Bearer")
            .asRuntimeException();
    }
  }

  private void validateOauth2(Oauth2 oauth2, boolean isUpdateRequest) {
    if (oauth2.hasToken()) {
      validateEncryptedText(oauth2.getToken(), "token", isUpdateRequest);
    }
    validateStringNotBlank(oauth2.getClientIdPlainText(), "client id not found");
    validateRequestTokenInfo(oauth2.getTokenInfo());
    if (oauth2.hasAuthorizationCodeFlow()) {
      validateStringNotBlank(oauth2.getAccessTokenUrl(), ACCESS_TOKEN_URL_NOT_FOUND);
      validateOauthAuthorizationCodeFlow(oauth2.getAuthorizationCodeFlow(), isUpdateRequest);
    } else if (oauth2.hasPkceFlow()) {
      validateStringNotBlank(oauth2.getAccessTokenUrl(), ACCESS_TOKEN_URL_NOT_FOUND);
      validateOauthPkceFlow(oauth2.getPkceFlow(), isUpdateRequest);
    } else if (oauth2.hasImplicitFlow()) {
      validateOauthImplicitFlow(oauth2.getImplicitFlow());
    } else if (oauth2.hasClientCredentialsFlow()) {
      validateStringNotBlank(oauth2.getAccessTokenUrl(), ACCESS_TOKEN_URL_NOT_FOUND);
      validateOauthClientCredentialsFlow(oauth2.getClientCredentialsFlow(), isUpdateRequest);
    } else if (oauth2.hasPasswordFlow()) {
      validateStringNotBlank(oauth2.getAccessTokenUrl(), ACCESS_TOKEN_URL_NOT_FOUND);
      validateOauthPasswordFlow(oauth2.getPasswordFlow(), isUpdateRequest);
    }
  }

  private void validateOauthPasswordFlow(OauthPasswordFlow passwordFlow, boolean isUpdateRequest) {
    validateStringNotBlank(passwordFlow.getUsername(), "username can not be blank");
    validateStringNotBlank(passwordFlow.getPassword(), "password can not be blank");
    validateEncryptedText(passwordFlow.getClientSecret(), "client secret", isUpdateRequest);
  }

  private void validateOauthClientCredentialsFlow(
      OauthClientCredentialsFlow clientCredentialsFlow, boolean isUpdateRequest) {
    validateEncryptedText(
        clientCredentialsFlow.getClientSecret(), "client secret", isUpdateRequest);
  }

  private void validateOauthImplicitFlow(OauthImplicitFlow implicitFlow) {
    validateStringNotBlank(implicitFlow.getState(), "state can not be blank");
  }

  private void validateOauthPkceFlow(OauthPkceFlow pkceFlow, boolean isUpdateRequest) {
    validateStringNotBlank(pkceFlow.getState(), "state can not be blank");
    validateStringNotBlank(pkceFlow.getCodeVerifier(), "code verifier can not be blank");
    validateEncryptedText(pkceFlow.getClientSecret(), "client secret", isUpdateRequest);
  }

  private void validateOauthAuthorizationCodeFlow(
      OauthAuthorizationCodeFlow authorizationCodeFlow, boolean isUpdateRequest) {
    validateStringNotBlank(authorizationCodeFlow.getState(), "state can not be blank");
    validateOauthClientAuthenticationType(authorizationCodeFlow.getClientAuthenticationType());
    validateEncryptedText(
        authorizationCodeFlow.getClientSecret(), "client secret", isUpdateRequest);
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

  private void validateDynamicBearer(DynamicBearer dynamicBearer, boolean isUpdateRequest) {
    switch (dynamicBearer.getBearerAuthTypeCase()) {
      case OAUTH2:
        validateOauth2(dynamicBearer.getOauth2(), isUpdateRequest);
        break;
      case BASIC_AUTH:
        validateBasicAuth(dynamicBearer.getBasicAuth(), isUpdateRequest);
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Auth not specified for dynamic bearer")
            .asRuntimeException();
    }
  }

  private void validateHmac(Hmac hmac, boolean isUpdateRequest) {
    validateStringNotBlank(hmac.getSignatureHeader(), "Hmac signature header not found");
    validateEncryptedText(hmac.getAccessKey(), "access key", isUpdateRequest);
    validateEncryptedText(hmac.getSecretKey(), "secret key", isUpdateRequest);
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
