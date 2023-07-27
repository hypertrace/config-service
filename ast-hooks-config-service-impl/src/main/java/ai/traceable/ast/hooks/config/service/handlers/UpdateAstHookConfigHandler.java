package ai.traceable.ast.hooks.config.service.handlers;

import ai.traceable.ast.hooks.config.service.v1.ApiKey;
import ai.traceable.ast.hooks.config.service.v1.AwsSignatureV4;
import ai.traceable.ast.hooks.config.service.v1.BasicAuthConfig;
import ai.traceable.ast.hooks.config.service.v1.Bearer;
import ai.traceable.ast.hooks.config.service.v1.ContentSignature;
import ai.traceable.ast.hooks.config.service.v1.DynamicBearer;
import ai.traceable.ast.hooks.config.service.v1.DynamicJwt;
import ai.traceable.ast.hooks.config.service.v1.Hmac;
import ai.traceable.ast.hooks.config.service.v1.HookConfig;
import ai.traceable.ast.hooks.config.service.v1.JwtConfig;
import ai.traceable.ast.hooks.config.service.v1.MutualTls;
import ai.traceable.ast.hooks.config.service.v1.MutualTls.MutualTlsAdvanced;
import ai.traceable.ast.hooks.config.service.v1.Oauth2;
import ai.traceable.ast.hooks.config.service.v1.OauthAuthorizationCodeFlow;
import ai.traceable.ast.hooks.config.service.v1.OauthClientCredentialsFlow;
import ai.traceable.ast.hooks.config.service.v1.OauthPasswordFlow;
import ai.traceable.ast.hooks.config.service.v1.OauthPkceFlow;
import ai.traceable.ast.hooks.config.service.v1.PopTokenSignature;
import io.grpc.Status;
import org.apache.commons.lang3.StringUtils;

public class UpdateAstHookConfigHandler {

  public HookConfig applyHookConfigUpdate(HookConfig newHookConfig, HookConfig oldHookConfig) {
    HookConfig.Builder hookConfigBuilder =
        newHookConfig.toBuilder().setAuthEndpoint(newHookConfig.getAuthEndpoint());
    switch (newHookConfig.getHookConfigCase()) {
      case POP_TOKEN_SIGNATURE:
        if (oldHookConfig.hasPopTokenSignature()) {
          hookConfigBuilder.setPopTokenSignature(
              applyPopTokenSignatureUpdate(
                  newHookConfig.getPopTokenSignature(), oldHookConfig.getPopTokenSignature()));
        }
        break;
      case CONTENT_SIGNATURE:
        if (oldHookConfig.hasContentSignature()) {
          hookConfigBuilder.setContentSignature(
              applyContentSignatureUpdate(
                  newHookConfig.getContentSignature(), oldHookConfig.getContentSignature()));
        }
        break;
      case AWS_SIGNATURE_V4:
        if (oldHookConfig.hasAwsSignatureV4()) {
          hookConfigBuilder.setAwsSignatureV4(
              applyAwsSignatureUpdate(
                  newHookConfig.getAwsSignatureV4(), oldHookConfig.getAwsSignatureV4()));
        }
        break;
      case BEARER:
        if (oldHookConfig.hasBearer()) {
          hookConfigBuilder.setBearer(
              applyBearerUpdate(newHookConfig.getBearer(), oldHookConfig.getBearer()));
        }
        break;
      case API_KEY:
        if (oldHookConfig.hasApiKey()) {
          hookConfigBuilder.setApiKey(
              applyApiKeyUpdate(newHookConfig.getApiKey(), oldHookConfig.getApiKey()));
        }
        break;
      case JWT_CONFIG:
        if (oldHookConfig.hasJwtConfig()) {
          hookConfigBuilder.setJwtConfig(
              applyJwtConfigUpdate(newHookConfig.getJwtConfig(), oldHookConfig.getJwtConfig()));
        }
        break;
      case BASIC_AUTH_CONFIG:
        if (oldHookConfig.hasBasicAuthConfig()) {
          hookConfigBuilder.setBasicAuthConfig(
              applyBasicAuthConfigUpdate(
                  newHookConfig.getBasicAuthConfig(), oldHookConfig.getBasicAuthConfig()));
        }
        break;
      case MUTUAL_TLS:
        if (oldHookConfig.hasMutualTls()) {
          hookConfigBuilder.setMutualTls(
              applyMtlsUpdate(newHookConfig.getMutualTls(), oldHookConfig.getMutualTls()));
        }
        break;
      case HMAC:
        if (oldHookConfig.hasHmac()) {
          hookConfigBuilder.setHmac(
              applyHmacUpdate(newHookConfig.getHmac(), oldHookConfig.getHmac()));
        }
        break;
      case OAUTH2:
        if (oldHookConfig.hasOauth2()) {
          hookConfigBuilder.setOauth2(
              applyOauthUpdate(newHookConfig.getOauth2(), oldHookConfig.getOauth2()));
        }
        break;
      case POSTMAN_HOOK_CONFIG:
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("hook config type not supported")
            .asRuntimeException();
    }
    return hookConfigBuilder.build();
  }

  private Hmac applyHmacUpdate(Hmac newHmac, Hmac oldHmac) {
    Hmac.Builder hmacBuilder = newHmac.toBuilder();
    if (StringUtils.isEmpty(newHmac.getAccessKey().getValue())) {
      hmacBuilder.setAccessKey(oldHmac.getAccessKey());
    }
    if (StringUtils.isEmpty(newHmac.getSecretKey().getValue())) {
      hmacBuilder.setSecretKey(oldHmac.getSecretKey());
    }
    return hmacBuilder.build();
  }

  private MutualTls applyMtlsUpdate(MutualTls newMtls, MutualTls oldMtls) {
    MutualTls.Builder mtlsBuilder = newMtls.toBuilder();
    if (StringUtils.isEmpty(newMtls.getClientKey().getValue())) {
      mtlsBuilder.setClientKey(oldMtls.getClientKey());
    }
    MutualTlsAdvanced.Builder mutualTlsAdvancedBuilder = newMtls.getAdvancedOptions().toBuilder();
    if (StringUtils.isEmpty(newMtls.getAdvancedOptions().getClientKeyPassphrase().getValue())) {
      mutualTlsAdvancedBuilder.setClientKeyPassphrase(
          oldMtls.getAdvancedOptions().getClientKeyPassphrase());
    }
    return mtlsBuilder.setAdvancedOptions(mutualTlsAdvancedBuilder).build();
  }

  private JwtConfig applyJwtConfigUpdate(JwtConfig newJwtConfig, JwtConfig oldJwtConfig) {
    JwtConfig.Builder jwtConfigBuilder = newJwtConfig.toBuilder();
    if (newJwtConfig.hasJwtToken()) {
      if (StringUtils.isEmpty(newJwtConfig.getJwtToken().getValue())) {
        if (oldJwtConfig.hasJwtToken()) {
          jwtConfigBuilder.setJwtToken(oldJwtConfig.getJwtToken());
        } else {
          throw Status.INVALID_ARGUMENT.withDescription("jwt token is empty").asRuntimeException();
        }
      }
    } else if (newJwtConfig.hasDynamicJwt()) {
      DynamicJwt.Builder dynamicJwtBuilder = newJwtConfig.getDynamicJwt().toBuilder();
      if (StringUtils.isEmpty(newJwtConfig.getDynamicJwt().getSecretKey().getValue())) {
        if (oldJwtConfig.hasDynamicJwt()) {
          dynamicJwtBuilder.setSecretKey(oldJwtConfig.getDynamicJwt().getSecretKey());
        } else {
          throw Status.INVALID_ARGUMENT
              .withDescription("secret access key is empty")
              .asRuntimeException();
        }
      }
      jwtConfigBuilder.setDynamicJwt(dynamicJwtBuilder);
    }
    return jwtConfigBuilder.build();
  }

  private ApiKey applyApiKeyUpdate(ApiKey newApiKey, ApiKey oldApiKey) {
    ApiKey.Builder apiKeyBuilder = newApiKey.toBuilder();
    if (StringUtils.isEmpty(newApiKey.getApiKeyValue().getValue())) {
      apiKeyBuilder.setApiKeyValue(oldApiKey.getApiKeyValue());
    }
    return apiKeyBuilder.build();
  }

  private ContentSignature applyContentSignatureUpdate(
      ContentSignature newContentSignature, ContentSignature oldContentSignature) {
    ContentSignature.Builder contentSignatureBuilder = newContentSignature.toBuilder();
    if (StringUtils.isEmpty(newContentSignature.getPrivateKey().getValue())) {
      contentSignatureBuilder.setPrivateKey(oldContentSignature.getPrivateKey());
    }
    return contentSignatureBuilder.build();
  }

  private PopTokenSignature applyPopTokenSignatureUpdate(
      PopTokenSignature newPopTokenSignature, PopTokenSignature oldPopTokenSignature) {
    PopTokenSignature.Builder popTokenSignatureBuilder = newPopTokenSignature.toBuilder();
    if (StringUtils.isEmpty(newPopTokenSignature.getPopPrivateKey().getValue())) {
      popTokenSignatureBuilder.setPopPrivateKey(oldPopTokenSignature.getPopPrivateKey());
    }
    return popTokenSignatureBuilder.build();
  }

  private AwsSignatureV4 applyAwsSignatureUpdate(
      AwsSignatureV4 newAwsSignature, AwsSignatureV4 oldAwsSignature) {
    AwsSignatureV4.Builder awsSignatureBuilder = newAwsSignature.toBuilder();
    if (StringUtils.isEmpty(newAwsSignature.getAccessIdKey().getValue())) {
      awsSignatureBuilder.setAccessIdKey(oldAwsSignature.getAccessIdKey());
    }
    if (StringUtils.isEmpty(newAwsSignature.getSecretAccessKey().getValue())) {
      awsSignatureBuilder.setSecretAccessKey(oldAwsSignature.getSecretAccessKey());
    }
    return awsSignatureBuilder.build();
  }

  private Bearer applyBearerUpdate(Bearer newBearer, Bearer oldBearer) {
    Bearer.Builder bearerBuilder = newBearer.toBuilder();
    if (newBearer.hasBearerToken()) {
      if (StringUtils.isEmpty(newBearer.getBearerToken().getValue())) {
        if (oldBearer.hasBearerToken()) {
          bearerBuilder.setBearerToken(oldBearer.getBearerToken());
        } else {
          throw Status.INVALID_ARGUMENT
              .withDescription("bearer token is missing")
              .asRuntimeException();
        }
      }
    } else if (newBearer.hasDynamicBearer()) {
      if (newBearer.getDynamicBearer().hasBasicAuth()) {
        if (oldBearer.hasDynamicBearer() && oldBearer.getDynamicBearer().hasBasicAuth()) {
          bearerBuilder.setDynamicBearer(
              DynamicBearer.newBuilder()
                  .setBasicAuth(
                      applyBasicAuthConfigUpdate(
                          newBearer.getDynamicBearer().getBasicAuth(),
                          oldBearer.getDynamicBearer().getBasicAuth())));
        } else {
          throw Status.INVALID_ARGUMENT
              .withDescription("invalid update on dynamic bearer")
              .asRuntimeException();
        }
      } else if (newBearer.getDynamicBearer().hasOauth2()) {
        if (oldBearer.hasDynamicBearer() && oldBearer.getDynamicBearer().hasOauth2()) {
          bearerBuilder.setDynamicBearer(
              DynamicBearer.newBuilder()
                  .setOauth2(
                      applyOauthUpdate(
                          newBearer.getDynamicBearer().getOauth2(),
                          oldBearer.getDynamicBearer().getOauth2())));
        } else {
          throw Status.INVALID_ARGUMENT
              .withDescription("invalid update on dynamic bearer")
              .asRuntimeException();
        }
      }
    }
    return bearerBuilder.build();
  }

  private Oauth2 applyOauthUpdate(Oauth2 newOauth, Oauth2 oldOauth) {
    Oauth2.Builder oauthBuilder = newOauth.toBuilder();
    if (StringUtils.isEmpty(newOauth.getToken().getValue())) {
      oauthBuilder.setToken(oldOauth.getToken());
    }
    if (StringUtils.isEmpty(newOauth.getClientId().getValue())) {
      oauthBuilder.setClientId(oldOauth.getClientId());
    }

    if (newOauth.hasAuthorizationCodeFlow()) {
      OauthAuthorizationCodeFlow.Builder oauthAuthorizationCodeFlowBuilder =
          newOauth.getAuthorizationCodeFlow().toBuilder();
      if (StringUtils.isEmpty(newOauth.getAuthorizationCodeFlow().getClientSecret().getValue())) {
        if (oldOauth.hasAuthorizationCodeFlow()) {
          oauthAuthorizationCodeFlowBuilder.setClientSecret(
              oldOauth.getAuthorizationCodeFlow().getClientSecret());
        } else {
          throw Status.INVALID_ARGUMENT
              .withDescription("client secret is empty")
              .asRuntimeException();
        }
      }
      oauthBuilder.setAuthorizationCodeFlow(oauthAuthorizationCodeFlowBuilder);
    } else if (newOauth.hasPkceFlow()) {
      OauthPkceFlow.Builder oauthPkceFlowBuilder = newOauth.getPkceFlow().toBuilder();
      if (StringUtils.isEmpty(newOauth.getPkceFlow().getClientSecret().getValue())) {
        if (oldOauth.getPkceFlow().hasClientSecret()) {
          oauthPkceFlowBuilder.setClientSecret(oldOauth.getPkceFlow().getClientSecret());
        } else {
          throw Status.UNAUTHENTICATED
              .withDescription("client secret is empty")
              .asRuntimeException();
        }
      }
      oauthBuilder.setPkceFlow(oauthPkceFlowBuilder);
    } else if (newOauth.hasPasswordFlow()) {
      OauthPasswordFlow.Builder oauthPasswordFlowBuilder = newOauth.getPasswordFlow().toBuilder();
      if (StringUtils.isEmpty(newOauth.getPasswordFlow().getClientSecret().getValue())) {
        if (oldOauth.hasPasswordFlow()) {
          oauthPasswordFlowBuilder.setClientSecret(oldOauth.getPasswordFlow().getClientSecret());
        } else {
          throw Status.INVALID_ARGUMENT
              .withDescription("client secret is empty")
              .asRuntimeException();
        }
      }
      oauthBuilder.setPasswordFlow(oauthPasswordFlowBuilder);
    } else if (newOauth.hasClientCredentialsFlow()) {
      OauthClientCredentialsFlow.Builder oauthClientCredentialsFlowBuilder =
          newOauth.getClientCredentialsFlow().toBuilder();
      if (StringUtils.isEmpty(newOauth.getClientCredentialsFlow().getClientSecret().getValue())) {
        if (oldOauth.hasClientCredentialsFlow()) {
          oauthClientCredentialsFlowBuilder.setClientSecret(
              oldOauth.getClientCredentialsFlow().getClientSecret());
        } else {
          throw Status.INVALID_ARGUMENT
              .withDescription("client secret is empty")
              .asRuntimeException();
        }
      }
      oauthBuilder.setClientCredentialsFlow(oauthClientCredentialsFlowBuilder);
    }
    return oauthBuilder.build();
  }

  private BasicAuthConfig applyBasicAuthConfigUpdate(
      BasicAuthConfig newBasicAuthConfig, BasicAuthConfig oldBasicAuthConfig) {
    BasicAuthConfig.Builder basicAuthConfigBuilder = newBasicAuthConfig.toBuilder();
    if (StringUtils.isEmpty(newBasicAuthConfig.getPassword().getValue())) {
      basicAuthConfigBuilder.setPassword(oldBasicAuthConfig.getPassword());
    }
    return basicAuthConfigBuilder.build();
  }
}
