package ai.traceable.cloud.bot.deployment.config.service.v1.encryption;

import ai.traceable.cloud.bot.deployment.config.service.v1.EncryptedText;
import com.typesafe.config.Config;
import io.grpc.Status;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Singleton
public class CloudBotEncryptionConfig {
  private static final String ENCRYPTION_CONFIG_PATH =
      "cloudBotDeployment.config.service.encryption";
  private static final String KEY_ID_KEY = "key_id";
  private static final String PUBLIC_KEY_VALUE_KEY = "public_key";
  private static final String PUBLIC_KEY_FORMAT_KEY = "format";
  private static final String ENCRYPTION_ALGORITHM_KEY = "algorithm";

  private final String keyId;
  private final Cipher cipher;

  @Inject
  public CloudBotEncryptionConfig(Config config)
      throws NoSuchPaddingException,
          NoSuchAlgorithmException,
          InvalidKeySpecException,
          InvalidKeyException {
    Config encryptionConfig = config.getConfig(ENCRYPTION_CONFIG_PATH);

    this.keyId = encryptionConfig.getString(KEY_ID_KEY);

    if (!keyId.isEmpty()) {
      String encryptionAlgorithm = encryptionConfig.getString(ENCRYPTION_ALGORITHM_KEY);
      this.cipher = Cipher.getInstance(encryptionAlgorithm);

      String publicKeyAlgorithm = encryptionConfig.getString(PUBLIC_KEY_FORMAT_KEY);
      String publicKeyValue = encryptionConfig.getString(PUBLIC_KEY_VALUE_KEY);

      KeyFactory kf = KeyFactory.getInstance(publicKeyAlgorithm);
      X509EncodedKeySpec keySpecX509 =
          new X509EncodedKeySpec(Base64.getDecoder().decode(publicKeyValue));
      PublicKey publicKey = kf.generatePublic(keySpecX509);
      this.cipher.init(Cipher.ENCRYPT_MODE, publicKey);

      log.info(
          "Initialized CloudBotEncryptionConfig with encryption details: {}", encryptionConfig);
    } else {
      cipher = null;
      log.info("Not initialized CloudBotEncryptionConfig as we got empty key-id");
    }
  }

  public EncryptedText encrypt(String text) {
    if (keyId.isEmpty()) {
      return EncryptedText.newBuilder().setValue(text).build();
    }

    try {
      byte[] encryptedBytes = cipher.doFinal(text.getBytes(StandardCharsets.UTF_8));
      String encryptedValue = Base64.getEncoder().encodeToString(encryptedBytes);

      return EncryptedText.newBuilder().setKeyId(keyId).setValue(encryptedValue).build();
    } catch (Exception e) {
      log.error("Failed to encrypt text", e);
      throw Status.INTERNAL
          .withDescription("Failed to encrypt private-key while creating cloud bot deployment")
          .asRuntimeException();
    }
  }
}
