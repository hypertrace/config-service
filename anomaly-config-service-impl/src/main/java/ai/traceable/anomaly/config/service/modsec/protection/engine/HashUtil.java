package ai.traceable.anomaly.config.service.modsec.protection.engine;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import lombok.experimental.UtilityClass;

@UtilityClass
public class HashUtil {
  private static final String SHA256_ALGORITHM_NAME = "SHA-256";

  static String calculateSHA256(String input) {
    try {
      // Create MessageDigest instance for SHA-256
      MessageDigest digest = MessageDigest.getInstance(SHA256_ALGORITHM_NAME);
      // Apply SHA-256 hashing to the input string
      byte[] hashBytes = digest.digest(input.getBytes(StandardCharsets.UTF_8));
      // Convert byte array to hex string
      StringBuilder hexString = new StringBuilder();
      for (byte b : hashBytes) {
        String hex = Integer.toHexString(0xff & b);
        if (hex.length() == 1) {
          hexString.append('0');
        }
        hexString.append(hex);
      }
      return hexString.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("SHA-256 algorithm not found", e);
    }
  }
}
