package ai.traceable.iprange.config.service.utils;

import org.apache.commons.net.util.SubnetUtils;
import org.apache.commons.validator.routines.InetAddressValidator;

public class IpValidationUtils {
  private final InetAddressValidator ipAddressValidator;

  public IpValidationUtils() {
    this.ipAddressValidator = new InetAddressValidator();
  }

  public boolean isValidIp(String ipAddress) {
    return ipAddressValidator.isValid(ipAddress);
  }

  public boolean isValidSubnet(String subnet) {
    // validate IP range CIDR syntax with SubnetUtils
    try {
      new SubnetUtils(subnet);
      return true;
    } catch (IllegalArgumentException iae) {
      return false;
    }
  }
}
