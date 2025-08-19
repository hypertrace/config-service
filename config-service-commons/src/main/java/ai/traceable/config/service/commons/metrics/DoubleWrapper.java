package ai.traceable.config.service.commons.metrics;

/** From <a href="https://github.com/firedome/dynamic-actuator-metrics">...</a> MIT LICENSE */
class DoubleWrapper {
  double value;

  public DoubleWrapper(double value) {
    this.value = value;
  }

  public double getValue() {
    return value;
  }

  public void setValue(double value) {
    this.value = value;
  }
}
