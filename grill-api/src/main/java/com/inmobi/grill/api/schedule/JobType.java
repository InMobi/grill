package com.inmobi.grill.api.schedule;

public enum JobType {
  QUERY(1), MODEL(2), TEST(3), OTHER(4);

  private final int value;

  private JobType(int value) {
    this.value = value;
  }

  /**
   * Get the integer value of this enum value.
   */
  public int getValue() {
    return value;
  }
}
