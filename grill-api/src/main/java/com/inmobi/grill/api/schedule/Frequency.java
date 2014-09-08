package com.inmobi.grill.api.schedule;

public enum Frequency {
  DAILY(1),
  WEEKLY(2),
  MONTHLY(3),
  QUARTERLY(4),
  YEARLY(5),
  CUSTOM(6);
  
  private final int value;
  
  private Frequency(int value) {
    this.value = value;
  }
  
  /**
   * Get the integer value of this enum value.
   */
  public int getValue() {
    return value;
  }
  
  /**
   * Find a the enum type by its integer value.
   * @return null if the value is not found.
   */
  public static Frequency findByValue(int value) { 
    switch (value) {
      case 1:
        return DAILY;
      case 2:
        return WEEKLY;
      case 3:
        return MONTHLY;
      case 4:
        return QUARTERLY;
      case 5:
        return YEARLY;
      case 6:
        return CUSTOM;
      default:
        return null;
    }
  }
}
