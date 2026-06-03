package dev.searchly.common;

public enum Tier {
    FREE(10, 1_000),
    STANDARD(100, 50_000),
    PREMIUM(1_000, 1_000_000),
    ENTERPRISE(10_000, Integer.MAX_VALUE);

    public final int qpsLimit;
    public final int dailyIndexLimit;

    Tier(int qpsLimit, int dailyIndexLimit) {
        this.qpsLimit = qpsLimit;
        this.dailyIndexLimit = dailyIndexLimit;
    }
}
