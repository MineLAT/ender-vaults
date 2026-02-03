package com.github.dig.endervaults.api.vault;

public enum VaultState {

    UNKNOWN,
    LOADING,
    LOADED,
    MODIFIED,
    ERROR;

    public boolean isValid() {
        return this == LOADED || this == MODIFIED;
    }

    public boolean isNotValid() {
        return !isValid();
    }
}
