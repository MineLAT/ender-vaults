package com.github.dig.endervaults.api.vault.exception;

import lombok.Getter;
import org.jetbrains.annotations.NotNull;

import java.util.UUID;

@Getter
public class VaultOrderException extends RuntimeException {

    private final Integer order;

    public VaultOrderException(Integer order, @NotNull UUID owner, @NotNull UUID id) {
        super("Duplicated vault #" + order + " entry found for owner " + owner + " and vault " + id);
        this.order = order;
    }
}
