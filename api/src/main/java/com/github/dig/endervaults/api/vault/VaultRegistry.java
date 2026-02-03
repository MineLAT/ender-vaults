package com.github.dig.endervaults.api.vault;

import org.jetbrains.annotations.NotNull;

import java.util.UUID;

public interface VaultRegistry {

    @NotNull
    VaultHolder getHolder(@NotNull UUID owner);

    void load(@NotNull UUID owner);

    void unload(@NotNull UUID owner);
}
