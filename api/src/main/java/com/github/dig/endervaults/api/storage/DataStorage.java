package com.github.dig.endervaults.api.storage;

import com.github.dig.endervaults.api.vault.Vault;
import com.github.dig.endervaults.api.vault.metadata.VaultDefaultMetadata;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface DataStorage {

    boolean init(Storage storage);

    void close();

    boolean exists(UUID ownerUUID, UUID id) throws Throwable;

    List<Vault> load(UUID ownerUUID) throws Throwable;

    Optional<Vault> load(UUID ownerUUID, UUID id) throws Throwable;

    @NotNull
    <T> Optional<Vault> loadSnapshot(@NotNull UUID ownerUUID, @NotNull VaultDefaultMetadata<T> meta, @NotNull T value, int snapshot) throws Throwable;

    default void loadContents(@NotNull Vault vault) throws Throwable {
        throw new IllegalStateException("The current database type doesn't support vault content loading");
    }

    void save(Vault vault) throws Throwable;

    int delete(UUID ownerUUID) throws Throwable;
}
