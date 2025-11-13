package com.github.dig.endervaults.api.storage;

import com.github.dig.endervaults.api.vault.Vault;
import com.github.dig.endervaults.api.vault.metadata.VaultDefaultMetadata;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface DataStorage {

    boolean init(Storage storage);

    void close();

    boolean exists(UUID ownerUUID, UUID id);

    List<Vault> load(UUID ownerUUID);

    Optional<Vault> load(UUID ownerUUID, UUID id);

    @NotNull
    <T> Optional<Vault> load(@NotNull UUID ownerUUID, @NotNull VaultDefaultMetadata<T> meta, @NotNull T value);

    void save(Vault vault) throws IOException;

    int delete(UUID ownerUUID);
}
