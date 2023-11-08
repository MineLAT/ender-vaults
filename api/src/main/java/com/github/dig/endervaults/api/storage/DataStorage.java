package com.github.dig.endervaults.api.storage;

import com.github.dig.endervaults.api.vault.Vault;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;

public interface DataStorage {

    boolean init(Storage storage);

    void close();

    boolean exists(UUID ownerUUID, UUID id);

    void load(UUID ownerUUID, Consumer<List<Vault>> consumer);

    Optional<Vault> load(UUID ownerUUID, UUID id);

    void save(Vault vault) throws IOException;

    default void save(UUID ownerUUID, Collection<Vault> vaults) throws IOException {
        for (Vault vault : vaults) {
            save(vault);
        }
    }
}
