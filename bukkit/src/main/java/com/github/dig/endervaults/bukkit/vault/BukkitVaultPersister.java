package com.github.dig.endervaults.bukkit.vault;

import com.github.dig.endervaults.api.VaultPluginProvider;
import com.github.dig.endervaults.api.storage.DataStorage;
import com.github.dig.endervaults.api.vault.VaultPersister;
import com.github.dig.endervaults.api.vault.VaultRegistry;
import lombok.extern.java.Log;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;

@Log
public class BukkitVaultPersister implements VaultPersister {

    private final DataStorage dataStorage = VaultPluginProvider.getPlugin().getDataStorage();
    private final VaultRegistry registry = VaultPluginProvider.getPlugin().getRegistry();
    private final Map<UUID, State> persisted = new HashMap<>();

    @Override
    public void load(UUID ownerUUID) {
        registry.clean(ownerUUID);
        update(ownerUUID, State.LOADING);
        try {
            dataStorage.load(ownerUUID).forEach(vault -> registry.register(ownerUUID, vault));
        } catch (Throwable t) {
            t.printStackTrace();
            update(ownerUUID, State.ERROR);
            return;
        }
        update(ownerUUID, State.LOADED);
    }

    @Override
    public void save(UUID ownerUUID) {
        registry.get(ownerUUID).values().forEach(vault -> {
            if (vault.isModified()) {
                try {
                    dataStorage.save(vault);
                } catch (IOException e) {
                    log.log(Level.SEVERE,
                            "[EnderVaults] Unable to save vault " + vault.getId() + " for player " + ownerUUID + ".", e);
                }
            }
        });

        remove(ownerUUID);
        registry.clean(ownerUUID);
    }

    private void saveNoUnload(UUID ownerUUID) {
        registry.get(ownerUUID).values().forEach(vault -> {
            try {
                dataStorage.save(vault);
            } catch (IOException e) {
                log.log(Level.SEVERE,
                        "[EnderVaults] Unable to save vault " + vault.getId() + " for player " + ownerUUID + ".", e);
            }
        });
    }

    @Override
    public void save() {
        registry.getAllOwners().forEach(this::saveNoUnload);
    }

    @Override
    public State getState(UUID ownerUUID) {
        return persisted.getOrDefault(ownerUUID, State.UNKNOWN);
    }

    private synchronized void update(UUID ownerUUID, State state) {
        persisted.put(ownerUUID, state);
    }

    private synchronized void remove(UUID ownerUUID) {
        persisted.remove(ownerUUID);
    }
}
