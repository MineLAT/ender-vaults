package com.github.dig.endervaults.bukkit.vault;

import com.github.dig.endervaults.api.VaultPluginProvider;
import com.github.dig.endervaults.api.storage.DataStorage;
import com.github.dig.endervaults.api.vault.VaultPersister;
import com.github.dig.endervaults.api.vault.VaultRegistry;
import lombok.extern.java.Log;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;

@Log
public class BukkitVaultPersister implements VaultPersister {

    private final DataStorage dataStorage = VaultPluginProvider.getPlugin().getDataStorage();
    private final VaultRegistry registry = VaultPluginProvider.getPlugin().getRegistry();
    private final List<UUID> persisted = new ArrayList<>();

    @Override
    public void load(UUID ownerUUID) {
        remove(ownerUUID);
        registry.clean(ownerUUID);
        dataStorage.load(ownerUUID, vaults -> {
            vaults.forEach(vault -> registry.register(ownerUUID, vault));
            finish(ownerUUID);
        });
    }

    @Override
    public void save(UUID ownerUUID) {
        remove(ownerUUID);
        try {
            dataStorage.save(ownerUUID, registry.get(ownerUUID).values());
        } catch (IOException e) {
            log.log(Level.SEVERE, "[EnderVaults] Unable to save vaults for player " + ownerUUID + ".", e);
        }

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
    public boolean isLoaded(UUID ownerUUID) {
        return persisted.contains(ownerUUID);
    }

    public List<UUID> getPersisted() {
        return persisted;
    }

    public synchronized void finish(UUID ownerUUID) {
        persisted.add(ownerUUID);
    }

    public synchronized void remove(UUID ownerUUID) {
        persisted.remove(ownerUUID);
    }
}
