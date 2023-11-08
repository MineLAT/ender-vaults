package com.github.dig.endervaults.bukkit;

import com.github.dig.endervaults.api.EnderVaultsPlugin;
import com.github.dig.endervaults.api.VaultPluginProvider;
import com.github.dig.endervaults.api.exception.PluginAlreadySetException;
import com.github.dig.endervaults.api.permission.UserPermission;
import com.github.dig.endervaults.api.storage.DataStorage;
import com.github.dig.endervaults.api.storage.Storage;
import com.github.dig.endervaults.api.vault.metadata.VaultDefaultMetadata;
import com.github.dig.endervaults.bukkit.command.VaultAdminCommand;
import com.github.dig.endervaults.bukkit.command.VaultMigrateCommand;
import com.github.dig.endervaults.bukkit.command.VaultReloadCommand;
import com.github.dig.endervaults.bukkit.permission.BukkitUserPermission;
import com.github.dig.endervaults.bukkit.ui.icon.SelectIconListener;
import com.github.dig.endervaults.bukkit.ui.selector.SelectorListener;
import com.github.dig.endervaults.bukkit.storage.YamlStorage;
import com.github.dig.endervaults.bukkit.storage.HikariStorage;
import com.github.dig.endervaults.bukkit.vault.BukkitVaultAutoSave;
import com.github.dig.endervaults.bukkit.vault.BukkitVaultPersister;
import com.github.dig.endervaults.bukkit.vault.metadata.BukkitVaultMetadataRegistry;
import com.github.dig.endervaults.bukkit.vault.metadata.IntegerMetadataConverter;
import com.github.dig.endervaults.bukkit.vault.metadata.StringMetadataConverter;
import com.github.dig.endervaults.bukkit.command.VaultCommand;
import com.github.dig.endervaults.bukkit.file.BukkitDataFile;
import com.github.dig.endervaults.bukkit.lang.BukkitLanguage;
import com.github.dig.endervaults.bukkit.vault.BukkitVaultRegistry;
import lombok.extern.java.Log;
import org.bukkit.Bukkit;
import org.bukkit.ChatColor;
import org.bukkit.configuration.Configuration;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitTask;

import java.io.File;
import java.util.*;
import java.util.logging.Level;

@Log
public class EVBukkitPlugin extends JavaPlugin implements EnderVaultsPlugin {

    private BukkitDataFile langFile;
    private BukkitDataFile configFile;

    private BukkitVaultRegistry registry;
    private BukkitLanguage language;
    private BukkitVaultMetadataRegistry metadataRegistry;
    private DataStorage dataStorage;
    private BukkitVaultPersister persister;
    private BukkitUserPermission permission;
    private Metrics metrics;

    private BukkitTask autoSaveTask;

    @Override
    public BukkitDataFile getLangFile() {
        return langFile;
    }

    @Override
    public BukkitDataFile getConfigFile() {
        return configFile;
    }

    @Override
    public BukkitVaultRegistry getRegistry() {
        return registry;
    }

    @Override
    public BukkitLanguage getLanguage() {
        return language;
    }

    @Override
    public DataStorage getDataStorage() {
        return dataStorage;
    }

    @Override
    public BukkitVaultPersister getPersister() {
        return persister;
    }

    @Override
    public UserPermission getPermission() {
        return permission;
    }

    @Override
    public BukkitVaultMetadataRegistry getMetadataRegistry() {
        return metadataRegistry;
    }

    @Override
    public void onEnable() {
        showStartUpMessage();
        metrics = new Metrics(this, 12345);

        if (!setProviders()) return;
        loadConfiguration();
        if (!setupDataStorage()) return;

        setupManagers();
        setupTasks();

        registerCommands();
        registerMetadataConverters();
        registerListeners();
    }

    @Override
    public void onDisable() {
        if (autoSaveTask != null) {
            autoSaveTask.cancel();
        }

        if (persister != null) {
            persister.save();
        }

        if (dataStorage != null) {
            dataStorage.close();
        }
    }

    private boolean setProviders() {
        try {
            VaultPluginProvider.set(this);
            return true;
        } catch (PluginAlreadySetException e) {
            log.log(Level.SEVERE, "[EnderVaults] Unable to set providers, disabling...", e);
            Bukkit.getPluginManager().disablePlugin(this);
        }
        return false;
    }

    private void loadConfiguration() {
        langFile = new BukkitDataFile(new File(getDataFolder(), "lang.yml"));
        configFile = new BukkitDataFile(new File(getDataFolder(), "config.yml"));
    }

    private boolean setupDataStorage() {
        Configuration configuration = (Configuration) configFile.getConfiguration();
        Storage storage;
        try {
            storage = Storage.valueOf(configuration.getString("storage.method"));
        } catch (IllegalArgumentException e) {
            log.log(Level.SEVERE, "[EnderVaults] Unknown data storage set, disabling...");
            Bukkit.getPluginManager().disablePlugin(this);
            return false;
        }

        switch (storage) {
            case FLATFILE:
                dataStorage = new YamlStorage();
                break;
            case MARIADB:
            case MYSQL:
                dataStorage = new HikariStorage();
                break;
        }

        log.log(Level.INFO, "[EnderVaults] Using data storage: " + storage.toString() + ".");
        if (!dataStorage.init(storage)) {
            log.log(Level.SEVERE, "[EnderVaults] Error with data storage, disabling...");
            Bukkit.getPluginManager().disablePlugin(this);
            return false;
        }

        return true;
    }

    private void setupManagers() {
        registry = new BukkitVaultRegistry();
        language = new BukkitLanguage();
        metadataRegistry = new BukkitVaultMetadataRegistry();
        persister = new BukkitVaultPersister();
        permission = new BukkitUserPermission();
    }

    private void setupTasks() {
        FileConfiguration configuration = (FileConfiguration) configFile.getConfiguration();
        int autoSaveMins = configuration.getInt("auto-save.minutes", 15);
        autoSaveTask = Bukkit.getScheduler().runTaskTimerAsynchronously(this, new BukkitVaultAutoSave(), autoSaveMins * 60 * 20, autoSaveMins * 60 * 20);
    }

    private void registerCommands() {
        getCommand("vault").setExecutor(new VaultCommand());
        getCommand("vaultreload").setExecutor(new VaultReloadCommand());
        getCommand("vaultadmin").setExecutor(new VaultAdminCommand());
        getCommand("vaultmigrate").setExecutor(new VaultMigrateCommand());
    }

    private void registerMetadataConverters() {
        metadataRegistry.register(VaultDefaultMetadata.ORDER.getKey(), new IntegerMetadataConverter());
        metadataRegistry.register(VaultDefaultMetadata.ICON.getKey(), new StringMetadataConverter());
    }

    private void registerListeners() {
        getServer().getPluginManager().registerEvents(new BukkitListener(), this);
        getServer().getPluginManager().registerEvents(new SelectorListener(), this);
        getServer().getPluginManager().registerEvents(new SelectIconListener(), this);
    }

    private void showStartUpMessage() {
        Arrays.asList(
                " ",
                "  &5EnderVaults &dv" + getDescription().getVersion(),
                "  &8Initializing...",
                " "
        )
                .stream()
                .map(s -> ChatColor.translateAlternateColorCodes('&', s))
                .forEach(s -> getServer().getConsoleSender().sendMessage(s));
    }
}
