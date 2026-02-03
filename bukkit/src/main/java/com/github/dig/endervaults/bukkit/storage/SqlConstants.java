package com.github.dig.endervaults.bukkit.storage;

public final class SqlConstants {

    public static final String SQL_CREATE_TABLE_VAULT = "CREATE TABLE IF NOT EXISTS `%s` ( `id` VARCHAR(36) NOT NULL , `owner_uuid` VARCHAR(36) NOT NULL , `size` INT(8) NOT NULL , `contents` LONGTEXT NOT NULL , PRIMARY KEY (`id`, `owner_uuid`) )";
    public static final String SQL_CREATE_TABLE_VAULT_METADATA = "CREATE TABLE IF NOT EXISTS `%s` ( `id` VARCHAR(36) NOT NULL , `owner_uuid` VARCHAR(36) NOT NULL , `name` VARCHAR(16) NOT NULL , `value` TEXT NOT NULL , PRIMARY KEY (`id`, `owner_uuid`, `name`) )";

    public static final String DELETE = "DELETE FROM `%s` WHERE `owner_uuid` = ?";

    public static final class Vault {
        public static final String CHECK_VAULT = "SELECT `size` FROM `%s` WHERE `id` = ? AND `owner_uuid` = ?";
        public static final String SELECT_VAULT = "SELECT `size`, `contents` FROM `%s` WHERE `id` = ? AND `owner_uuid` = ?";
        public static final String SELECT_VAULTS = "SELECT `id`, `size` FROM `%s` WHERE `owner_uuid` = ?";
        public static final String INSERT_VAULT = "INSERT INTO `%s` (`id`, `owner_uuid`, `size`, `contents`) VALUES (?, ?, ?, ?)";
        public static final String SELECT_CONTENT = "SELECT `contents` FROM `%s` WHERE `id` = ?";
        public static final String UPDATE_CONTENT = "UPDATE `%s` SET `size` = ?, `contents` = ? WHERE `id` = ? AND `owner_uuid` = ?";
    }

    public static final class Metadata {
        public static final String SELECT_KEY_VALUE = "SELECT `name`, `value` FROM `%s` WHERE `id` = ? AND `owner_uuid` = ?";
        public static final String SELECT_VALUE = "SELECT `value` FROM `%s` WHERE `id` = ? AND `owner_uuid` = ? AND `name` = ?";
        public static final String SELECT_ID = "SELECT `id` FROM `%s` WHERE `owner_uuid` = ? AND `name` = ? AND `value` = ?";
        public static final String INSERT = "INSERT INTO `%s` (`id`, `owner_uuid`, `name`, `value`) VALUES (?, ?, ?, ?)";
        public static final String UPDATE = "UPDATE `%s` SET `value` = ? WHERE `id` = ? AND `owner_uuid` = ? AND `name` = ?";
        public static final String DELETE = "DELETE FROM `%s` WHERE `id` = ? AND `name` = ?";
    }
}
