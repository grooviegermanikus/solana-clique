use std::sync::{Arc, RwLock};
use solana_geyser_plugin_manager::geyser_plugin_manager::GeyserPluginManager;

pub fn main() {
    solana_logger::setup();

    // Initialize empty manager
    let plugin_manager = Arc::new(RwLock::new(GeyserPluginManager::new()));
    let mut plugin_manager_lock = plugin_manager.write().unwrap();

    // Load rpc call
    let load_result = plugin_manager_lock.load_plugin("geyser-plugin-config.json");
    assert!(load_result.is_ok());
    assert_eq!(plugin_manager_lock.plugins.len(), 1);


}

