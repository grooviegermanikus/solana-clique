use std::sync::{Arc, RwLock};
use solana_geyser_plugin_manager::geyser_plugin_manager::GeyserPluginManager;

pub fn main() {
    solana_logger::setup();

    // Initialize empty manager
    let plugin_manager = Arc::new(RwLock::new(GeyserPluginManager::new()));
    let mut plugin_manager_lock = plugin_manager.write().unwrap();
    let filename = "geyser-plugin-config.json";

    println!("Trying to load from config file {}", filename);
    let load_result = plugin_manager_lock.load_plugin(filename);

    println!("PLUGIN LOADED: {:?}", load_result.is_ok());


}

