use crate::config::Export as _;
use crate::config::{ConfigError, Secret};
use consensus::{ConsensusReceiverHandler};
use log::info;
use mempool::{ TxReceiverHandler, MempoolReceiverHandler};
use network::{Receiver as NetworkReceiver};
use std::sync::{Arc};
use std::collections::HashMap;
use tokio::sync::RwLock;
use std::net::SocketAddr;
use crate::dvfcore::DvfReceiverHandler;
/// The default channel capacity for this module.
/// 
pub struct Node {
    pub tx_handler_map : Arc<RwLock<HashMap<String, TxReceiverHandler>>>,
    pub mempool_handler_map : Arc<RwLock<HashMap<String, MempoolReceiverHandler>>>,
    pub consensus_handler_map: Arc<RwLock<HashMap<String, ConsensusReceiverHandler>>>,
}

impl Node {
    pub async fn new(
        tx_receiver_address: &str,
        mempool_receiver_address: &str,
        consensus_receiver_address: &str,
        dvfcore_receiver_address: &str,
        key_file: &str,
        store_path: &str,
        _parameters: Option<&str>,
    ) -> Result<Self, ConfigError> {
        // secret key from file.
        let secret = Secret::read(key_file)?;
        let name = secret.name;
        let secret_key = secret.secret;

        // Load default parameters if none are specified.
        // let parameters = match parameters {
        //     Some(filename) => Parameters::read(filename)?,
        //     None => Parameters::default(),
        // };

        let tx_handler_map = Arc::new(RwLock::new(HashMap::new()));
        let mempool_handler_map = Arc::new(RwLock::new(HashMap::new()));
        let consensus_handler_map = Arc::new(RwLock::new(HashMap::new()));

        let mut tx_network_address : SocketAddr = tx_receiver_address.parse().unwrap();
        tx_network_address.set_ip("0.0.0.0".parse().unwrap());
        NetworkReceiver::spawn(tx_network_address, Arc::clone(&tx_handler_map));
        info!("Mempool listening to client transactions on {}", tx_network_address);

        let mut mempool_network_address : SocketAddr = mempool_receiver_address.parse().unwrap();
        mempool_network_address.set_ip("0.0.0.0".parse().unwrap());
        NetworkReceiver::spawn(mempool_network_address, Arc::clone(&mempool_handler_map));
        info!("Mempool listening to mempool messages on {}", mempool_network_address);


        let mut consensus_network_address : SocketAddr = consensus_receiver_address.parse().unwrap();
        consensus_network_address.set_ip("0.0.0.0".parse().unwrap());
        NetworkReceiver::spawn(consensus_network_address, Arc::clone(&consensus_handler_map));
        info!(
            "Node {} listening to consensus messages on {}",
            name, consensus_network_address
        );
        
        
        // set dvfcore handler map
        let dvfcore_handler_map : Arc<RwLock<HashMap<String, DvfReceiverHandler>>>= Arc::new(RwLock::new(HashMap::new()));
        {
            let mut dvfcore_handlers = dvfcore_handler_map.write().await; 
            let empty_vec: Vec<u8> = vec![0; 88];
            let empty_id = String::from_utf8(empty_vec).unwrap();
            dvfcore_handlers.insert(
                empty_id, 
                DvfReceiverHandler { 
                    name: name, 
                    secret_key: secret_key,
                    base_store_path: store_path.to_string(),
                    tx_handler_map: Arc::clone(&tx_handler_map),
                    mempool_handler_map: Arc::clone(&mempool_handler_map),
                    consensus_handler_map: Arc::clone(&consensus_handler_map)
                }
            );
        }

        let mut dvfcore_network_address : SocketAddr = dvfcore_receiver_address.parse().unwrap();
        dvfcore_network_address.set_ip("0.0.0.0".parse().unwrap());
        NetworkReceiver::spawn(dvfcore_network_address, Arc::clone(&dvfcore_handler_map));
        info!("DvfCore listening to dvf messages on {}", dvfcore_network_address);

        info!("Node {} successfully booted", name);
        Ok(Self { tx_handler_map, mempool_handler_map, consensus_handler_map})
    }

    pub fn print_key_file(filename: &str) -> Result<(), ConfigError> {
        Secret::new().write(filename)
    }
}

