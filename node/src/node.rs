use crate::config::Export as _;
use crate::config::{ConfigError, Secret};
use log::{info, error};
use consensus::{ConsensusReceiverHandler};
use mempool::{TxReceiverHandler, MempoolReceiverHandler};
use network::{Receiver as NetworkReceiver};
use std::sync::{Arc};
use std::collections::HashMap;
use tokio::sync::RwLock;
use std::net::SocketAddr;
use crate::dvfcore::DvfCore;
use tokio::sync::mpsc::{channel, Receiver};
use crypto::{PublicKey, SecretKey};
/// The default channel capacity for this module.
use crate::dvfcore::{DvfInfo, DvfReceiverHandler, DvfSignatureReceiverHandler};
pub struct Node {
    pub name : PublicKey,
    pub secret_key: SecretKey,
    pub base_store_path: String,
    pub rx_dvfinfo: Receiver<DvfInfo>,
    pub tx_handler_map : Arc<RwLock<HashMap<String, TxReceiverHandler>>>,
    pub mempool_handler_map : Arc<RwLock<HashMap<String, MempoolReceiverHandler>>>,
    pub consensus_handler_map: Arc<RwLock<HashMap<String, ConsensusReceiverHandler>>>,
    pub signature_handler_map: Arc<RwLock<HashMap<String, DvfSignatureReceiverHandler>>>,
}
impl Node {
    pub async fn new(
        tx_receiver_address: &str,
        mempool_receiver_address: &str,
        consensus_receiver_address: &str,
        dvfcore_receiver_address: &str,
        signature_receiver_address: &str,
        secret: Secret,
        store_path: &str,
        _parameters: Option<&str>,
    ) -> Result<Self, ConfigError> {
        // secret key from file.
        let name = secret.name;
        let secret_key = secret.secret;
        let base_store_path = store_path.to_string();
        // Load default parameters if none are specified.
        // let parameters = match parameters {
        //     Some(filename) => Parameters::read(filename)?,
        //     None => Parameters::default(),
        // };

        let tx_handler_map = Arc::new(RwLock::new(HashMap::new()));
        let mempool_handler_map = Arc::new(RwLock::new(HashMap::new()));
        let consensus_handler_map = Arc::new(RwLock::new(HashMap::new()));
        let signature_handler_map = Arc::new(RwLock::new(HashMap::new()));

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

        let mut signature_network_address : SocketAddr = signature_receiver_address.parse().unwrap();
        signature_network_address.set_ip("0.0.0.0".parse().unwrap());
        NetworkReceiver::spawn(signature_network_address, Arc::clone(&signature_handler_map));
        info!(
            "Node {} listening to signature messages on {}",
            name, signature_network_address
        );
        
        // set dvfcore handler map
        let dvfcore_handler_map : Arc<RwLock<HashMap<String, DvfReceiverHandler>>>= Arc::new(RwLock::new(HashMap::new()));
        let (tx_dvfinfo, rx_dvfinfo) = channel(1);
        {
            let mut dvfcore_handlers = dvfcore_handler_map.write().await; 
            let empty_vec: Vec<u8> = vec![48; 88];
            let empty_id = String::from_utf8(empty_vec).unwrap();
            
            dvfcore_handlers.insert(
                empty_id,
                DvfReceiverHandler {
                    tx_dvfinfo
                }
            );
        }
        
        let mut dvfcore_network_address : SocketAddr = dvfcore_receiver_address.parse().unwrap();
        dvfcore_network_address.set_ip("0.0.0.0".parse().unwrap());
        NetworkReceiver::spawn(dvfcore_network_address, Arc::clone(&dvfcore_handler_map));
        info!("DvfCore listening to dvf messages on {}", dvfcore_network_address);

        info!("Node {} successfully booted", name);
        Ok(Self { name, secret_key, base_store_path, rx_dvfinfo, tx_handler_map: Arc::clone(&tx_handler_map), mempool_handler_map: Arc::clone(&mempool_handler_map), consensus_handler_map: Arc::clone(&consensus_handler_map), signature_handler_map: Arc::clone(&signature_handler_map)})
    }

    pub fn print_key_file(filename: &str) -> Result<(), ConfigError> {
        Secret::new().write(filename)
    }

    pub async fn process_dvfinfo(&mut self) {
        while let Some(dvfinfo) = self.rx_dvfinfo.recv().await {
            // This is where we can further process committed block.
            info!("received validator {}", dvfinfo.validator_id);
            match DvfCore::new(
                dvfinfo.committee,
                self.name.clone(),
                self.secret_key.clone(),
                dvfinfo.validator_id,
                self.base_store_path.clone(),
                Arc::clone(&self.tx_handler_map),
                Arc::clone(&self.mempool_handler_map),
                Arc::clone(&self.consensus_handler_map)
              ).await {
                Ok(mut dvfcore) => {
                  tokio::spawn(async move {
                    // dvfcore.analyze_block().await;
                  })
                  .await
                  .expect("Failed to analyze committed blocks");
                }
                Err(e) => {
                  error!("{}", e);
                }
              };
        }
      }
}

