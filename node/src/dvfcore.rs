use crate::config::Export as _;
use crate::config::{Committee, ConfigError, Parameters};
use crypto::{PublicKey, SecretKey};
use consensus::{Block, Consensus, ConsensusReceiverHandler};
use crypto::SignatureService;
use log::info;
use mempool::{Mempool, TxReceiverHandler, MempoolReceiverHandler};
use store::Store;
use tokio::sync::mpsc::{channel, Receiver};
use network::{MessageHandler, Writer, PREFIX_LEN};
use std::sync::{Arc};
use async_trait::async_trait;
use std::collections::HashMap;
use bytes::Bytes;
use std::error::Error;
use tokio::sync::RwLock;
use log::error;

pub const CHANNEL_CAPACITY: usize = 1_000;
#[derive(Clone)]
pub struct DvfReceiverHandler {
  pub name: PublicKey,
  pub secret_key: SecretKey,
  pub base_store_path: String,
  pub tx_handler_map : Arc<RwLock<HashMap<String, TxReceiverHandler>>>,
  pub mempool_handler_map : Arc<RwLock<HashMap<String, MempoolReceiverHandler>>>,
  pub consensus_handler_map: Arc<RwLock<HashMap<String, ConsensusReceiverHandler>>>
}

#[async_trait]
impl MessageHandler for DvfReceiverHandler {
    async fn dispatch(&self, _writer: &mut Writer, message: Bytes) -> Result<(), Box<dyn Error>> {
        let prefix = String::from_utf8(message[0..PREFIX_LEN-1].to_vec()).unwrap();
        let committee_str = String::from_utf8(message[PREFIX_LEN..].to_vec()).unwrap();

        match DvfCore::new(
          &committee_str.as_bytes().to_vec(),
          self.name,
          self.secret_key.clone(),
          prefix,
          self.base_store_path.clone(),
          Arc::clone(&self.tx_handler_map),
          Arc::clone(&self.mempool_handler_map),
          Arc::clone(&self.consensus_handler_map)
        ).await {
          Ok(mut dvfcore) => {
            tokio::spawn(async move {
              dvfcore.analyze_block().await;
            })
            .await
            .expect("Failed to analyze committed blocks");
          }
          Err(e) => {
            error!("{}", e);
          }
        };

        // Give the change to schedule other tasks.
        tokio::task::yield_now().await;
        Ok(())
    }
}

pub struct DvfCore {
  pub commit: Receiver<Block>
}

impl DvfCore {
  pub async fn new(
    committee_str: &Vec<u8>,
    name: PublicKey,
    secret_key: SecretKey,
    validator_id: String,
    base_store_path: String,
    tx_handler_map : Arc<RwLock<HashMap<String, TxReceiverHandler>>>,
    mempool_handler_map : Arc<RwLock<HashMap<String, MempoolReceiverHandler>>>,
    consensus_handler_map: Arc<RwLock<HashMap<String, ConsensusReceiverHandler>>>,
  ) -> Result<Self, ConfigError> {
    let (tx_commit, rx_commit) = channel(CHANNEL_CAPACITY);
    let (tx_consensus_to_mempool, rx_consensus_to_mempool) = channel(CHANNEL_CAPACITY);
    let (tx_mempool_to_consensus, rx_mempool_to_consensus) = channel(CHANNEL_CAPACITY);

    let committee = Committee::parse(committee_str)?;
    let parameters = Parameters::default();

    let store_path = base_store_path + "/" + &validator_id;
    let store = Store::new(&store_path).expect("Failed to create store");

    // Run the signature service.
    let signature_service = SignatureService::new(secret_key);

    info!(
      "Node {} create a dvfcore instance with validator id {}",
      name, &validator_id
    );

    Mempool::spawn(
      name,
      committee.mempool,
      parameters.mempool,
      store.clone(),
      rx_consensus_to_mempool,
      tx_mempool_to_consensus,
      validator_id.clone(),
      tx_handler_map,
      mempool_handler_map
    );

    Consensus::spawn(
      name,
      committee.consensus,
      parameters.consensus,
      signature_service,
      store,
      rx_mempool_to_consensus,
      tx_consensus_to_mempool,
      tx_commit,
      validator_id.clone(),
      consensus_handler_map
    );
    info!("dvfcore {} successfully booted", validator_id);
    Ok(Self { commit: rx_commit })
  }

  pub async fn analyze_block(&mut self) {
    while let Some(_block) = self.commit.recv().await {
        // This is where we can further process committed block.
    }
  }
}