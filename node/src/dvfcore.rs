use crate::config::{Committee, ConfigError, Parameters};
use crypto::{PublicKey, SecretKey};
use consensus::{Block, Consensus, ConsensusReceiverHandler};
use crypto::SignatureService;
use log::info;
use mempool::{Mempool, TxReceiverHandler, MempoolReceiverHandler};
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use network::{MessageHandler, Writer};
use std::sync::{Arc};
use async_trait::async_trait;
use std::collections::HashMap;
use bytes::Bytes;
use std::error::Error;
use tokio::sync::RwLock;
use serde::{Deserialize, Serialize};
use std::fmt;
pub const CHANNEL_CAPACITY: usize = 1_000;
use bls::{Hash256, Signature};
use std::net::SocketAddr;
use types::Keypair;
#[derive(Serialize, Deserialize, Clone)]
pub struct DvfInfo {
  pub validator_id : String,
  pub committee: Committee
}

impl fmt::Debug for DvfInfo {
  fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
      write!(
          f,
          "{}: Commitee({:?})",
          self.validator_id.clone(),
          serde_json::to_string_pretty(&self.committee)
      )
  }
}

#[derive(Clone)]
pub struct DvfReceiverHandler {
  // pub name: PublicKey,
  // pub secret_key: SecretKey,
  // pub base_store_path: String,
  // pub tx_handler_map : Arc<RwLock<HashMap<String, TxReceiverHandler>>>,
  // pub mempool_handler_map : Arc<RwLock<HashMap<String, MempoolReceiverHandler>>>,
  // pub consensus_handler_map: Arc<RwLock<HashMap<String, ConsensusReceiverHandler>>>,
  pub tx_dvfinfo : Sender<DvfInfo>
}

#[async_trait]
impl MessageHandler for DvfReceiverHandler {
    async fn dispatch(&self, _writer: &mut Writer, message: Bytes) -> Result<(), Box<dyn Error>> {
        let dvfinfo = serde_json::from_slice(&message.to_vec())?;
        self.tx_dvfinfo.send(dvfinfo).await.unwrap();
        // Give the change to schedule other tasks.
        tokio::task::yield_now().await;
        Ok(())
    }
}



#[derive(Serialize, Deserialize, Clone)]
pub struct SignatureInfo {
  pub from : bls::PublicKey,
  pub signature: Signature,
  pub msg : Hash256
}

impl fmt::Debug for SignatureInfo {
  fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
      write!(
          f,
          "from: {:?}, signature: {:?}, msg: {:?}",
          self.from,
          self.signature,
          self.msg
      )
  }
}

#[derive(Clone)]
pub struct DvfSignatureReceiverHandler {
  pub tx_signature : Sender<SignatureInfo>
}

#[async_trait]
impl MessageHandler for DvfSignatureReceiverHandler {
    async fn dispatch(&self, _writer: &mut Writer, message: Bytes) -> Result<(), Box<dyn Error>> {
        println!("receive a signature");
        let signature_info = serde_json::from_slice(&message.to_vec())?;
        self.tx_signature.send(signature_info).await.unwrap();
        // Give the change to schedule other tasks.
        tokio::task::yield_now().await;
        Ok(())
    }
}

pub struct DvfCore {
  pub store: Store,
  pub commit: Receiver<Block>,
  pub broadcast_signature_addresses : Vec<SocketAddr>,
  pub validator_id: String
}

impl DvfCore {
  pub async fn new(
    committee: Committee,
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

    let parameters = Parameters::default();

    let store_path = base_store_path + "/" + &validator_id;
    let store = Store::new(&store_path).expect("Failed to create store");

    // Run the signature service.
    let signature_service = SignatureService::new(secret_key);

    info!(
      "Node {} create a dvfcore instance with validator id {}",
      name, validator_id.clone()
    );

    let broadcast_signature_addresses = committee.mempool.broadcast_signature_addresses(&name);

    Mempool::spawn(
      name,
      committee.mempool,
      parameters.mempool,
      store.clone(),
      rx_consensus_to_mempool,
      tx_mempool_to_consensus,
      validator_id.clone(),
      Arc::clone(&tx_handler_map),
      Arc::clone(&mempool_handler_map)
    );

    Consensus::spawn(
      name,
      committee.consensus,
      parameters.consensus,
      signature_service,
      store.clone(),
      rx_mempool_to_consensus,
      tx_consensus_to_mempool,
      tx_commit,
      validator_id.clone(),
      Arc::clone(&consensus_handler_map)
    );
    info!("dvfcore {} successfully booted", validator_id);
    Ok(Self { commit: rx_commit, store: store, broadcast_signature_addresses: broadcast_signature_addresses, validator_id: validator_id})
    
  }

  pub async fn analyze_block(&mut self) {
    while let Some(_block) = self.commit.recv().await {
        // This is where we can further process committed block.
        info!("receive consensus block");
    }
  }
}