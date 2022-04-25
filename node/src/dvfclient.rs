mod config;
mod dvfcore;
use anyhow::{Context, Result};
use bytes::Bytes;
use clap::{crate_name, crate_version, App, AppSettings};
use env_logger::Env;
use futures::sink::SinkExt as _;
use log::{info, warn};
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use crate::config::Committee;
use crate::config::Export;
use crate::dvfcore::DvfInfo;
#[tokio::main]
async fn main() -> Result<()> {
  let matches = App::new(crate_name!())
    .version(crate_version!())
    .about("client for HotStuff nodes.")
    .args_from_usage("<ADDR> 'The network address of the node where to send dvf command'")
    .args_from_usage("<FILE> 'The file of committee info.'")
    .setting(AppSettings::ArgRequiredElseHelp)
    .get_matches();
  env_logger::Builder::from_env(Env::default().default_filter_or("info"))
    .format_timestamp_millis()
    .init();

  let target = matches
    .value_of("ADDR")
    .unwrap()
    .parse::<SocketAddr>()
    .context("Invalid socket address format")?; 
  
  let committee_file = matches
    .value_of("FILE")
    .unwrap(); 

  let committee = Committee::read(committee_file)?;
  info!("Node address: {}", target);

  // Connect to the mempool.
  let stream = TcpStream::connect(target)
  .await
  .context(format!("failed to connect to {}", target))?;

  let mut transport = Framed::new(stream, LengthDelimitedCodec::new());

  let validator_vec : Vec<u8>= vec![50; 88];
  let validator_id = String::from_utf8(validator_vec).unwrap();
  let dvfinfo = DvfInfo { validator_id, committee };
  let empty_vec : Vec<u8>= vec![48;88];
  let mut prefix_msg : Vec<u8> = Vec::new();
  prefix_msg.extend(empty_vec);
  let dvfinfo_bytes = serde_json::to_vec(&dvfinfo).unwrap();
  prefix_msg.extend(dvfinfo_bytes);
  if let Err(e) = transport.send(Bytes::from(prefix_msg)).await {
    warn!("Failed to send dvf command: {}", e);
  }
  Ok(())
}