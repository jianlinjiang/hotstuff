use anyhow::{Context, Result};
use bytes::Bytes;
use clap::{crate_name, crate_version, App, AppSettings};
use env_logger::Env;
use futures::sink::SinkExt as _;
use log::{warn};
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[tokio::main]
async fn main() -> Result<()> {
  let matches = App::new(crate_name!())
    .version(crate_version!())
    .about("client for HotStuff nodes.")
    .args_from_usage("<ADDR> 'The network address of the node where to send tx transaction'")
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
  let stream = TcpStream::connect(target)
    .await
    .context(format!("failed to connect to {}", target))?;
  let mut transport = Framed::new(stream, LengthDelimitedCodec::new());
  let mut message : Vec<u8>= vec![50; 88];
  let data: Vec<u8> = vec![96; 32];
  message.extend(data);
  if let Err(e) = transport.send(Bytes::from(message)).await {
    println!("Failed to send transaction: {}", e);
  } 
  Ok(())
}