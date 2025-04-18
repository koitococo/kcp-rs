use std::{
  io::{self,},
  net::{Ipv4Addr, SocketAddr, SocketAddrV4},
  str::FromStr,
  sync::Arc,
  time::Duration,
};

use bytes::Bytes;
use clap::Parser;
use kcp::{AsyncBufSend, Kcp, KcpError};
use log::{Log, info};
use tokio::{io::{AsyncReadExt as _, AsyncWriteExt as _}, net::UdpSocket};

pub mod kcp;

#[derive(Parser)]
struct Cli {
  /// The address to connect to
  #[clap(short = 'A', long)]
  addr: String,
  /// The port to connect to
  #[clap(short = 'P', long)]
  port: u16,
  /// The local addr to bind to
  #[clap(short = 'a', long)]
  local_addr: String,
  /// The local port to bind to
  #[clap(short = 'p', long)]
  local_port: u16,
}

struct SimpleLogger();
impl Log for SimpleLogger {
  fn enabled(&self, _: &log::Metadata) -> bool { true }

  fn log(&self, record: &log::Record) {
    eprintln!("{}: {}", record.level(), record.args());
  }

  fn flush(&self) {}
}

impl SimpleLogger {
  fn new() -> Self { Self {} }

  fn init(self) {
    log::set_max_level(log::LevelFilter::Trace);
    log::set_boxed_logger(Box::new(self)).unwrap();
  }
}

impl AsyncBufSend for Arc<UdpSocket> {
  async fn send_buf(&self, buf: &[u8]) -> Result<usize, KcpError> {
    let mut buf = buf.to_vec();
    let len = self.send(buf.as_mut_slice()).await?;
    info!("Send {} bytes via socket {:?}", len, buf);
    Ok(len)
  }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  SimpleLogger::new().init();

  let cli = Cli::parse();

  let socket = UdpSocket::bind(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::from_str(&cli.local_addr.as_str())?, cli.local_port))).await?;

  info!("Listening on {}", socket.local_addr()?);
  let dest = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::from_str(cli.addr.as_str())?, cli.port));
  info!("Connecting to {}", dest);
  socket.connect(dest).await?;
  let r = Arc::new(socket);
  let s = r.clone();

  let mut kcp = Kcp::new(0x114514, r, true, false, 0);
  info!("Setting up protocol");

  let now = tokio::time::Instant::now();
  let pre_recv = async || -> Result<Option<Bytes>, io::Error> {
    let mut recv_buf = vec![0u8;64 * 1024];
    let (len, remote) = s.recv_from(recv_buf.as_mut()).await?;
    info!("Received {} bytes from socket from {}", len, remote);
    recv_buf.truncate(len);
    Ok(Some(Bytes::from_owner(recv_buf)))
  };

  let mut stdout = tokio::io::stdout();
  let mut post_recv = async |buf: Bytes, kcp: &mut Kcp<Arc<UdpSocket>>| -> anyhow::Result<bool> {
    kcp.input(buf.as_ref())?;
    let mut buf = vec![0u8;64 * 1024];
    let r = kcp.recv(&mut buf);
    match r {
      Ok(size) => {
        buf.truncate(size);
        info!("Write {} bytes to stdout", size);
        stdout.write_all(buf.as_ref()).await?;
      }
      Err(_) => (),
    }
    Ok(true)
  };

  let mut stdin = tokio::io::stdin();
  let mut pre_send = async || -> Result<Option<Bytes>, io::Error> {
    let mut send_buf = vec![0u8; 64 * 1024];
    let len = stdin.read(&mut send_buf).await?;
    if len == 0 {
      return Ok(None);
    }
    info!("Read {} bytes from stdin", len);
    send_buf.truncate(len);
    Ok(Some(Bytes::from_owner(send_buf)))
  };

  let post_send = async |buf: Bytes, kcp: &mut Kcp<Arc<UdpSocket>>| -> anyhow::Result<bool> {
    let len = kcp.send(buf.as_ref())?;
    info!("Send {} bytes to kcp", len);
    kcp.async_update(now.elapsed().as_millis() as u32).await?;
    Ok(true)
  };

  let mut int = tokio::time::interval(Duration::from_millis(100));

  info!("Starting loop");
  loop {
    tokio::select! {
      _ = int.tick() => {
        kcp.async_update(now.elapsed().as_millis() as u32).await?;
      },
      r = pre_recv() => {
        if let Some(r) = r? {
          post_recv(r, &mut kcp).await?;
        }
      },
      r = pre_send() => {
        if let Some(r) = r? {
          post_send(r, &mut kcp).await?;
        }
      }
    };
  }
}
