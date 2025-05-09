use bytes::{Buf, BufMut, BytesMut};
use std::{
  cmp,
  cmp::Ordering,
  collections::VecDeque,
  fmt::Debug,
  io::{self, Cursor, ErrorKind, Read, Write},
};

const KCP_RTO_NDL: u32 = 30; // no delay min rto
const KCP_RTO_MIN: u32 = 100; // normal min rto
const KCP_RTO_DEF: u32 = 200;
const KCP_RTO_MAX: u32 = 60000;
const KCP_CMD_PUSH: u8 = 81; // cmd: push data
const KCP_CMD_ACK: u8 = 82; // cmd: ack
const KCP_CMD_WASK: u8 = 83; // cmd: window probe (ask)
const KCP_CMD_WINS: u8 = 84; // cmd: window size (tell)
const KCP_ASK_SEND: u32 = 1; // need to send IKCP_CMD_WASK
const KCP_ASK_TELL: u32 = 2; // need to send IKCP_CMD_WINS
const KCP_WND_SND: u16 = 32;
const KCP_WND_RCV: u16 = 128; // must >= max fragment size
const KCP_MTU_DEF: usize = 1400;
const KCP_INTERVAL: u32 = 100;
const KCP_OVERHEAD: usize = 24; // KCP Header size
const KCP_DEADLINK: u32 = 20;
const KCP_THRESH_INIT: u16 = 2;
const KCP_THRESH_MIN: u16 = 2;
const KCP_PROBE_INIT: u32 = 7000; // 7 secs to probe window size
const KCP_PROBE_LIMIT: u32 = 120000; // up to 120 secs to probe window
const KCP_FASTACK_LIMIT: u32 = 5; // max times to trigger fastack

/// KCP protocol errors
#[derive(Debug, thiserror::Error)]
pub enum KcpError {
  #[error("conv inconsistent, expected {0}, found {1}")]
  ConversationIdMismatch(u32, u32),
  #[error("invalid mtu {0}")]
  InvalidMtu(usize),
  #[error("invalid segment size {0}")]
  InvalidSegmentSize(usize),
  #[error("invalid segment data size, expected {0}, found {1}")]
  InvalidSegmentDataSize(usize, usize),
  #[error("{0}")]
  IoError(
    #[from]
    #[source]
    io::Error,
  ),
  #[error("recv queue is empty")]
  RecvQueueEmpty,
  #[error("expecting fragment")]
  ExpectingFragment,
  #[error("command {0} is not supported")]
  UnsupportedCmd(u8),
  #[error("invalid length of buffer")]
  InvalidBufSize,
  #[error("closed conversation")]
  ClosedConv,
}

impl From<KcpError> for io::Error {
  fn from(err: KcpError) -> io::Error {
    match err {
      KcpError::IoError(err) => err,
      KcpError::ExpectingFragment | KcpError::RecvQueueEmpty => io::Error::new(ErrorKind::WouldBlock, err),
      _ => io::Error::new(ErrorKind::Other, err),
    }
  }
}

#[inline]
fn timediff(later: u32, earlier: u32) -> i32 { later as i32 - earlier as i32 }

#[derive(Default, Clone, Debug)]
struct KcpSegment {
  conv: u32,
  cmd: u8,
  frg: u8,
  wnd: u16,
  ts: u32,
  sn: u32,
  una: u32,
  resendts: u32,
  rto: u32,
  fastack: u32,
  xmit: u32,
  data: BytesMut,
}

impl KcpSegment {
  fn new_with_data(data: BytesMut) -> Self {
    KcpSegment {
      conv: 0,
      cmd: 0,
      frg: 0,
      wnd: 0,
      ts: 0,
      sn: 0,
      una: 0,
      resendts: 0,
      rto: 0,
      fastack: 0,
      xmit: 0,
      data,
    }
  }

  fn encode(&self, buf: &mut BytesMut) {
    if buf.remaining_mut() < self.encoded_len() {
      panic!("REMAIN {} encoded {} {:?}", buf.remaining_mut(), self.encoded_len(), self);
    }
    buf.put_u32_le(self.conv);
    buf.put_u8(self.cmd);
    buf.put_u8(self.frg);
    buf.put_u16_le(self.wnd);
    buf.put_u32_le(self.ts);
    buf.put_u32_le(self.sn);
    buf.put_u32_le(self.una);
    buf.put_u32_le(self.data.len() as u32);
    buf.put_slice(&self.data);
  }

  fn encoded_len(&self) -> usize { KCP_OVERHEAD as usize + self.data.len() }
}

pub trait AsyncBufSend {
  fn send_buf(&self, buf: &[u8]) -> impl std::future::Future<Output = Result<usize, KcpError>>;
}

#[derive(Debug)]
struct AnyWrite<T>(T);

impl<T: AsyncBufSend> AsyncBufSend for AnyWrite<T> {
  async fn send_buf(&self, buf: &[u8]) -> Result<usize, KcpError> { self.0.send_buf(buf).await }
}

#[derive(Debug)]
pub struct Kcp<T> {
  /// Conversation ID
  conv: u32,
  /// Maximum Transmission Unit
  mtu: usize,
  /// Maximum Segment Size
  mss: usize,
  /// Connection state
  available: bool,
  /// First unacknowledged packet
  snd_una: u32,
  /// Next packet
  snd_nxt: u32,
  /// Next packet to be received
  rcv_nxt: u32,
  /// Congestion window threshold
  ssthresh: u16,
  /// ACK receive variable RTT
  rx_rttval: u32,
  /// ACK receive static RTT
  rx_srtt: u32,
  /// Resend time (calculated by ACK delay time)
  rx_rto: u32,
  /// Minimal resend timeout
  rx_minrto: u32,
  /// Send window
  snd_wnd: u16,
  /// Receive window
  rcv_wnd: u16,
  /// Remote receive window
  rmt_wnd: u16,
  /// Congestion window
  cwnd: u16,
  /// Check window
  /// - IKCP_ASK_TELL, telling window size to remote
  /// - IKCP_ASK_SEND, ask remote for window size
  probe: u32,
  /// Last update time
  current: u32,
  /// Flush interval
  interval: u32,
  /// Next flush interval
  ts_flush: u32,
  xmit: u32,
  /// Enable nodelay
  nodelay: bool,
  /// Next check window timestamp
  ts_probe: u32,
  /// Check window wait time
  probe_wait: u32,
  /// Maximum resend time
  dead_link: u32,
  /// Maximum payload size
  incr: usize,
  snd_queue: VecDeque<KcpSegment>,
  rcv_queue: VecDeque<KcpSegment>,
  snd_buf: VecDeque<KcpSegment>,
  rcv_buf: VecDeque<KcpSegment>,
  /// Pending ACK
  acklist: VecDeque<(u32, u32)>,
  buf: BytesMut,
  /// ACK number to trigger fast resend
  fastresend: u32,
  fastlimit: u32,
  /// Disable congestion control
  nocwnd: bool,
  output: AnyWrite<T>,
  fastack: bool,
}

impl<T> Kcp<T> {
  /// Creates a KCP control object, `conv` must be equal in both endpoints in one connection.
  /// `output` is the callback object for writing.
  ///
  /// `conv` represents conversation.
  pub fn new(conv: u32, output: T, fastack: bool, now: u32) -> Self {
    Kcp {
      conv,
      snd_una: 0,
      snd_nxt: 0,
      rcv_nxt: 0,
      ts_probe: 0,
      probe_wait: 0,
      snd_wnd: KCP_WND_SND,
      rcv_wnd: KCP_WND_RCV,
      rmt_wnd: KCP_WND_RCV,
      cwnd: 0,
      incr: 0,
      probe: 0,
      mtu: KCP_MTU_DEF,
      mss: KCP_MTU_DEF - KCP_OVERHEAD,
      buf: BytesMut::with_capacity((KCP_MTU_DEF + KCP_OVERHEAD) as usize * 3),
      snd_queue: VecDeque::new(),
      rcv_queue: VecDeque::new(),
      snd_buf: VecDeque::new(),
      rcv_buf: VecDeque::new(),
      available: true,
      acklist: VecDeque::new(),
      rx_srtt: 0,
      rx_rttval: 0,
      rx_rto: KCP_RTO_DEF,
      rx_minrto: KCP_RTO_MIN,
      current: now,
      interval: KCP_INTERVAL,
      ts_flush: now,
      nodelay: false,
      ssthresh: KCP_THRESH_INIT,
      fastresend: 0,
      fastlimit: KCP_FASTACK_LIMIT,
      nocwnd: false,
      xmit: 0,
      dead_link: KCP_DEADLINK,
      output: AnyWrite(output),
      fastack,
    }
  }

  // move available data from rcv_buf -> rcv_queue
  fn move_buf(&mut self) {
    while !self.rcv_buf.is_empty() {
      let nrcv_que = self.rcv_queue.len();
      {
        let seg = self.rcv_buf.front().unwrap();
        if seg.sn == self.rcv_nxt && nrcv_que < self.rcv_wnd as usize {
          self.rcv_nxt += 1;
        } else {
          break;
        }
      }
      let seg = self.rcv_buf.pop_front().unwrap();
      self.rcv_queue.push_back(seg);
    }
  }

  /// Receive data from buffer
  pub fn recv(&mut self, buf: &mut [u8]) -> Result<usize, KcpError> {
    if self.rcv_queue.is_empty() {
      return Err(KcpError::RecvQueueEmpty);
    }
    let peeksize = self.peeksize()?;
    if peeksize > buf.len() {
      return Err(KcpError::InvalidBufSize);
    }
    let recover = self.rcv_queue.len() >= self.rcv_wnd as usize;
    // Merge fragment
    let mut cur = Cursor::new(buf);
    while let Some(seg) = self.rcv_queue.pop_front() {
      Write::write_all(&mut cur, &seg.data)?;
      if seg.frg == 0 {
        break;
      }
    }
    assert_eq!(cur.position() as usize, peeksize);
    self.move_buf();
    // fast recover
    if self.rcv_queue.len() < self.rcv_wnd as usize && recover {
      // ready to send back IKCP_CMD_WINS in ikcp_flush
      // tell remote my window size
      self.probe |= KCP_ASK_TELL;
    }
    Ok(cur.position() as usize)
  }

  /// Check buffer size without actually consuming it
  pub fn peeksize(&self) -> Result<usize, KcpError> {
    match self.rcv_queue.front() {
      Some(segment) => {
        if segment.frg == 0 {
          return Ok(segment.data.len());
        }
        if self.rcv_queue.len() < (segment.frg + 1) as usize {
          return Err(KcpError::ExpectingFragment);
        }
        let mut len = 0;
        for segment in &self.rcv_queue {
          len += segment.data.len();
          if segment.frg == 0 {
            break;
          }
        }
        Ok(len)
      }
      None => Err(KcpError::RecvQueueEmpty),
    }
  }

  /// Send bytes into buffer
  pub fn send(&mut self, mut buf: &[u8]) -> Result<usize, KcpError> {
    if !self.available {
      return Err(KcpError::ClosedConv);
    }
    let mut sent_size = 0;
    let count = buf.len().div_ceil(self.mss);
    if count >= KCP_WND_RCV as usize || count == 0 {
      return Err(KcpError::InvalidBufSize);
    }
    for i in 0..count {
      let size = cmp::min(self.mss as usize, buf.len());
      let (lf, rt) = buf.split_at(size);
      let mut new_segment = KcpSegment::new_with_data(lf.into());
      buf = rt;
      new_segment.frg = (count - i - 1) as u8 ;
      self.snd_queue.push_back(new_segment);
      sent_size += size;
    }
    Ok(sent_size)
  }

  fn update_ack(&mut self, rtt: u32) {
    if self.rx_srtt == 0 {
      self.rx_srtt = rtt;
      self.rx_rttval = rtt / 2;
    } else {
      let delta = if rtt > self.rx_srtt { rtt - self.rx_srtt } else { self.rx_srtt - rtt };
      self.rx_rttval = (3 * self.rx_rttval + delta) / 4;
      self.rx_srtt = (7 * self.rx_srtt + rtt) / 8;
      if self.rx_srtt < 1 {
        self.rx_srtt = 1;
      }
    }
    let rto = self.rx_srtt + cmp::max(self.interval, 4 * self.rx_rttval);
    self.rx_rto = if rto < self.rx_minrto {
      self.rx_minrto
    } else if rto > KCP_RTO_MAX {
      KCP_RTO_MAX
    } else {
      rto
    };
  }

  #[inline]
  fn shrink_buf(&mut self) {
    self.snd_una = match self.snd_buf.front() {
      Some(seg) => seg.sn,
      None => self.snd_nxt,
    };
  }

  fn parse_ack(&mut self, sn: u32) {
    if timediff(sn, self.snd_una) < 0 || timediff(sn, self.snd_nxt) >= 0 {
      return;
    }
    let mut i = 0 as usize;
    while i < self.snd_buf.len() {
      match sn.cmp(&self.snd_buf[i].sn) {
        Ordering::Equal => {
          self.snd_buf.remove(i);
          break;
        }
        Ordering::Less => break,
        _ => i = i + 1,
      }
    }
  }

  fn parse_una(&mut self, una: u32) {
    while let Some(seg) = self.snd_buf.front() {
      if timediff(una, seg.sn) > 0 {
        self.snd_buf.pop_front();
      } else {
        break;
      }
    }
  }

  fn parse_fastack(&mut self, sn: u32, ts: u32) {
    if timediff(sn, self.snd_una) < 0 || timediff(sn, self.snd_nxt) >= 0 {
      return;
    }
    for seg in &mut self.snd_buf {
      if timediff(sn, seg.sn) < 0 {
        break;
      } else if sn != seg.sn {
        if self.fastack {
          seg.fastack += 1;
        } else if timediff(ts, seg.ts) >= 0 {
          seg.fastack += 1;
        }
      }
    }
  }

  fn parse_data(&mut self, new_segment: KcpSegment) {
    let sn = new_segment.sn;
    if timediff(sn, self.rcv_nxt + self.rcv_wnd as u32) >= 0 || timediff(sn, self.rcv_nxt) < 0 {
      return;
    }
    let mut repeat = false;
    let mut new_index = self.rcv_buf.len();
    for segment in self.rcv_buf.iter().rev() {
      if segment.sn == sn {
        repeat = true;
        break;
      }
      if timediff(sn, segment.sn) > 0 {
        break;
      }
      new_index -= 1;
    }
    if !repeat {
      self.rcv_buf.insert(new_index, new_segment);
    }
    // move available data from rcv_buf -> rcv_queue
    self.move_buf();
  }

  /// Get `conv`
  #[inline]
  pub fn conv(&self) -> u32 { self.conv }

  /// Call this when you received a packet from raw connection
  pub fn input(&mut self, buf: &[u8]) -> Result<usize, KcpError> {
    if buf.len() < KCP_OVERHEAD as usize {
      return Err(KcpError::InvalidSegmentSize(buf.len()));
    }
    let mut flag = false;
    let mut max_ack = 0;
    let old_una = self.snd_una;
    let mut latest_ts = 0;
    let mut buf = Cursor::new(buf);
    while buf.remaining() >= KCP_OVERHEAD as usize {
      let conv = buf.get_u32_le();
      if conv != self.conv {
        return Err(KcpError::ConversationIdMismatch(self.conv, conv));
      }
      let cmd = buf.get_u8();
      let frg = buf.get_u8();
      let wnd = buf.get_u16_le();
      let ts = buf.get_u32_le();
      let sn = buf.get_u32_le();
      let una = buf.get_u32_le();
      let len = buf.get_u32_le() as usize;
      if buf.remaining() < len as usize {
        return Err(KcpError::InvalidSegmentDataSize(len, buf.remaining()));
      }
      match cmd {
        KCP_CMD_PUSH | KCP_CMD_ACK | KCP_CMD_WASK | KCP_CMD_WINS => {}
        _ => {
          return Err(KcpError::UnsupportedCmd(cmd));
        }
      }
      self.rmt_wnd = wnd;
      self.parse_una(una);
      self.shrink_buf();
      let mut has_read_data = false;
      match cmd {
        KCP_CMD_ACK => {
          let rtt = timediff(self.current, ts);
          if rtt >= 0 {
            self.update_ack(rtt as u32);
          }
          self.parse_ack(sn);
          self.shrink_buf();
          if !flag {
            flag = true;
            max_ack = sn;
            latest_ts = ts;
          } else if timediff(sn, max_ack) > 0 {
            if self.fastack {
              max_ack = sn;
              latest_ts = ts;
            } else if timediff(ts, latest_ts) > 0 {
              max_ack = sn;
              latest_ts = ts;
            }
          }
        }
        KCP_CMD_PUSH => {
          if timediff(sn, self.rcv_nxt + self.rcv_wnd as u32) < 0 {
            {
                let this = &mut *self; this.acklist.push_back((sn, ts)); };
            if timediff(sn, self.rcv_nxt) >= 0 {
              let mut sbuf = BytesMut::with_capacity(len as usize);
              unsafe {
                sbuf.set_len(len as usize);
              }
              buf.read_exact(&mut sbuf).unwrap();
              has_read_data = true;
              let mut segment = KcpSegment::new_with_data(sbuf);
              segment.conv = conv;
              segment.cmd = cmd;
              segment.frg = frg;
              segment.wnd = wnd;
              segment.ts = ts;
              segment.sn = sn;
              segment.una = una;
              self.parse_data(segment);
            }
          }
        }
        KCP_CMD_WASK => {
          // ready to send back IKCP_CMD_WINS in ikcp_flush
          // tell remote my window size
          self.probe |= KCP_ASK_TELL;
        }
        KCP_CMD_WINS => (),
        _ => unreachable!(),
      }
      // Force skip unread data
      if !has_read_data {
        let next_pos = buf.position() + len as u64;
        buf.set_position(next_pos);
      }
    }
    if flag {
      self.parse_fastack(max_ack, latest_ts);
    }
    if timediff(self.snd_una, old_una) > 0 && self.cwnd < self.rmt_wnd {
      let mss = self.mss;
      if self.cwnd < self.ssthresh {
        self.cwnd += 1;
        self.incr += mss;
      } else {
        if self.incr < mss {
          self.incr = mss;
        }
        self.incr += (mss * mss) / self.incr + (mss / 16);
        if (self.cwnd as usize + 1) * mss <= self.incr {
          // self.cwnd += 1;
          self.cwnd = ((self.incr + mss - 1) / if mss > 0 { mss } else { 1 }) as u16;
        }
      }
      if self.cwnd > self.rmt_wnd {
        self.cwnd = self.rmt_wnd;
        self.incr = self.rmt_wnd as usize * mss;
      }
    }
    Ok(buf.position() as usize)
  }

  fn wnd_unused(&self) -> u16 {
    if self.rcv_queue.len() < self.rcv_wnd as usize {
      self.rcv_wnd - self.rcv_queue.len() as u16
    } else {
      0
    }
  }

  fn probe_wnd_size(&mut self) {
    // probe window size (if remote window size equals zero)
    if self.rmt_wnd == 0 {
      if self.probe_wait == 0 {
        self.probe_wait = KCP_PROBE_INIT;
        self.ts_probe = self.current + self.probe_wait;
      } else {
        if timediff(self.current, self.ts_probe) >= 0 {
          if self.probe_wait < KCP_PROBE_INIT {
            self.probe_wait = KCP_PROBE_INIT;
          }
          self.probe_wait += self.probe_wait / 2;
          if self.probe_wait > KCP_PROBE_LIMIT {
            self.probe_wait = KCP_PROBE_LIMIT;
          }
          self.ts_probe = self.current + self.probe_wait;
          self.probe |= KCP_ASK_SEND;
        }
      }
    } else {
      self.ts_probe = 0;
      self.probe_wait = 0;
    }
  }

  /// Change MTU size, default is 1400
  ///
  /// MTU = Maximum Transmission Unit
  pub fn set_mtu(&mut self, mtu: usize) -> Result<(), KcpError> {
    if mtu < 50 || mtu < KCP_OVERHEAD {
      return Err(KcpError::InvalidMtu(mtu));
    }
    self.mtu = mtu;
    self.mss = self.mtu - KCP_OVERHEAD;
    let target_size = ((mtu + KCP_OVERHEAD) * 3) as usize;
    if target_size > self.buf.capacity() {
      self.buf.reserve(target_size - self.buf.capacity());
    }
    Ok(())
  }

  /// Get MTU
  #[inline]
  pub fn mtu(&self) -> usize { self.mtu }

  /// Set check interval
  pub fn set_interval(&mut self, interval: u32) {
    self.interval = if interval > 5000 {
      5000
    } else if interval < 10 {
      10
    } else {
      interval
    };
  }

  /// Set nodelay
  ///
  /// fastest config: nodelay(true, 20, 2, true)
  ///
  /// `nodelay`: default is disable (false)
  /// `interval`: internal update timer interval in millisec, default is 100ms
  /// `resend`: 0:disable fast resend(default), 1:enable fast resend
  /// `nc`: `false`: normal congestion control(default), `true`: disable congestion control
  pub fn set_nodelay(&mut self, nodelay: bool, interval: i32, resend: i32, nc: bool) {
    if nodelay {
      self.nodelay = true;
      self.rx_minrto = KCP_RTO_NDL;
    } else {
      self.nodelay = false;
      self.rx_minrto = KCP_RTO_MIN;
    }
    match interval {
      interval if interval < 10 => self.interval = 10,
      interval if interval > 5000 => self.interval = 5000,
      _ => self.interval = interval as u32,
    }
    if resend >= 0 {
      self.fastresend = resend as u32;
    }
    self.nocwnd = nc;
  }

  /// Set `wndsize`
  /// set maximum window size: `sndwnd=32`, `rcvwnd=32` by default
  pub fn set_wndsize(&mut self, sndwnd: u16, rcvwnd: u16) {
    if sndwnd > 0 {
      self.snd_wnd = sndwnd as u16;
    }
    if rcvwnd > 0 {
      self.rcv_wnd = cmp::max(rcvwnd, KCP_WND_RCV) as u16;
    }
  }

  /// `snd_wnd` Send window
  #[inline]
  pub fn snd_wnd(&self) -> u16 { self.snd_wnd }

  /// `rcv_wnd` Receive window
  #[inline]
  pub fn rcv_wnd(&self) -> u16 { self.rcv_wnd }

  /// Get `waitsnd`, how many packet is waiting to be sent
  #[inline]
  pub fn wait_snd(&self) -> usize { self.snd_buf.len() + self.snd_queue.len() }

  /// Get `rmt_wnd`, remote window size
  #[inline]
  pub fn rmt_wnd(&self) -> u16 { self.rmt_wnd }

  /// Set `rx_minrto`
  #[inline]
  pub fn set_rx_minrto(&mut self, rto: u32) { self.rx_minrto = rto; }

  /// Set `fastresend`
  #[inline]
  pub fn set_fast_resend(&mut self, fr: u32) { self.fastresend = fr; }

  /// KCP header size
  #[inline]
  pub fn header_len() -> usize { KCP_OVERHEAD as usize }

  /// Maximum Segment Size
  #[inline]
  pub fn mss(&self) -> usize { self.mss }

  /// Set maximum resend times
  #[inline]
  pub fn set_maximum_resend_times(&mut self, dead_link: u32) { self.dead_link = dead_link; }

  /// Check if KCP connection is dead (resend times excceeded)
  #[inline]
  pub fn is_unavailable(&self) -> bool { !self.available }
}

impl<T: AsyncBufSend> Kcp<T> {
  pub async fn async_update(&mut self, current: u32) -> Result<(), KcpError> {
    self.current = current;
    let mut slap = timediff(self.current, self.ts_flush);
    if slap >= 10000 || slap < -10000 {
      self.ts_flush = self.current;
      slap = 0;
    }
    if slap >= 0 {
      self.ts_flush += self.interval;
      if timediff(self.current, self.ts_flush) >= 0 {
        self.ts_flush = self.current + self.interval;
      }

      // flush data
      let mut segment = KcpSegment {
        conv: self.conv,
        cmd: KCP_CMD_ACK,
        wnd: self.wnd_unused(),
        una: self.rcv_nxt,
        ..Default::default()
      };
      // flush pending ACKs
      for &(sn, ts) in &self.acklist {
        if self.buf.len() + KCP_OVERHEAD as usize > self.mtu as usize {
          self.output.send_buf(&self.buf).await?;
          self.buf.clear();
        }
        segment.sn = sn;
        segment.ts = ts;
        segment.encode(&mut self.buf);
      }
      self.acklist.clear();
  
      // flush pending wnd probe
      self.probe_wnd_size();
      if (self.probe & KCP_ASK_SEND) != 0 {
        segment.cmd = KCP_CMD_WASK;
        if self.buf.len() + KCP_OVERHEAD > self.mtu {
          self.output.send_buf(&self.buf).await?;
          self.buf.clear();
        }
        segment.encode(&mut self.buf);
      }
      if (self.probe & KCP_ASK_TELL) != 0 {
        segment.cmd = KCP_CMD_WINS;
        if self.buf.len() + KCP_OVERHEAD > self.mtu {
          self.output.send_buf(&self.buf).await?;
          self.buf.clear();
        }
        segment.encode(&mut self.buf);
      }
      self.probe = 0;
  
      // calculate window size
      let mut cwnd = cmp::min(self.snd_wnd, self.rmt_wnd);
      if !self.nocwnd {
        cwnd = cmp::min(self.cwnd, cwnd);
      }
      // move data from snd_queue to snd_buf
      while timediff(self.snd_nxt, self.snd_una + cwnd as u32) < 0 {
        match self.snd_queue.pop_front() {
          Some(mut new_segment) => {
            new_segment.conv = self.conv;
            new_segment.cmd = KCP_CMD_PUSH;
            new_segment.wnd = segment.wnd;
            new_segment.ts = self.current;
            new_segment.sn = self.snd_nxt;
            self.snd_nxt += 1;
            new_segment.una = self.rcv_nxt;
            new_segment.resendts = self.current;
            new_segment.rto = self.rx_rto;
            new_segment.fastack = 0;
            new_segment.xmit = 0;
            self.snd_buf.push_back(new_segment);
          }
          None => break,
        }
      }
      // calculate resent
      let resent = if self.fastresend > 0 { self.fastresend } else { u32::max_value() };
      let rtomin = if !self.nodelay { self.rx_rto >> 3 } else { 0 };
      let mut lost = false;
      let mut change = 0;
      for snd_segment in &mut self.snd_buf {
        let mut need_send = false;
        if snd_segment.xmit == 0 {
          need_send = true;
          snd_segment.xmit += 1;
          snd_segment.rto = self.rx_rto;
          snd_segment.resendts = self.current + snd_segment.rto + rtomin;
        } else if timediff(self.current, snd_segment.resendts) >= 0 {
          need_send = true;
          snd_segment.xmit += 1;
          self.xmit += 1;
          if !self.nodelay {
            snd_segment.rto += cmp::max(snd_segment.rto, self.rx_rto);
          } else {
            let step = snd_segment.rto; // (kcp->nodelay < 2) ? ((IINT32)(segment->rto)) : kcp->rx_rto;
            snd_segment.rto += step / 2;
          }
          snd_segment.resendts = self.current + snd_segment.rto;
          lost = true;
        } else if snd_segment.fastack >= resent {
          if snd_segment.xmit <= self.fastlimit || self.fastlimit <= 0 {
            need_send = true;
            snd_segment.xmit += 1;
            snd_segment.fastack = 0;
            snd_segment.resendts = self.current + snd_segment.rto;
            change += 1;
          }
        }
        if need_send {
          snd_segment.ts = self.current;
          snd_segment.wnd = segment.wnd;
          snd_segment.una = self.rcv_nxt;
          let need = KCP_OVERHEAD as usize + snd_segment.data.len();
          if self.buf.len() + need > self.mtu as usize {
            self.output.send_buf(&self.buf).await?;
            self.buf.clear();
          }
          snd_segment.encode(&mut self.buf);
          if snd_segment.xmit >= self.dead_link {
            self.available = false;
          }
        }
      }
      // Flush all data in buffer
      if !self.buf.is_empty() {
        self.output.send_buf(&self.buf).await?;
        self.buf.clear();
      }
      // update ssthresh
      if change > 0 {
        let inflight = self.snd_nxt - self.snd_una;
        self.ssthresh = inflight as u16 / 2;
        if self.ssthresh < KCP_THRESH_MIN {
          self.ssthresh = KCP_THRESH_MIN;
        }
        self.cwnd = self.ssthresh + resent as u16;
        self.incr = self.cwnd as usize * self.mss;
      }
      if lost {
        self.ssthresh = cwnd / 2;
        if self.ssthresh < KCP_THRESH_MIN {
          self.ssthresh = KCP_THRESH_MIN;
        }
        self.cwnd = 1;
        self.incr = self.mss;
      }
      if self.cwnd < 1 {
        self.cwnd = 1;
        self.incr = self.mss;
      }  
    }
    Ok(())
  }
}
