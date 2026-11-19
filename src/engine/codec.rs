//! 二进制编解码原语：引擎的状态快照、WAL 记录、线协议共用这一套
//! 手写 codec。不引 serde/bincode——引擎的字节布局是要写进磁盘和
//! 协议的稳定契约，手写保证"版本位 + 字段顺序"显式可控。
//!
//! - 整数一律小端（x86/ARM 原生序，省 byteswap）。
//! - 变长：`bytes`/`name` 用 u32 长度前缀；`array` 用 u32 计数。
//! - 读侧 `Rd` 是零拷贝切片游标，越界即 `Err(Codec)`，不会 panic。

use crate::error::{Error, Result};

pub struct W(pub Vec<u8>);

impl W {
    pub fn new() -> Self {
        W(Vec::with_capacity(64))
    }
    pub fn u8(&mut self, v: u8) {
        self.0.push(v);
    }
    pub fn u16(&mut self, v: u16) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    pub fn u32(&mut self, v: u32) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    pub fn u64(&mut self, v: u64) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    pub fn i64(&mut self, v: i64) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    pub fn bool(&mut self, v: bool) {
        self.u8(v as u8);
    }
    pub fn bytes(&mut self, v: &[u8]) {
        self.u32(v.len() as u32);
        self.0.extend_from_slice(v);
    }
    pub fn opt_u64(&mut self, v: Option<u64>) {
        match v {
            Some(x) => {
                self.u8(1);
                self.u64(x);
            }
            None => self.u8(0),
        }
    }
    pub fn opt_i64(&mut self, v: Option<i64>) {
        match v {
            Some(x) => {
                self.u8(1);
                self.i64(x);
            }
            None => self.u8(0),
        }
    }
    pub fn opt_bytes(&mut self, v: Option<&[u8]>) {
        match v {
            Some(x) => {
                self.u8(1);
                self.bytes(x);
            }
            None => self.u8(0),
        }
    }
    pub fn finish(self) -> Vec<u8> {
        self.0
    }
}

pub struct Rd<'a> {
    pub buf: &'a [u8],
    pub p: usize,
}

impl<'a> Rd<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Rd { buf, p: 0 }
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.p + n > self.buf.len() {
            return Err(Error::Codec(format!(
                "short read: need {n} at {}, have {}",
                self.p,
                self.buf.len() - self.p
            )));
        }
        let s = &self.buf[self.p..self.p + n];
        self.p += n;
        Ok(s)
    }
    pub fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    pub fn u16(&mut self) -> Result<u16> {
        let s = self.take(2)?;
        Ok(u16::from_le_bytes([s[0], s[1]]))
    }
    pub fn u32(&mut self) -> Result<u32> {
        let s = self.take(4)?;
        Ok(u32::from_le_bytes([s[0], s[1], s[2], s[3]]))
    }
    pub fn u64(&mut self) -> Result<u64> {
        let s = self.take(8)?;
        Ok(u64::from_le_bytes([
            s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7],
        ]))
    }
    pub fn i64(&mut self) -> Result<i64> {
        let s = self.take(8)?;
        Ok(i64::from_le_bytes([
            s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7],
        ]))
    }
    pub fn bool(&mut self) -> Result<bool> {
        Ok(self.u8()? != 0)
    }
    pub fn bytes(&mut self) -> Result<Vec<u8>> {
        let n = self.u32()? as usize;
        Ok(self.take(n)?.to_vec())
    }
    /// 长度上限：防对端恶意/损坏声明把内存一次吃穿。
    pub fn bytes_bounded(&mut self, cap: u32) -> Result<Vec<u8>> {
        let n = self.u32()?;
        if n > cap {
            return Err(Error::Codec(format!("declared len {n} over cap {cap}")));
        }
        Ok(self.take(n as usize)?.to_vec())
    }
    pub fn opt_u64(&mut self) -> Result<Option<u64>> {
        if self.u8()? == 1 {
            Ok(Some(self.u64()?))
        } else {
            Ok(None)
        }
    }
    pub fn opt_i64(&mut self) -> Result<Option<i64>> {
        if self.u8()? == 1 {
            Ok(Some(self.i64()?))
        } else {
            Ok(None)
        }
    }
    pub fn opt_bytes(&mut self) -> Result<Option<Vec<u8>>> {
        if self.u8()? == 1 {
            Ok(Some(self.bytes()?))
        } else {
            Ok(None)
        }
    }
    pub fn remaining(&self) -> usize {
        self.buf.len() - self.p
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_all_primitives() {
        let mut w = W::new();
        w.u8(7);
        w.u16(5000);
        w.u32(70000);
        w.u64(u64::MAX);
        w.i64(i64::MIN);
        w.bool(true);
        w.bytes(b"hello");
        w.opt_u64(Some(9));
        w.opt_u64(None);
        w.opt_i64(Some(-3));
        w.opt_bytes(Some(b"x"));
        w.opt_bytes(None);
        let b = w.finish();
        let mut r = Rd::new(&b);
        assert_eq!(r.u8().unwrap(), 7);
        assert_eq!(r.u16().unwrap(), 5000);
        assert_eq!(r.u32().unwrap(), 70000);
        assert_eq!(r.u64().unwrap(), u64::MAX);
        assert_eq!(r.i64().unwrap(), i64::MIN);
        assert!(r.bool().unwrap());
        assert_eq!(r.bytes().unwrap(), b"hello");
        assert_eq!(r.opt_u64().unwrap(), Some(9));
        assert_eq!(r.opt_u64().unwrap(), None);
        assert_eq!(r.opt_i64().unwrap(), Some(-3));
        assert_eq!(r.opt_bytes().unwrap().as_deref(), Some(&b"x"[..]));
        assert_eq!(r.opt_bytes().unwrap(), None);
        assert_eq!(r.remaining(), 0);
    }

    #[test]
    fn short_read_is_err_not_panic() {
        let mut r = Rd::new(&[0u8, 1, 2]);
        assert!(r.u64().is_err());
        // 声明超界的长度被 cap 拦住。
        let mut w = W::new();
        w.u32(1_000_000);
        let b = w.finish();
        let mut r = Rd::new(&b);
        assert!(r.bytes_bounded(1024).is_err());
    }
}
