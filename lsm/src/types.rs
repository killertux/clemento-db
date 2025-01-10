use std::{ops::BitAnd, u8};

use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[derive(Debug, Clone)]
pub enum UVarInt {
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    U128(u128),
}

impl UVarInt {
    pub async fn read<R>(reader: &mut R) -> Result<Self, std::io::Error>
    where
        R: AsyncRead + Unpin,
    {
        let mut bytes = BytesMut::new();
        loop {
            let buf = reader.read_u8().await?;
            bytes.put_u8(buf);
            if buf.bitand(0b10000000) == 0 {
                break;
            }
        }
        Ok(match bytes.len() {
            1 => {
                let mut result = 0u8;
                for (i, byte) in bytes.iter().enumerate() {
                    let byte = byte & 0b01111111;
                    result |= (byte as u8) << (7 * i);
                }
                UVarInt::U8(result)
            }
            2 => {
                let mut result = 0u16;
                for (i, byte) in bytes.iter().enumerate() {
                    let byte = byte & 0b01111111;
                    result |= (byte as u16) << (7 * i);
                }
                if result <= u8::MAX as u16 {
                    UVarInt::U8(result as u8)
                } else {
                    UVarInt::U16(result)
                }
            }
            4 => {
                let mut result = 0u32;
                for (i, byte) in bytes.iter().enumerate() {
                    let byte = byte & 0b01111111;
                    result |= (byte as u32) << (7 * i);
                }
                if result <= u16::MAX as u32 {
                    UVarInt::U16(result as u16)
                } else {
                    UVarInt::U32(result)
                }
            }
            8 => {
                let mut result = 0u64;
                for (i, byte) in bytes.iter().enumerate() {
                    let byte = byte & 0b01111111;
                    result |= (byte as u64) << (7 * i);
                }
                if result <= u32::MAX as u64 {
                    UVarInt::U32(result as u32)
                } else {
                    UVarInt::U64(result)
                }
            }
            16 => {
                let mut result = 0u128;
                for (i, byte) in bytes.iter().enumerate() {
                    let byte = byte & 0b01111111;
                    result |= (byte as u128) << (7 * i);
                }
                if result <= u64::MAX as u128 {
                    UVarInt::U64(result as u64)
                } else {
                    UVarInt::U128(result)
                }
            }
            other => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Invalid VarInt length: {}", other),
                ))
            }
        })
    }

    pub async fn write<W>(mut self, writer: &mut W) -> Result<(), std::io::Error>
    where
        W: AsyncWrite + Unpin,
    {
        if self.is_zero() {
            writer.write_all(&[0]).await?;
            return Ok(());
        }
        let mut bytes = BytesMut::new();
        while !self.is_zero() {
            let byte = self.pop_7_bits();
            if bytes.is_empty() {
                bytes.put_u8(byte);
            } else {
                bytes.put_u8(byte | 0b10000000);
            }
        }
        for byte in (*bytes).into_iter().rev() {
            writer.write_u8(*byte).await?;
        }
        Ok(())
    }

    pub fn is_zero(&self) -> bool {
        match self {
            UVarInt::U8(n) => *n == 0,
            UVarInt::U16(n) => *n == 0,
            UVarInt::U32(n) => *n == 0,
            UVarInt::U64(n) => *n == 0,
            UVarInt::U128(n) => *n == 0,
        }
    }

    pub fn is_one(&self) -> bool {
        match self {
            UVarInt::U8(n) => *n == 1,
            UVarInt::U16(n) => *n == 1,
            UVarInt::U32(n) => *n == 1,
            UVarInt::U64(n) => *n == 1,
            UVarInt::U128(n) => *n == 1,
        }
    }

    fn pop_7_bits(&mut self) -> u8 {
        match self {
            UVarInt::U8(n) => {
                let byte = *n & 0b01111111;
                *n >>= 7;
                byte
            }
            UVarInt::U16(n) => {
                let byte = *n & 0b01111111;
                *n >>= 7;
                byte as u8
            }
            UVarInt::U32(n) => {
                let byte = *n & 0b01111111;
                *n >>= 7;
                byte as u8
            }
            UVarInt::U64(n) => {
                let byte = *n & 0b01111111;
                *n >>= 7;
                byte as u8
            }
            UVarInt::U128(n) => {
                let byte = *n & 0b01111111;
                *n >>= 7;
                byte as u8
            }
        }
    }
}

impl TryInto<u8> for UVarInt {
    type Error = String;

    fn try_into(self) -> Result<u8, Self::Error> {
        Ok(match self {
            UVarInt::U8(n) => n,
            other => return Err(format!("Invalid coercion of VarInt {:?} to u8", other)),
        })
    }
}

impl TryInto<u16> for UVarInt {
    type Error = String;

    fn try_into(self) -> Result<u16, Self::Error> {
        Ok(match self {
            UVarInt::U8(n) => n.into(),
            UVarInt::U16(n) => n,
            other => return Err(format!("Invalid coercion of VarInt {:?} to u16", other)),
        })
    }
}

impl TryInto<u32> for UVarInt {
    type Error = String;

    fn try_into(self) -> Result<u32, Self::Error> {
        Ok(match self {
            UVarInt::U8(n) => n.into(),
            UVarInt::U16(n) => n.into(),
            UVarInt::U32(n) => n,
            other => return Err(format!("Invalid coercion of VarInt {:?} to u32", other)),
        })
    }
}

impl TryInto<u64> for UVarInt {
    type Error = String;

    fn try_into(self) -> Result<u64, Self::Error> {
        Ok(match self {
            UVarInt::U8(n) => n.into(),
            UVarInt::U16(n) => n.into(),
            UVarInt::U32(n) => n.into(),
            UVarInt::U64(n) => n,
            other => return Err(format!("Invalid coercion of VarInt {:?} to u64", other)),
        })
    }
}

impl TryInto<u128> for UVarInt {
    type Error = String;

    fn try_into(self) -> Result<u128, Self::Error> {
        Ok(match self {
            UVarInt::U8(n) => n.into(),
            UVarInt::U16(n) => n.into(),
            UVarInt::U32(n) => n.into(),
            UVarInt::U64(n) => n.into(),
            UVarInt::U128(n) => n,
        })
    }
}

impl TryInto<usize> for UVarInt {
    type Error = String;

    fn try_into(self) -> Result<usize, Self::Error> {
        Ok(match self {
            UVarInt::U8(n) => n.into(),
            UVarInt::U16(n) => n.into(),
            UVarInt::U32(n) => n
                .try_into()
                .map_err(|_| "Invalid coercion of u32 to usize")?,
            UVarInt::U64(n) => n
                .try_into()
                .map_err(|_| "Invalid coercion of u64 to usize")?,
            UVarInt::U128(n) => n
                .try_into()
                .map_err(|_| "Invalid coercion of u128 to usize")?,
        })
    }
}

impl TryFrom<usize> for UVarInt {
    type Error = String;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        u8::try_from(value)
            .map(UVarInt::U8)
            .or_else(|_| u16::try_from(value).map(UVarInt::U16))
            .or_else(|_| u32::try_from(value).map(UVarInt::U32))
            .or_else(|_| u64::try_from(value).map(UVarInt::U64))
            .or_else(|_| u128::try_from(value).map(UVarInt::U128))
            .map_err(|_| format!("Invalid coercion of usize {} to VarInt", value))
    }
}
