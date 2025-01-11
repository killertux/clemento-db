use std::{ops::BitAnd, u8};

use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[derive(Debug, Clone, PartialEq, Eq)]
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
        let mut result = 0u128;
        for (i, byte) in bytes.iter().rev().enumerate() {
            let byte = byte & 0b01111111;
            result |= (byte as u128) << (7 * i);
        }
        return Ok(if result <= u8::MAX as u128 {
            UVarInt::U8(result as u8)
        } else if result <= u16::MAX as u128 {
            UVarInt::U16(result as u16)
        } else if result <= u32::MAX as u128 {
            UVarInt::U32(result as u32)
        } else if result <= u64::MAX as u128 {
            UVarInt::U64(result as u64)
        } else {
            UVarInt::U128(result)
        });
    }

    pub async fn write<W>(mut self, writer: &mut W) -> Result<usize, std::io::Error>
    where
        W: AsyncWrite + Unpin,
    {
        if self.is_zero() {
            writer.write_all(&[0]).await?;
            return Ok(1);
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
        let bytes_written = bytes.len();
        for byte in (*bytes).into_iter().rev() {
            writer.write_u8(*byte).await?;
        }
        Ok(bytes_written)
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fixed28BitsInt(u32);

impl Fixed28BitsInt {
    pub const MAX: u32 = 0xFFFFFFF;

    pub fn new(value: u32) -> Self {
        if value > Self::MAX {
            panic!("Value {} is too big for a 21 bits integer", value);
        }
        Self(value)
    }

    pub fn value(&self) -> u32 {
        self.0
    }

    pub async fn write<W>(self, writer: &mut W) -> Result<(), std::io::Error>
    where
        W: AsyncWrite + Unpin,
    {
        if self.0 == 0 {
            writer.write_all(&[0b10000000]).await?;
            return Ok(());
        }
        writer.write_u8((self.0 >> 21) as u8 | 0b10000000).await?;
        writer.write_u8((self.0 >> 14) as u8 | 0b10000000).await?;
        writer.write_u8((self.0 >> 7) as u8 | 0b10000000).await?;
        writer.write_u8(self.0 as u8 | 0b10000000).await?;
        Ok(())
    }

    pub async fn read<R>(reader: &mut R) -> Result<Self, std::io::Error>
    where
        R: AsyncRead + Unpin,
    {
        let mut value = 0u32;
        value |= ((reader.read_u8().await? & 0b01111111) as u32) << 21;
        value |= ((reader.read_u8().await? & 0b01111111) as u32) << 14;
        value |= ((reader.read_u8().await? & 0b01111111) as u32) << 7;
        value |= (reader.read_u8().await? & 0b01111111) as u32;
        Ok(Self(value))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fixed56BitsInt(u64);

impl Fixed56BitsInt {
    pub const MAX: u64 = 0x00FFFFFFFFFFFFFF;

    pub fn new(value: u64) -> Self {
        if value > Self::MAX {
            panic!("Value {} is too big for a 56 bits integer", value);
        }
        Self(value)
    }

    pub fn value(&self) -> u64 {
        self.0
    }

    pub async fn write<W>(self, writer: &mut W) -> Result<(), std::io::Error>
    where
        W: AsyncWrite + Unpin,
    {
        if self.0 == 0 {
            writer.write_u64(0x8080808080808080).await?;
            return Ok(());
        }
        writer.write_u8((self.0 >> 49) as u8 | 0b10000000).await?;
        writer.write_u8((self.0 >> 42) as u8 | 0b10000000).await?;
        writer.write_u8((self.0 >> 35) as u8 | 0b10000000).await?;
        writer.write_u8((self.0 >> 28) as u8 | 0b10000000).await?;
        writer.write_u8((self.0 >> 21) as u8 | 0b10000000).await?;
        writer.write_u8((self.0 >> 14) as u8 | 0b10000000).await?;
        writer.write_u8((self.0 >> 7) as u8 | 0b10000000).await?;
        writer.write_u8(self.0 as u8 | 0b10000000).await?;
        Ok(())
    }

    pub async fn read<R>(reader: &mut R) -> Result<Self, std::io::Error>
    where
        R: AsyncRead + Unpin,
    {
        let mut value = 0u64;
        value |= ((reader.read_u8().await? & 0b01111111) as u64) << 49;
        value |= ((reader.read_u8().await? & 0b01111111) as u64) << 42;
        value |= ((reader.read_u8().await? & 0b01111111) as u64) << 35;
        value |= ((reader.read_u8().await? & 0b01111111) as u64) << 28;
        value |= ((reader.read_u8().await? & 0b01111111) as u64) << 21;
        value |= ((reader.read_u8().await? & 0b01111111) as u64) << 14;
        value |= ((reader.read_u8().await? & 0b01111111) as u64) << 7;
        value |= (reader.read_u8().await? & 0b01111111) as u64;
        Ok(Self(value))
    }
}

#[cfg(test)]
mod test {
    use std::u128;

    use super::*;

    #[tokio::test]
    async fn write_and_read_one() {
        let mut buffer = Vec::new();
        let uvarint = UVarInt::U8(1);
        let bytes_written = uvarint.write(&mut buffer).await.unwrap();
        assert_eq!(bytes_written, 1);
        assert_eq!(&[1], buffer.as_slice());
        assert_eq!(
            UVarInt::read(&mut &buffer[..]).await.unwrap(),
            UVarInt::U8(1)
        );
    }

    #[tokio::test]
    async fn write_and_read_uvarint_u8() {
        let mut buffer_1 = Vec::new();
        let mut buffer_2 = Vec::new();
        let uvarint_1 = UVarInt::U8(32);
        let uvarint_2 = UVarInt::U8(255);
        let bytes_written = uvarint_1.write(&mut buffer_1).await.unwrap();
        assert_eq!(bytes_written, 1);
        let bytes_written = uvarint_2.write(&mut buffer_2).await.unwrap();
        assert_eq!(bytes_written, 2);
        assert_eq!(
            UVarInt::read(&mut &buffer_1[..]).await.unwrap(),
            UVarInt::U8(32)
        );
        assert_eq!(
            UVarInt::read(&mut &buffer_2[..]).await.unwrap(),
            UVarInt::U8(255)
        );
    }

    #[tokio::test]
    async fn write_and_read_uvarint() {
        let mut buffer = Vec::new();
        let uvarint = UVarInt::U128(u128::MAX);
        let bytes_written = uvarint.write(&mut buffer).await.unwrap();
        assert_eq!(bytes_written, 19);
        let mut buffer = &buffer[..];
        let uvarint = UVarInt::read(&mut buffer).await.unwrap();
        assert_eq!(uvarint, UVarInt::U128(u128::MAX));
    }

    #[tokio::test]
    async fn write_and_read_fixed_21_bits_int() {
        let mut buffer = Vec::new();
        let fixed_int = Fixed28BitsInt::new(Fixed28BitsInt::MAX);
        fixed_int.write(&mut buffer).await.unwrap();
        let mut buffer = &buffer[..];
        let fixed_int = Fixed28BitsInt::read(&mut buffer).await.unwrap();
        assert_eq!(fixed_int.value(), Fixed28BitsInt::MAX);
    }

    #[tokio::test]
    async fn write_and_read_fixed_56_bits_int() {
        let mut buffer = Vec::new();
        let fixed_int = Fixed56BitsInt::new(Fixed56BitsInt::MAX);
        fixed_int.write(&mut buffer).await.unwrap();
        let mut buffer = &buffer[..];
        let fixed_int = Fixed56BitsInt::read(&mut buffer).await.unwrap();
        assert_eq!(fixed_int.value(), Fixed56BitsInt::MAX);
    }
}
