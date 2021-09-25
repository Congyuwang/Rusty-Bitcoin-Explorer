use crate::parser::errors::OpResult;
use bitcoin::consensus::Decodable;
use bitcoin::{Block, BlockHeader, Transaction};
use byteorder::{LittleEndian, ReadBytesExt};
use std::fs::File;
use std::io::{BufReader, Cursor};

pub trait BlockchainRead: std::io::Read {
    fn read_varint(&mut self) -> OpResult<usize> {
        let mut n = 0;
        loop {
            let ch_data = self.read_u8()?;
            n = (n << 7) | (ch_data & 0x7F) as usize;
            if ch_data & 0x80 > 0 {
                n += 1;
            } else {
                break;
            }
        }
        Ok(n)
    }

    #[inline]
    fn read_u8(&mut self) -> OpResult<u8> {
        let mut slice = [0u8; 1];
        self.read_exact(&mut slice)?;
        Ok(slice[0])
    }

    #[inline]
    fn read_u256(&mut self) -> OpResult<[u8; 32]> {
        let mut arr = [0u8; 32];
        self.read_exact(&mut arr)?;
        Ok(arr)
    }

    #[inline]
    fn read_u32(&mut self) -> OpResult<u32> {
        let u = ReadBytesExt::read_u32::<LittleEndian>(self)?;
        Ok(u)
    }

    #[inline]
    fn read_i32(&mut self) -> OpResult<i32> {
        let u = ReadBytesExt::read_i32::<LittleEndian>(self)?;
        Ok(u)
    }

    #[inline]
    fn read_u8_vec(&mut self, count: u32) -> OpResult<Vec<u8>> {
        let mut arr = vec![0u8; count as usize];
        self.read_exact(&mut arr)?;
        Ok(arr)
    }

    #[inline]
    fn read_block(&mut self) -> OpResult<Block> {
        Ok(Block::consensus_decode(self)?)
    }

    #[inline]
    fn read_transaction(&mut self) -> OpResult<Transaction> {
        Ok(Transaction::consensus_decode(self)?)
    }

    #[inline]
    fn read_block_header(&mut self) -> OpResult<BlockHeader> {
        Ok(BlockHeader::consensus_decode(self)?)
    }
}

impl BlockchainRead for Cursor<&[u8]> {}
impl BlockchainRead for Cursor<Vec<u8>> {}
impl BlockchainRead for BufReader<File> {}
