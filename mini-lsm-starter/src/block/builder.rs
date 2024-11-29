#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use bytes::BufMut;

use crate::key::{KeySlice, KeyVec};

use super::{Block, SIZEOF_U16};

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size: block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let key_len = key.len();
        let value_len = value.len();
        // -----------------------------------------------------------------------
        // |                           Entry #1                            | ... |
        // -----------------------------------------------------------------------
        // | key_len (2B) | key (keylen) | value_len (2B) | value (varlen) | ... |
        // -----------------------------------------------------------------------
        let entry_size = SIZEOF_U16 + key_len + SIZEOF_U16 + value_len;
        if self.estimated_size() + entry_size > self.block_size && !self.is_empty() {
            return false;
        }
        // offset
        self.offsets.push(self.data.len() as u16);
        // entry - key_len
        self.data.put_u16(key_len as u16);
        // entry - key
        self.data.put(key.raw_ref());
        // enrty - value_len
        self.data.put_u16(value_len as u16);
        // entry - value
        self.data.put(value);

        // Set the first key if this is the first entry being added
        if self.first_key.is_empty() {
            self.first_key.set_from_slice(key);
        }

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        if self.is_empty() {
            panic!("block should not be empty");
        }
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }

    fn estimated_size(&self) -> usize {
        self.data.len() + self.offsets.len() * SIZEOF_U16 + SIZEOF_U16
    }
}
