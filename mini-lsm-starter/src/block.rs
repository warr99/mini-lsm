#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;
pub(crate) const SIZEOF_U16: usize = std::mem::size_of::<u16>();
pub(crate) const SIZEOF_U32: usize = std::mem::size_of::<u32>();


/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
// ----------------------------------------------------------------------------------------------------
// |             Data Section             |              Offset Section             |      Extra      |
// ----------------------------------------------------------------------------------------------------
// | Entry #1 | Entry #2 | ... | Entry #N | Offset #1 | Offset #2 | ... | Offset #N | num_of_elements |
// ----------------------------------------------------------------------------------------------------
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut buf = self.data.clone();
        let num_of_elements = self.offsets.len();
        self.offsets.iter().for_each(|&offset| buf.put_u16(offset));
        buf.put_u16(num_of_elements as u16);
        buf.into()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        assert!(
            data.len() >= 2,
            "data is too short to contain elements count"
        );
        let num_of_elements_pos = data.len() - 2;
        let num_of_elements = (&data[num_of_elements_pos..]).get_u16() as usize;
        assert!(
            data.len() >= 2 * num_of_elements + 2,
            "data is too short to contain required number of offsets"
        );
        let data_end = num_of_elements_pos - SIZEOF_U16 * num_of_elements;
        let offsets_raw = &data[data_end..num_of_elements_pos];

        let offsets = offsets_raw
            .chunks(SIZEOF_U16)
            .map(|mut x| x.get_u16())
            .collect();
        let data = data[0..data_end].to_vec();

        Self { data, offsets }
    }
}
