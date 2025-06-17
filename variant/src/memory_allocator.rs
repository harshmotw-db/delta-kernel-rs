use std::error::Error;
use std::sync::Arc;

pub trait MemoryAllocator {
    fn get_bytes(size: usize) -> Result<Arc<[u8]>, Box<dyn Error>>;
}
