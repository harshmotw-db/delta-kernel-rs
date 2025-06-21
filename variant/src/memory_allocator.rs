use std::cell::RefCell;
use std::error::Error;
use std::rc::Rc;

pub trait MemoryAllocator {
    /// Returns the slice where value needs to be written to. This method may be called several
    /// times during the construction of a new `value` field in a variant. The implementation must
    /// make sure that on every call, all the data written to the value buffer written so far are
    /// preserved.
    fn borrow_value_buffer(&mut self) -> &mut [u8];

    /// Ensures that the next call to `borrow_value_buffer` returns a slice having at least `size`
    /// bytes. Also ensures that the value bytes written so far are persisted - this means that
    /// if `borrow_value_buffer` is to written a new buffer from the next call onwards, the new
    /// buffer must have the contents of the old value buffer.
    fn ensure_value_buffer_size(&mut self, size: usize) -> Result<(), Box<dyn Error>>;
}

pub struct SampleMemoryAllocator {
    pub value_buffer: Box<[u8]>,
}

impl MemoryAllocator for SampleMemoryAllocator {
    fn borrow_value_buffer(&mut self) -> &mut [u8] {
        return &mut *self.value_buffer;
    }

    fn ensure_value_buffer_size(&mut self, size: usize) -> Result<(), Box<dyn Error>> {
        let cur_len = self.value_buffer.len();
        if size > cur_len {
            // Reallocate larger buffer
            let mut new_buffer = vec![0u8; size].into_boxed_slice();
            new_buffer[..cur_len].copy_from_slice(&self.value_buffer);
            self.value_buffer = new_buffer;
        }
        Ok(())
    }

    // fn get_buffer(&mut self, size: usize) -> Result<&mut [u8], Box<dyn Error>> {
    //     let cur_len = self.buffer.len();
    //     if size > cur_len {
    //         // reallocate buffer
    //         let new_buffer = vec![0u8; size].into_boxed_slice();
    //         self.buffer = new_buffer;
    //     }
    //     Ok(&mut *self.buffer)
    // }
}
