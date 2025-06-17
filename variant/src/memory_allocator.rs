use std::cell::RefCell;
use std::error::Error;
use std::rc::Rc;

pub trait MemoryAllocator {
    fn get_buffer(&mut self, size: usize) -> Result<Rc<RefCell<Box<[u8]>>>, Box<dyn Error>>;
}

pub struct SampleMemoryAllocator {
    pub buffer: Rc<RefCell<Box<[u8]>>>,
}

impl MemoryAllocator for SampleMemoryAllocator {
    fn get_buffer(&mut self, size: usize) -> Result<Rc<RefCell<Box<[u8]>>>, Box<dyn Error>> {
        let cur_len = self.buffer.borrow().len();
        if size > cur_len {
            // reallocate buffer
            let new_buffer = Rc::new(RefCell::new(vec![0u8; size].into_boxed_slice()));
            self.buffer = new_buffer;
        }
        Ok(self.buffer.clone())
    }
}
