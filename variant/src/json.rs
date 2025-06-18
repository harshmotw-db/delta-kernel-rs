//! Tools for working with JSON strings and Variants

use serde_json::Value;
use std::cell::RefCell;
use std::collections::HashMap;
use std::error::Error;
use std::rc::Rc;
use std::usize;
use crate::{memory_allocator, variant_utils};
use crate::memory_allocator::{MemoryAllocator, SampleMemoryAllocator};

const DEFAULT_SIZE_LIMIT: usize = 16 * 1024 * 1024;

struct VariantBuilder<T: MemoryAllocator> {
    value: Rc<RefCell<Box<[u8]>>>,
    size: usize,
    size_limit: usize,
    dictionary: HashMap<String, usize>,
    memory_allocator: T,
}

struct FieldEntry<'a> {
    key: &'a str,
    id: usize,
    offset: usize
}

impl<T: MemoryAllocator> VariantBuilder<T> {
    fn build(&mut self, json: &Value) -> Result<(), Box<dyn Error>> {
        match json {
            Value::Null => self.append_null(),
            Value::Bool(b) => self.append_boolean(*b),
            Value::Number(n) => {
                // With the arbitrary_precision feature, numbers are internally stored as strings
                if n.is_i64() {
                    self.append_int(n.as_i64().unwrap())?;
                } else {
                    return Err("Only integral numbers are supported for now.".into());
                    // let n_str = n.as_str();
                    // // Check if decimal

                    // // Assume float

                }
                Ok(())
            }
            Value::String(s) => {
                self.append_string(s)?;
                Ok(())
            }
            Value::Array(arr) => {
                let start = self.size;
                let mut offsets = Vec::<usize>::new();
                for v in arr {
                    offsets.push(self.size - start);
                    self.build(v)?;
                }
                self.finish_writing_array(start, &mut offsets)?;
                Ok(())
            }
            Value::Object(mp) => {
                let mut fields = Vec::<FieldEntry>::new();
                let start = self.size;
                for (k, v) in mp.iter() {
                    let id = self.add_key(k);
                    fields.push(FieldEntry { key: k, id: id, offset: self.size - start });
                    self.build(v)?;
                }
                self.finish_writing_object(start, &mut fields)?;
                Ok(())
            }
        }?;
        Ok(())
    }

    fn check_capacity(&mut self, additional: usize) -> Result<(), Box<dyn Error>> {
        let required = self.size + additional;
        if required > self.size_limit {
            // TODO: Formalize this error.
            return Err("Variant size limit exceeded.".into());
        }
        let cur_len = self.value.borrow().len();
        if required > cur_len {
            // Need to get new buffer
            let new_size = required.next_power_of_two();
            let old_value = self.value.clone();
            self.value = self.memory_allocator.get_buffer(new_size)?;
            self.value.borrow_mut()[0..self.size].copy_from_slice(&old_value.borrow()[0..self.size]);
        }
        Ok(())
    }

    fn append_null(&mut self) -> Result<(), Box<dyn Error>> {
        self.check_capacity(1)?;
        self.write_primitive_header(variant_utils::NULL)?;
        Ok(())
    }

    fn append_boolean(&mut self, b: bool) -> Result<(), Box<dyn Error>> {
        self.check_capacity(1)?;
        self.write_primitive_header(
            if b { variant_utils::TRUE } else { variant_utils::FALSE }
        )?;
        Ok(())
    }

    fn append_int(&mut self, i: i64) -> Result<(), Box<dyn Error>> {
        self.check_capacity(1 + variant_utils::U64_SIZE as usize)?;
        if i as i8 as i64 == i {
            self.write_primitive_header(variant_utils::INT1)?;
            self.write_bytes(&(i as i8).to_le_bytes())?;
        } else if i as i16 as i64 == i {
            self.write_primitive_header(variant_utils::INT2)?;
            self.write_bytes(&(i as i16).to_le_bytes())?;
        } else if i as i32 as i64 == i {
            self.write_primitive_header(variant_utils::INT4)?;
            self.write_bytes(&(i as i32).to_le_bytes())?;
        } else {
            self.write_primitive_header(variant_utils::INT8)?;
            self.write_bytes(&(i as i64).to_le_bytes())?;
        }
        Ok(())
    }

    fn append_string(&mut self, s: &String) -> Result<(), Box<dyn Error>> {
        let bytes = s.as_bytes();
        let long_str = bytes.len() > variant_utils::MAX_SHORT_STR_SIZE.into();
        let additional = if long_str { 1 + variant_utils::U32_SIZE as usize } else { 1 };
        self.check_capacity(additional + bytes.len())?;
        if long_str {
            self.write_primitive_header(variant_utils::LONG_STR)?;
            self.write_bytes(&(s.len() as u32).to_le_bytes())?;
        } else {
            self.write_short_string_header(bytes.len() as u8)?;
        }
        self.write_bytes(bytes)?;
        Ok(())
    }

    fn finish_writing_array(
        &mut self,
        start: usize,
        offsets: &mut Vec<usize>
    ) -> Result<(), Box<dyn Error>> {
        let data_size = self.size - start;
        let num_offsets = offsets.len();
        let large_size = num_offsets > variant_utils::U8_MAX as usize;
        let size_bytes = if large_size {
            variant_utils::U32_SIZE as usize
        } else {
            variant_utils::U8_SIZE as usize
        };
        let offset_size = self.get_integer_size(data_size);
        let header_size = 1 + size_bytes + (num_offsets + 1) * offset_size;
        self.check_capacity(header_size)?;
        self.shift_bytes(start + header_size, start, start + data_size)?;
        let offset_start = start + 1 + size_bytes;
        let mut borrowed_value = self.value.borrow_mut();
        borrowed_value[start] = self.array_header(large_size, offset_size as u8);
        borrowed_value[start + 1..offset_start].copy_from_slice(&num_offsets.to_le_bytes()[..size_bytes]);
        let mut offset_itr = offset_start;
        for offset in offsets {
            borrowed_value[offset_itr..offset_itr+offset_size]
                .copy_from_slice(&offset.to_le_bytes()[..offset_size]);
            offset_itr += offset_size;
        }
        borrowed_value[offset_itr..offset_itr+offset_size]
            .copy_from_slice(&data_size.to_le_bytes()[..offset_size]);
        Ok(())
    }

    fn add_key(&mut self, key: &str) -> usize {
        match self.dictionary.get(key) {
            Some(id) => id.clone(),
            None => {
                let id = self.dictionary.len();
                self.dictionary.insert(key.to_string(), id);
                id
            }
        }
    }

    fn finish_writing_object(
        &mut self,
        start: usize, fields: &mut Vec<FieldEntry>
    ) -> Result<(), Box<dyn Error>> {
        let num_fields = fields.len();
        fields.sort_by_key(|f: &FieldEntry<'_>| f.key);
        let mut max_id: usize = 0;
        for field in &*fields {
            if field.id > max_id {
                max_id = field.id;
            }
        }
        let data_size = self.size - start;
        let large_size = num_fields > variant_utils::U8_MAX as usize;
        let size_bytes: usize = if large_size {
            variant_utils::U32_SIZE as usize
        } else {
            variant_utils::U8_SIZE as usize
        };
        let id_size = self.get_integer_size(max_id);
        let offset_size = self.get_integer_size(data_size);
        let header_size = 1 + size_bytes + num_fields * id_size + (num_fields + 1) * offset_size;
        self.check_capacity(header_size)?;
        self.shift_bytes(start + header_size, start, start + data_size)?;
        let mut borrowed_value = self.value.borrow_mut();
        borrowed_value[start] = self.object_header(large_size, id_size as u8, offset_size as u8);
        let id_start = start + 1 + size_bytes;
        let offset_start = id_start + num_fields * id_size;
        if large_size {
            borrowed_value[start + 1..id_start]
                .copy_from_slice(&(num_fields as u32).to_le_bytes());
        } else {
            borrowed_value[start + 1..id_start]
                .copy_from_slice(&(num_fields as u8).to_le_bytes());
        }
        std::mem::drop(borrowed_value);
        self.write_field_ids_and_offsets(id_start, id_size, offset_start, offset_size,
            fields.as_slice());
        Ok(())
    }

    fn write_field_ids_and_offsets(
        &mut self,
        id_start: usize,
        id_size: usize,
        offset_start: usize,
        offset_size: usize,
        fields: &[FieldEntry],
    ) {
        let mut id_itr = id_start;
        let mut offset_itr = offset_start;
        for field in fields {
            let mut borrowed_value = self.value.borrow_mut();
            borrowed_value[id_itr..id_itr + id_size]
                .copy_from_slice(&(field.id).to_le_bytes()[..id_size]);
            borrowed_value[offset_itr..offset_itr + id_size]
                .copy_from_slice(&(field.offset).to_le_bytes()[..offset_size]);
            id_itr += id_size;
            offset_itr += offset_size;
        }
    }

    fn write_primitive_header(&mut self, typ: u8) -> Result<(), Box<dyn Error>> {
        self.write_bytes(&[(typ << 2) | variant_utils::PRIMITIVE])?;
        Ok(())
    }

    fn write_short_string_header(&mut self, size: u8) -> Result<(), Box<dyn Error>> {
        self.write_bytes(&[(size << 2) | variant_utils::SHORT_STR])?;
        Ok(())
    }

    fn object_header(
        &self,
        large_size: bool,
        id_size: u8,
        offset_size: u8,
    ) -> u8 {
        ((large_size as u8) << (variant_utils::BASIC_TYPE_BITS + 4))
        | ((id_size - 1) << (variant_utils::BASIC_TYPE_BITS + 2))
        | ((offset_size - 1) << variant_utils::BASIC_TYPE_BITS)
        | variant_utils::OBJECT
    }

    fn array_header(
        &self,
        large_size: bool,
        offset_size: u8,
    ) -> u8 {
        ((large_size as u8) << (variant_utils::BASIC_TYPE_BITS + 2))
        | ((offset_size - 1) << variant_utils::BASIC_TYPE_BITS)
        | variant_utils::ARRAY
    }

    fn write_bytes(&mut self, bytes: &[u8]) -> Result<(), Box<dyn Error>> {
        let mut borrowed_value = self.value.borrow_mut();
        if self.size + bytes.len() > borrowed_value.len() {
            // Formalize this error
            return Err("Buffer size insufficient. There might be a bug in the memory allocator.".into());
        }
        borrowed_value[self.size..self.size + bytes.len()].copy_from_slice(bytes);
        self.size += bytes.len();
        Ok(())
    }

    fn shift_bytes(
        &mut self,
        new_start: usize,
        start: usize,
        end: usize,
    ) -> Result<(), Box<dyn Error>> {
        let additional = new_start - start;
        let mut borrowed_value = self.value.borrow_mut();
        if self.size + additional > borrowed_value.len() {
            return Err("Buffer size limit exceeded".into());
        }
        borrowed_value.copy_within(start..end, new_start);
        self.size += additional;
        Ok(())
    }

    fn get_integer_size(&self, value: usize) -> usize {
        if value <= variant_utils::U8_MAX as usize {
            return variant_utils::U8_SIZE as usize;
        }
        if value <= variant_utils::U16_MAX as usize {
            return variant_utils::U16_SIZE as usize;
        }
        return variant_utils::U24_SIZE as usize;
    }

    fn parse_decimal(d: &str, unscaled: &mut i128, scale: &mut u8) -> Result<bool, Box<dyn Error>> {
        let mut chars = d.chars();
        if let Some(first) = chars.next() {
            let multiplier = if first == '-' { -1 } else { 1 };
            for c in chars {
                if c >= '0' && c <= '9' {
                    *unscaled = (*unscaled * 10) + (c as u8 - b'0') as i128;
                }
            }
        } else {
            return Ok(false);
        }
        Ok(true)
    }
}

/// Constructs a variant representation from a json string `json` (assumed to be valid utf-8) and
/// writes the "value" and "metadata" fields of the variant into `value` and `metadata` buffers
/// respectively.
pub fn json_to_variant(
    value: &mut Rc<RefCell<Box<[u8]>>>,
    metadata: &mut [u8],
    value_size: &mut usize,
    json: &str,
) -> Result<(), Box<dyn Error>> {
    let json: Value = serde_json::from_str(json)?;
    let memory_allocator = SampleMemoryAllocator {
        buffer: value.clone()
    };

    let mut vb = VariantBuilder {
        value: value.clone(),
        size: 0,
        dictionary: HashMap::new(),
        size_limit: DEFAULT_SIZE_LIMIT,
        memory_allocator: memory_allocator,
    };
    vb.build(&json)?;
    *value_size = vb.size;
    *value = vb.memory_allocator.buffer;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::cell::RefCell;
    use std::rc::Rc;
    use crate::json::json_to_variant;

    #[test]
    fn test_json_to_variant() -> Result<(), Box<dyn Error>> {
        fn compare_results(json: &str, expected_value: &[u8]) -> Result<(), Box<dyn Error>> {
            let json = json;
            let mut value_buffer: Rc<RefCell<Box<[u8]>>> = Rc::new(RefCell::new(Box::new([0u8; 1])));
            let mut value_size: usize = 0;
            json_to_variant(&mut value_buffer, &mut [1, 2, 3, 4, 5], &mut value_size, json)?;
            let computed_slize: &[u8] = &*value_buffer.borrow();
            assert_eq!(&computed_slize[..value_size], expected_value);
            Ok(())
        }

        // Null
        compare_results("null", &[0u8])?;
        // Bool
        compare_results("true", &[4u8])?;
        compare_results("false", &[8u8])?;
        // Integers
        compare_results("  127 ", &[12u8, 127u8])?;
        compare_results("  -128  ", &[12u8, 128u8])?;
        compare_results(" 27134  ", &[16u8, 254u8, 105u8])?;
        compare_results(" -32767431  ", &[20u8, 57u8, 2u8, 12u8, 254u8])?;
        compare_results("92842754201389",
            &[24u8, 45u8, 87u8, 98u8, 163u8, 112u8, 84u8, 0u8, 0u8])?;
        // Decimals (including large integers)

        // Floating point numbers

        // short strings
        // random short string
        compare_results(
            "\"harsh\"", 
            &[21u8, 104u8, 97u8, 114u8, 115u8, 104u8])?;
        // longest short string
        let mut expected = [97u8; 64];
        expected[0] = 253u8;
        compare_results(
            &format!("\"{}\"", std::iter::repeat('a').take(63).collect::<String>()),
            &expected
        )?;
        // long strings
        let mut expected = [97u8; 69];
        expected[..5].copy_from_slice(&[64u8, 64u8, 0, 0, 0]);
        compare_results(
            &format!("\"{}\"", std::iter::repeat('a').take(64).collect::<String>()),
            &expected
        )?;
        let mut expected = [98u8; 100005];
        expected[0] = 64u8;
        expected[1..5].copy_from_slice(&(100000 as u32).to_le_bytes());
        compare_results(
            &format!("\"{}\"", std::iter::repeat('b').take(100000).collect::<String>()),
            &expected
        )?;

        // arrays
        compare_results(
            "[127, 128, -32767431]",
            &[3u8, 3u8, 0u8, 2u8, 5u8, 10u8, 12u8, 127u8, 16u8, 128u8, 0u8, 20u8, 57u8, 2u8, 12u8, 254u8]
        )?;

        compare_results(
            "[[\"a\", null, true, 4], 128, false]",
            &[3u8, 3u8, 0u8, 13u8, 16u8, 17u8, 3u8, 4u8, 0u8, 2u8, 3u8, 4u8, 6u8, 5u8, 97u8, 0u8, 4u8, 12u8, 4u8, 16u8, 128u8, 0u8, 8u8]
        )?;

        // objects

        Ok(())
    }
}
