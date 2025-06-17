//! Tools for working with JSON strings and Variants

use serde_json::Value;
use std::collections::HashMap;
use std::env::var;
use std::error::Error;
use std::usize;
use crate::variant_utils;
use crate::memory_allocator::MemoryAllocator;

const DEFAULT_SIZE_LIMIT: u32 = 16 * 1024 * 1024;

struct VariantBuilder<'a, T: MemoryAllocator> {
    value: &'a mut [u8],
    size: usize,
    size_limit: usize,
    dictionary: HashMap<String, u32>,
    memory_allocator: T,
}

struct FieldEntry<'a> {
    key: &'a str,
    id: usize,
    offset: usize
}

impl<'a> VariantBuilder<'a> {
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
            Value::Array(_) => {Ok(())}
            Value::Object(mp) => {
                let mut fields = Vec::<FieldEntry>::new();
                let start = self.size;
                for (k, v) in mp.iter() {
                    let id = self.add_key(k);
                    fields.push(FieldEntry { key: k, id: id, offset: self.size - start });
                    self.build(v)?;
                }
                // FINISH WRITING OBJECT
                Ok(())
            }
        }?;
        Ok(())
    }

    fn check_capacity(&self, additional: usize) -> Result<(), Box<dyn Error>> {
        let required = self.size + additional;
        if required > self.size_limit {
            // TODO: Formalize this error.
            return Err("Variant size limit exceeded.".into());
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
            self.write_primitive_header(variant_utils::LONG_STR);
            self.write_bytes(&(s.len() as u32).to_le_bytes())?;
        } else {
            self.write_short_string_header(bytes.len() as u8);
        }
        self.write_bytes(bytes)?;
        Ok(())
    }

    fn add_key(&mut self, key: &str) -> u32 {
        match self.dictionary.get(key) {
            Some(id) => id.clone(),
            None => {
                let id: u32 = self.dictionary.len() as u32;
                self.dictionary.insert(key.to_string(), id);
                id
            }
        }
    }

    fn finish_writing__object(
        &mut self,
        start: usize, fields: &mut Vec<FieldEntry>
    ) -> Result<(), Box<dyn Error>> {
        let num_fields = fields.len();
        fields.sort_by_key(|f: &FieldEntry<'_>| f.key);
        let mut max_id: usize = 0;
        for field in fields {
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
        self.check_capacity(header_size);
        self.shift_bytes(start, start + data_size, start + header_size)?;
        self.value[start] = self.object_header(large_size, id_size as u8, offset_size as u8);
        let id_start = start + 1 + size_bytes;
        let offset_start = id_start + num_fields * id_size;
        if large_size {
            self.value[start + 1..id_start]
                .copy_from_slice(&(num_fields as u32).to_le_bytes());

        } else {
            self.value[start + 1..id_start]
                .copy_from_slice(&(num_fields as u8).to_le_bytes());
        }
        let mut id_itr = id_start;
        let mut offset_itr = offset_start;
        for field in fields {
            self.value[id_itr..id_itr + id_size].copy_from_slice(src);
        }
        
        Ok(())
    }

    fn write_field_ids<T: Copy>(
        &mut self,
        field_start: usize,
        id_size: usize,
        num_fields: usize,
    ) -> Result<(), Box<dyn Error>> {
        &(num_fields as T).to_le_bytes();
        if id_size == variant_utils::U8_SIZE as usize {
            Ok(())
        } else if id_size == variant_utils::U16_SIZE as usize {
            Ok(())
        } else if id_size == variant_utils::U24_SIZE as usize {
            Ok(())
        } else {
            Err("UNEXPECTED ID SIZE".into())
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
        &mut self,
        large_size: bool,
        id_size: u8,
        offset_size: u8,
    ) -> u8 {
        ((large_size as u8) << (variant_utils::BASIC_TYPE_BYTES + 4))
        | ((id_size - 1) << (variant_utils::BASIC_TYPE_BYTES + 2))
        | ((offset_size - 1) << variant_utils::BASIC_TYPE_BYTES)
        | variant_utils::OBJECT
    }

    fn write_bytes(&mut self, bytes: &[u8]) -> Result<(), Box<dyn Error>> {
        if self.size + bytes.len() > self.value.len() {
            // Formalize this error as a proper way of reporting to the caller to send a larger
            // buffer
            return Err("Buffer size limit exceeded".into());
        }
        self.value[self.size..self.size + bytes.len()].copy_from_slice(bytes);
        self.size += bytes.len();
        Ok(())
    }

    fn shift_bytes(
        &mut self,
        new_start: usize,
        start: usize,
        end: usize,
    ) -> Result<(), Box<dyn Error>> {
        let additional = end - start;
        if self.size + additional > self.value.len() {
            // Formalize this error as a proper way of reporting to the caller to send a larger
            // buffer
            return Err("Buffer size limit exceeded".into());
        }
        self.value.copy_within(start..end, new_start);
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
    value: &mut [u8],
    metadata: &mut [u8],
    json: &str,
) -> Result<(), Box<dyn Error>> {
    let json: Value = serde_json::from_str(json)?;
    let mut vb = VariantBuilder {
        value: value,
        size: 0,
        dictionary: HashMap::new(),
        size_limit: DEFAULT_SIZE_LIMIT,
    };
    vb.build(&json)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::io::Cursor;
    use crate::json::json_to_variant;

    #[test]
    fn test_json_to_variant() -> Result<(), Box<dyn Error>> {
        fn compare_results(json: &str, expected_value: Vec<u8>) -> Result<(), Box<dyn Error>> {
            let json = json;
            let value_buffer: Vec<u8> = Vec::new();
            let metadata_buffer: Vec<u8> = Vec::new();
            let mut value_cursor = Cursor::new(value_buffer);
            let mut metadata_cursor = Cursor::new(metadata_buffer);
            json_to_variant(&mut value_cursor, &mut metadata_cursor, json)?;
            assert_eq!(value_cursor.into_inner(), expected_value);
            Ok(())
        }

        compare_results("null", vec![0u8])?;
        compare_results("true", vec![4u8])?;
        compare_results("false", vec![8u8])?;
        compare_results("  127 ", vec![12u8, 127u8])?;
        compare_results("  -128  ", vec![12u8, 128u8])?;
        compare_results(" 27134  ", vec![16u8, 254u8, 105u8])?;
        compare_results(" -32767431  ", vec![20u8, 57u8, 2u8, 12u8, 254u8])?;
        compare_results("92842754201389",
            vec![24u8, 45u8, 87u8, 98u8, 163u8, 112u8, 84u8, 0u8, 0u8])?;
        Ok(())
    }
}
