//! Tools for working with JSON strings and Variants

use crate::memory_allocator::MemoryAllocator;
use crate::variant_utils;
use rust_decimal::prelude::*;
use serde_json::Value;
use std::collections::HashMap;
use std::error::Error;

const DEFAULT_SIZE_LIMIT: usize = 16 * 1024 * 1024;

struct VariantBuilder<'a, T: MemoryAllocator> {
    size: usize,
    size_limit: usize,
    dictionary: HashMap<String, usize>,
    memory_allocator: &'a mut T,
}

struct FieldEntry<'a> {
    key: &'a str,
    id: usize,
    offset: usize,
}

impl<'a, T: MemoryAllocator> VariantBuilder<'a, T> {
    fn build(&mut self, json: &Value) -> Result<(), Box<dyn Error>> {
        match json {
            Value::Null => self.append_null(),
            Value::Bool(b) => self.append_boolean(*b),
            Value::Number(n) => {
                // With the arbitrary_precision feature, numbers are internally stored as strings
                if n.is_i64() {
                    self.append_int(n.as_i64().unwrap())?;
                } else {
                    // Check if decimal
                    match Decimal::from_str_exact(n.as_str()) {
                        // TODO: Replace with custom decimal parsing to support decimal unscaled
                        // value greater than 2^96 - 1
                        Ok(dec) => self.append_decimal(dec)?,
                        Err(_) => {
                            // Try float
                            match n.as_f64() {
                                Some(f) => self.append_double(f),
                                None => {
                                    Err(format!("Failed to parse {} as number", n.as_str()).into())
                                }
                            }?
                        }
                    };
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
                    fields.push(FieldEntry {
                        key: k,
                        id,
                        offset: self.size - start,
                    });
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
        let cur_len = self.memory_allocator.borrow_value_buffer().len();
        if required > cur_len {
            // Need to get new buffer
            let new_size = required.next_power_of_two();
            self.memory_allocator.ensure_value_buffer_size(new_size)?;
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
        self.write_primitive_header(if b {
            variant_utils::TRUE
        } else {
            variant_utils::FALSE
        })?;
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
            self.write_bytes(&(i).to_le_bytes())?;
        }
        Ok(())
    }

    fn append_decimal(&mut self, dec: Decimal) -> Result<(), Box<dyn Error>> {
        self.check_capacity(2 + 16)?;
        let unscaled: i128 = dec.mantissa();
        let scale = dec.scale() as u8;
        if unscaled.abs() <= variant_utils::MAX_UNSCALED_DECIMAL_4 as i128
            && scale <= variant_utils::MAX_PRECISION_DECIMAL_4
        {
            self.write_primitive_header(variant_utils::DECIMAL4)?;
            self.write_bytes(&(scale).to_le_bytes())?;
            self.write_bytes(&(unscaled as i32).to_le_bytes())?;
        } else if unscaled.abs() <= variant_utils::MAX_UNSCALED_DECIMAL_8 as i128
            && scale <= variant_utils::MAX_PRECISION_DECIMAL_8
        {
            self.write_primitive_header(variant_utils::DECIMAL8)?;
            self.write_bytes(&(scale).to_le_bytes())?;
            self.write_bytes(&(unscaled as i64).to_le_bytes())?;
        } else {
            self.write_primitive_header(variant_utils::DECIMAL16)?;
            self.write_bytes(&(scale).to_le_bytes())?;
            self.write_bytes(&unscaled.to_le_bytes())?;
        }
        Ok(())
    }

    fn append_double(&mut self, f: f64) -> Result<(), Box<dyn Error>> {
        self.check_capacity(1 + 8)?;
        self.write_primitive_header(variant_utils::DOUBLE)?;
        self.write_bytes(&f.to_le_bytes())?;
        Ok(())
    }

    fn append_string(&mut self, s: &String) -> Result<(), Box<dyn Error>> {
        let bytes = s.as_bytes();
        let long_str = bytes.len() > variant_utils::MAX_SHORT_STR_SIZE.into();
        let additional = if long_str {
            1 + variant_utils::U32_SIZE as usize
        } else {
            1
        };
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
        offsets: &mut Vec<usize>,
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
        let value_buffer = self.memory_allocator.borrow_value_buffer();
        value_buffer[start] = Self::array_header(large_size, offset_size as u8);
        value_buffer[start + 1..offset_start]
            .copy_from_slice(&num_offsets.to_le_bytes()[..size_bytes]);
        let mut offset_itr = offset_start;
        for offset in offsets {
            value_buffer[offset_itr..offset_itr + offset_size]
                .copy_from_slice(&offset.to_le_bytes()[..offset_size]);
            offset_itr += offset_size;
        }
        value_buffer[offset_itr..offset_itr + offset_size]
            .copy_from_slice(&data_size.to_le_bytes()[..offset_size]);
        Ok(())
    }

    fn add_key(&mut self, key: &str) -> usize {
        match self.dictionary.get(key) {
            Some(id) => *id,
            None => {
                let id = self.dictionary.len();
                self.dictionary.insert(key.to_string(), id);
                id
            }
        }
    }

    fn array_header(large_size: bool, offset_size: u8) -> u8 {
        ((large_size as u8) << (variant_utils::BASIC_TYPE_BITS + 2))
            | ((offset_size - 1) << variant_utils::BASIC_TYPE_BITS)
            | variant_utils::ARRAY
    }

    fn finish_writing_object(
        &mut self,
        start: usize,
        fields: &mut Vec<FieldEntry>,
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
        let value_buffer = self.memory_allocator.borrow_value_buffer();
        value_buffer[start] = Self::object_header(large_size, id_size as u8, offset_size as u8);
        let id_start = start + 1 + size_bytes;
        let offset_start = id_start + num_fields * id_size;
        if large_size {
            value_buffer[start + 1..id_start].copy_from_slice(&(num_fields as u32).to_le_bytes());
        } else {
            value_buffer[start + 1..id_start].copy_from_slice(&(num_fields as u8).to_le_bytes());
        }
        self.write_field_ids_and_offsets(
            id_start,
            id_size,
            offset_start,
            offset_size,
            data_size,
            fields.as_slice(),
        );
        Ok(())
    }

    fn object_header(large_size: bool, id_size: u8, offset_size: u8) -> u8 {
        ((large_size as u8) << (variant_utils::BASIC_TYPE_BITS + 4))
            | ((id_size - 1) << (variant_utils::BASIC_TYPE_BITS + 2))
            | ((offset_size - 1) << variant_utils::BASIC_TYPE_BITS)
            | variant_utils::OBJECT
    }

    fn write_field_ids_and_offsets(
        &mut self,
        id_start: usize,
        id_size: usize,
        offset_start: usize,
        offset_size: usize,
        data_size: usize,
        fields: &[FieldEntry],
    ) {
        let mut id_itr = id_start;
        let mut offset_itr = offset_start;
        let value_buffer = self.memory_allocator.borrow_value_buffer();
        for field in fields {
            value_buffer[id_itr..id_itr + id_size]
                .copy_from_slice(&(field.id).to_le_bytes()[..id_size]);
            value_buffer[offset_itr..offset_itr + offset_size]
                .copy_from_slice(&(field.offset).to_le_bytes()[..offset_size]);
            id_itr += id_size;
            offset_itr += offset_size;
        }
        value_buffer[offset_itr..offset_itr + id_size]
            .copy_from_slice(&(data_size).to_le_bytes()[..offset_size]);
    }

    fn write_primitive_header(&mut self, typ: u8) -> Result<(), Box<dyn Error>> {
        self.write_bytes(&[(typ << 2) | variant_utils::PRIMITIVE])?;
        Ok(())
    }

    fn write_short_string_header(&mut self, size: u8) -> Result<(), Box<dyn Error>> {
        self.write_bytes(&[(size << 2) | variant_utils::SHORT_STR])?;
        Ok(())
    }

    fn write_bytes(&mut self, bytes: &[u8]) -> Result<(), Box<dyn Error>> {
        let value_buffer = self.memory_allocator.borrow_value_buffer();
        if self.size + bytes.len() > value_buffer.len() {
            // Formalize this error
            return Err(
                "Buffer size insufficient. There might be a bug in the memory allocator.".into(),
            );
        }
        value_buffer[self.size..self.size + bytes.len()].copy_from_slice(bytes);
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
        let borrowed_value = self.memory_allocator.borrow_value_buffer();
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
        variant_utils::U24_SIZE as usize
    }
}

/// Constructs a variant representation from a json string `json` (assumed to be valid utf-8) and
/// writes the "value" and "metadata" fields of the variant into `value` and `metadata` buffers
/// respectively.
pub fn json_to_variant<T: MemoryAllocator>(
    json: &str,
    memory_allocator: &mut T,
    value_size: &mut usize,
) -> Result<(), Box<dyn Error>> {
    let json: Value = serde_json::from_str(json)?;

    let mut vb = VariantBuilder {
        size: 0,
        dictionary: HashMap::new(),
        size_limit: DEFAULT_SIZE_LIMIT,
        memory_allocator,
    };
    vb.build(&json)?;
    *value_size = vb.size;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::json::json_to_variant;
    use crate::memory_allocator::SampleMemoryAllocator;
    use std::error::Error;

    #[test]
    fn test_json_to_variant() -> Result<(), Box<dyn Error>> {
        fn compare_results(json: &str, expected_value: &[u8]) -> Result<(), Box<dyn Error>> {
            let json = json;
            let mut value_size: usize = 0;

            let mut memory_allocator = SampleMemoryAllocator {
                value_buffer: vec![0u8; 1].into_boxed_slice(),
            };
            json_to_variant(json, &mut memory_allocator, &mut value_size)?;
            let computed_slize: &[u8] = &*memory_allocator.value_buffer;
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
        compare_results(
            "92842754201389",
            &[24u8, 45u8, 87u8, 98u8, 163u8, 112u8, 84u8, 0u8, 0u8],
        )?;
        // Decimals
        // Decimal 4
        compare_results("1.23", &[32u8, 2u8, 123u8, 0u8, 0u8, 0u8])?;
        compare_results("99999999.9", &[32u8, 1u8, 0xffu8, 0xc9u8, 0x9au8, 0x3bu8])?;
        compare_results("-99999999.9", &[32u8, 1u8, 1u8, 0x36u8, 0x65u8, 0xc4u8])?;
        compare_results("0.999999999", &[32u8, 9u8, 0xffu8, 0xc9u8, 0x9au8, 0x3bu8])?;
        compare_results("0.000000001", &[32u8, 9u8, 1u8, 0, 0, 0])?;
        compare_results("-0.999999999", &[32u8, 9u8, 1u8, 0x36u8, 0x65u8, 0xc4u8])?;
        compare_results("-0.000000001", &[32u8, 9u8, 0xffu8, 0xffu8, 0xffu8, 0xffu8])?;
        // Decimal 8
        compare_results(
            "999999999.0",
            &[36u8, 1u8, 0xf6u8, 0xe3u8, 0x0bu8, 0x54u8, 0x02u8, 0, 0, 0],
        )?;
        compare_results(
            "-999999999.0",
            &[
                36u8, 1u8, 0x0au8, 0x1cu8, 0xf4u8, 0xabu8, 0xfdu8, 0xffu8, 0xffu8, 0xffu8,
            ],
        )?;
        compare_results(
            "0.999999999999999999",
            &[
                36u8, 18u8, 0xffu8, 0xffu8, 0x63u8, 0xa7u8, 0xb3u8, 0xb6u8, 0xe0u8, 0x0du8,
            ],
        )?;
        compare_results(
            "-9999999999999999.99",
            &[
                36u8, 2u8, 0x01u8, 0x00u8, 0x9cu8, 0x58u8, 0x4cu8, 0x49u8, 0x1fu8, 0xf2u8,
            ],
        )?;
        // Decimal 16
        compare_results(
            "9999999999999999999", // integer larger than i64
            &[
                40u8, 0u8, 0xffu8, 0xffu8, 0xe7u8, 0x89u8, 4u8, 0x23u8, 0xc7u8, 0x8au8, 0u8, 0u8,
                0u8, 0u8, 0u8, 0u8, 0u8, 0u8,
            ],
        )?;
        compare_results(
            "0.9999999999999999999",
            &[
                40u8, 19u8, 0xffu8, 0xffu8, 0xe7u8, 0x89u8, 4u8, 0x23u8, 0xc7u8, 0x8au8, 0u8, 0u8,
                0u8, 0u8, 0u8, 0u8, 0u8, 0u8,
            ],
        )?;
        compare_results(
            "79228162514264337593543950335", // 2 ^ 96 - 1
            &[
                40u8, 0u8, 0xffu8, 0xffu8, 0xffu8, 0xffu8, 0xffu8, 0xffu8, 0xffu8, 0xffu8, 0xffu8,
                0xffu8, 0xffu8, 0xffu8, 0u8, 0u8, 0u8, 0u8,
            ],
        )?;
        compare_results(
            "7.9228162514264337593543950335", // using scale higher than this falls into double
            // since the max scale is 28.
            &[
                40u8, 28u8, 0xffu8, 0xffu8, 0xffu8, 0xffu8, 0xffu8, 0xffu8, 0xffu8, 0xffu8, 0xffu8,
                0xffu8, 0xffu8, 0xffu8, 0u8, 0u8, 0u8, 0u8,
            ],
        )?;
        // Double
        {
            let mut arr = [28u8; 9];
            arr[1..].copy_from_slice(&0.79228162514264337593543950335f64.to_le_bytes());
            compare_results("0.79228162514264337593543950335", &arr)?;
        }
        compare_results("15e-1", &[28u8, 0, 0, 0, 0, 0, 0, 0xf8, 0x3fu8])?;
        compare_results("-15e-1", &[28u8, 0, 0, 0, 0, 0, 0, 0xf8, 0xBfu8])?;

        // short strings
        // random short string
        compare_results("\"harsh\"", &[21u8, 104u8, 97u8, 114u8, 115u8, 104u8])?;
        // longest short string
        let mut expected = [97u8; 64];
        expected[0] = 253u8;
        compare_results(
            &format!(
                "\"{}\"",
                std::iter::repeat('a').take(63).collect::<String>()
            ),
            &expected,
        )?;
        // long strings
        let mut expected = [97u8; 69];
        expected[..5].copy_from_slice(&[64u8, 64u8, 0, 0, 0]);
        compare_results(
            &format!(
                "\"{}\"",
                std::iter::repeat('a').take(64).collect::<String>()
            ),
            &expected,
        )?;
        let mut expected = [98u8; 100005];
        expected[0] = 64u8;
        expected[1..5].copy_from_slice(&(100000 as u32).to_le_bytes());
        compare_results(
            &format!(
                "\"{}\"",
                std::iter::repeat('b').take(100000).collect::<String>()
            ),
            &expected,
        )?;

        // arrays
        // u8 offset
        compare_results(
            "[127, 128, -32767431]",
            &[
                3u8, 3u8, 0u8, 2u8, 5u8, 10u8, 12u8, 127u8, 16u8, 128u8, 0u8, 20u8, 57u8, 2u8,
                12u8, 254u8,
            ],
        )?;
        compare_results(
            "[[\"a\", null, true, 4], 128, false]",
            &[
                3u8, 3u8, 0u8, 13u8, 16u8, 17u8, 3u8, 4u8, 0u8, 2u8, 3u8, 4u8, 6u8, 5u8, 97u8, 0u8,
                4u8, 12u8, 4u8, 16u8, 128u8, 0u8, 8u8,
            ],
        )?;
        // u16 offset - 128 i8's + 1 "true" = 257 bytes
        compare_results(
            &format!(
                "[{} true]",
                std::iter::repeat("1, ").take(128).collect::<String>()
            ),
            &[
                7u8, 129u8, 0, 0, 2, 0, 4, 0, 6, 0, 8, 0, 10, 0, 12, 0, 14, 0, 16, 0, 18, 0, 20, 0,
                22, 0, 24, 0, 26, 0, 28, 0, 30, 0, 32, 0, 34, 0, 36, 0, 38, 0, 40, 0, 42, 0, 44, 0,
                46, 0, 48, 0, 50, 0, 52, 0, 54, 0, 56, 0, 58, 0, 60, 0, 62, 0, 64, 0, 66, 0, 68, 0,
                70, 0, 72, 0, 74, 0, 76, 0, 78, 0, 80, 0, 82, 0, 84, 0, 86, 0, 88, 0, 90, 0, 92, 0,
                94, 0, 96, 0, 98, 0, 100, 0, 102, 0, 104, 0, 106, 0, 108, 0, 110, 0, 112, 0, 114,
                0, 116, 0, 118, 0, 120, 0, 122, 0, 124, 0, 126, 0, 128, 0, 130, 0, 132, 0, 134, 0,
                136, 0, 138, 0, 140, 0, 142, 0, 144, 0, 146, 0, 148, 0, 150, 0, 152, 0, 154, 0,
                156, 0, 158, 0, 160, 0, 162, 0, 164, 0, 166, 0, 168, 0, 170, 0, 172, 0, 174, 0,
                176, 0, 178, 0, 180, 0, 182, 0, 184, 0, 186, 0, 188, 0, 190, 0, 192, 0, 194, 0,
                196, 0, 198, 0, 200, 0, 202, 0, 204, 0, 206, 0, 208, 0, 210, 0, 212, 0, 214, 0,
                216, 0, 218, 0, 220, 0, 222, 0, 224, 0, 226, 0, 228, 0, 230, 0, 232, 0, 234, 0,
                236, 0, 238, 0, 240, 0, 242, 0, 244, 0, 246, 0, 248, 0, 250, 0, 252, 0, 254, 0, 0,
                1, 1, 1, // Final offset is 257
                12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1,
                12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1,
                12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1,
                12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1,
                12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1,
                12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1,
                12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1,
                12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1,
                12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1,
                12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1,
                12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 4u8,
            ],
        )?;
        // verify u24, and large_size
        {
            let null_array: [u8; 513] = std::array::from_fn(|i| {
                match i {
                    0 => 3u8,
                    1 => 255u8,
                    j => if j <= 257 {
                        (j - 2) as u8
                    } else {
                        0u8
                    }
                }
            });
            // 256 elements => large size
            // each element is an array of 256 nulls => u24 offset
            let mut whole_array: [u8; 5 + 3 * 257 + 256 * 513] = std::array::from_fn(|i| {
                match i {
                    0 => 0x1Bu8,
                    1 => 0u8,
                    2 => 1u8,
                    3 => 0u8,
                    4 => 0u8,
                    _ => 0
                }
            });
            for i in 0..257 {
                let cur_idx = 5 + i * 3 as usize;
                let cur_offset: usize = i * 513;
                whole_array[cur_idx..cur_idx + 3].copy_from_slice(&cur_offset.to_le_bytes()[..3]);
                if i != 256 {
                    let abs_offset = 5 + 3 * 257 + cur_offset;
                    whole_array[abs_offset..abs_offset + 513].copy_from_slice(&null_array);
                }
            }
            let intermediate = format!("[{}]", vec!["null"; 255].join(", "));
            let json = format!("[{}]", vec![intermediate; 256].join(", "));
            compare_results(
                json.as_str(),
                &whole_array,
            )?;
        }

        // objects
        compare_results(
            "{\"b\": 2, \"a\": 1, \"a\": 3}",
            &[2u8, 2u8, 1u8, 0u8, 2u8, 0u8, 4u8, 12u8, 2u8, 12u8, 3u8],
        )?;
        compare_results(
            "{\"numbers\": [4, -3e0, 1.001], \"null\": null, \"booleans\": [true, false]}",
            &[
                2u8, 3u8, 2u8, 1u8, 0u8, 24u8, 23u8, 0u8, 31u8, 3u8, 3u8, 0u8, 2u8, 11u8, 17u8,
                12u8, 4u8, 28u8, 0, 0, 0, 0, 0, 0, 0x08, 0xc0, 32u8, 3, 0xe9, 0x03, 0, 0, 0, 3u8,
                2u8, 0u8, 1u8, 2u8, 4u8, 8u8,
            ],
        )?;
        // TODO: verify different offset_size, id_size and is_large values

        Ok(())
    }
}
