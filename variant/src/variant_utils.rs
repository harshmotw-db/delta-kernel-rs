pub const PRIMITIVE: u8 = 0;
pub const SHORT_STR: u8 = 1;
pub const OBJECT: u8 = 2;
pub const ARRAY: u8 = 3;

pub const NULL: u8 = 0;
pub const TRUE: u8 = 1;
pub const FALSE: u8 = 2;

pub const INT1: u8 = 3;
pub const INT2: u8 = 4;
pub const INT4: u8 = 5;
pub const INT8: u8 = 6;

pub const DOUBLE: u8 = 7;

pub const DECIMAL4: u8 = 8;
pub const DECIMAL8: u8 = 9;
pub const DECIMAL16: u8 = 10;

pub const LONG_STR: u8 = 16;

pub const MAX_SHORT_STR_SIZE: u8 = 0x3F;

pub const U8_SIZE: u8 = 1;
pub const U16_SIZE: u8 = 2;
pub const U24_SIZE: u8 = 3;
pub const U32_SIZE: u8 = 4;
pub const U64_SIZE: u8 = 8;

pub const U8_MAX: u8 = 0xFF;
pub const U16_MAX: u16 = 0xFFFF;

pub const BASIC_TYPE_BITS: u8 = 2;

pub const MAX_UNSCALED_DECIMAL_4: i32 = 999999999;
pub const MAX_PRECISION_DECIMAL_4: u8 = 9;
pub const MAX_UNSCALED_DECIMAL_8: i64 = 999999999999999999i64;
pub const MAX_PRECISION_DECIMAL_8: u8 = 18;
