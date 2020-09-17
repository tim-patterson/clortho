use std::convert::TryInto;

/// Writes an unsigned int into a buffer with lexicographical sort attempting
/// to not use too much space
pub(crate) fn write_varint_unsigned(i: u32, buffer: &mut Vec<u8>) {
    // To maintain the lexicographical sorting we'll use the first byte to encode the size of
    // the integer, with the integer itself encoded as bigendian we'll encode very small values
    // into the discriminator, for desc, we'll just flip all the bits
    if i < 253 {
        buffer.push(i as u8);
    } else if i <= u16::MAX as u32 {
        buffer.push(253);
        buffer.extend_from_slice((i as u16).to_be_bytes().as_ref());
    } else {
        buffer.push(254);
        buffer.extend_from_slice(i.to_be_bytes().as_ref());
    }
}

/// Read an unsigned int from a buffer
pub(crate) fn read_varint_unsigned<'a>(i: &mut u32, buffer: &'a [u8]) -> &'a [u8] {
    let rem = &buffer[1..];
    match buffer[0] {
        253 => {
            *i = u16::from_be_bytes(rem[..2].as_ref().try_into().unwrap()) as u32;
            &rem[2..]
        }
        254 => {
            *i = u32::from_be_bytes(rem[..4].as_ref().try_into().unwrap());
            &rem[4..]
        }
        b => {
            *i = b as u32;
            rem
        }
    }
}

/// The byte encoding for 0.
pub const VARINT_SIGNED_ZERO_ENC: u8 = 103;
/// Writes a signed int into a buffer with lexicographical sort attempting
/// to not use too much space
pub(crate) fn write_varint_signed(i: i64, buffer: &mut Vec<u8>) {
    // To maintain the lexicographical sorting we'll use the first byte to encode the size and sign
    // of the integer.
    // 0 for -i64, 1 for -u32, 2 for -u16, 3 for -u8
    // 255 for i64, 254 for u32, 253 for u16, 252 for u8
    // As we're using the discriminator to store the sign we'll use unsigned encoding to
    // squeeze a tiny bit more space out without having to resort to bit shifting etc
    // That leaves space for 248 small values, positives will be more likely so we'll
    // make 4 = -100, which means 251 = 148 with a "displacement" of 103

    #[allow(clippy::collapsible_if)]
    if i >= 0 {
        if i <= 148 {
            buffer.push(i as u8 + 103);
        } else if i <= u8::MAX as i64 {
            buffer.push(252);
            buffer.push(i as u8);
        } else if i <= u16::MAX as i64 {
            buffer.push(253);
            buffer.extend_from_slice((i as u16).to_be_bytes().as_ref());
        } else if i <= u32::MAX as i64 {
            buffer.push(254);
            buffer.extend_from_slice((i as u32).to_be_bytes().as_ref());
        } else {
            buffer.push(255);
            buffer.extend_from_slice(i.to_be_bytes().as_ref());
        }
    } else {
        if i >= -99 {
            buffer.push((i + 103) as u8);
        } else if i >= -(u8::MAX as i64) {
            buffer.push(3);
            buffer.push(!(-i as u8));
        } else if i >= -(u16::MAX as i64) {
            buffer.push(2);
            buffer.extend_from_slice((!(-i as u16)).to_be_bytes().as_ref());
        } else if i >= -(u32::MAX as i64) {
            buffer.push(1);
            buffer.extend_from_slice((!(-i as u32)).to_be_bytes().as_ref());
        } else {
            buffer.push(0);
            buffer.extend_from_slice(i.to_be_bytes().as_ref());
        }
    }
}

/// Read an signed int from a buffer
pub(crate) fn read_varint_signed<'a>(i: &mut i64, buffer: &'a [u8]) -> &'a [u8] {
    let mut rem = &buffer[1..];
    rem = match buffer[0] {
        0 => {
            *i = i64::from_be_bytes(rem[..8].as_ref().try_into().unwrap());
            &rem[8..]
        }
        1 => {
            *i = -(!(u32::from_be_bytes(rem[..4].as_ref().try_into().unwrap())) as i64);
            &rem[4..]
        }
        2 => {
            *i = -(!(u16::from_be_bytes(rem[..2].as_ref().try_into().unwrap())) as i64);
            &rem[2..]
        }
        3 => {
            *i = -(!rem[0] as i64);
            &rem[1..]
        }
        252 => {
            *i = rem[0] as i64;
            &rem[1..]
        }
        253 => {
            *i = u16::from_be_bytes(rem[..2].as_ref().try_into().unwrap()) as i64;
            &rem[2..]
        }
        254 => {
            *i = u32::from_be_bytes(rem[..4].as_ref().try_into().unwrap()) as i64;
            &rem[4..]
        }
        255 => {
            let u = u64::from_be_bytes(rem[..8].as_ref().try_into().unwrap());
            *i = u as i64;
            &rem[8..]
        }
        b => {
            *i = b as i64 - 103;
            rem
        }
    };
    rem
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_varint_unsigned() {
        let mut numbers = [0_u32, 123, u8::MAX.into(), u16::MAX.into(), u32::MAX];
        let mut asc_byte_arrays = vec![];

        // Encode into separate buffers
        for i in &numbers {
            let mut buf = vec![];
            write_varint_unsigned(*i, &mut buf);
            asc_byte_arrays.push(buf);
        }

        // Sort the buffers and the numbers;
        asc_byte_arrays.sort();
        numbers.sort();

        assert_eq!(asc_byte_arrays.len(), numbers.len());

        // Decode and make sure we're still in numeric order
        for (expected, asc_buf) in numbers.iter().zip(asc_byte_arrays) {
            let mut actual = 0_u32;
            let rem = read_varint_unsigned(&mut actual, &asc_buf);
            assert_eq!(actual, *expected);
            assert!(rem.is_empty());
        }
    }

    #[test]
    fn test_varint_signed() {
        let mut numbers = [
            0_i64,
            i8::MIN.into(),
            i8::MAX.into(),
            u8::MAX.into(),
            i16::MIN.into(),
            i16::MAX.into(),
            u16::MAX.into(),
            i32::MIN.into(),
            i32::MAX.into(),
            u32::MAX.into(),
            i64::MIN,
            i64::MAX,
        ];
        let mut asc_byte_arrays = vec![];

        // Encode into separate buffers
        for i in &numbers {
            let mut buf = vec![];
            write_varint_signed(*i, &mut buf);
            asc_byte_arrays.push(buf);
        }

        // Sort the buffers and the numbers;
        asc_byte_arrays.sort();
        numbers.sort();

        assert_eq!(asc_byte_arrays.len(), numbers.len());

        // Decode and make sure we're still in numeric order
        for (expected, asc_buf) in numbers.iter().zip(asc_byte_arrays) {
            let mut actual = 0_i64;
            let rem = read_varint_signed(&mut actual, &asc_buf);
            assert_eq!(actual, *expected);
            assert!(rem.is_empty());
        }
    }
    #[test]
    fn test_varint_signed_zero_constant() {
        let encoded = [VARINT_SIGNED_ZERO_ENC];
        let mut i = 999_i64;
        read_varint_signed(&mut i, &encoded);
        assert_eq!(i, 0)
    }
}
