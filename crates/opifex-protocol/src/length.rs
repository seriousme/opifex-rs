use crate::ProtocolError;

pub const MAX_REMAINING_LENGTH: usize = 268_435_455;

pub fn encode_remaining_length(value: usize) -> Result<Vec<u8>, ProtocolError> {
    if value > MAX_REMAINING_LENGTH {
        return Err(ProtocolError::RemainingLengthOverflow);
    }

    let mut encoded = Vec::new();
    let mut remaining = value;

    loop {
        let mut byte = (remaining % 128) as u8;
        remaining /= 128;
        if remaining > 0 {
            byte |= 0x80;
        }
        encoded.push(byte);
        if remaining == 0 {
            break;
        }
    }

    Ok(encoded)
}

pub fn decode_remaining_length(bytes: &[u8]) -> Result<(usize, usize), ProtocolError> {
    let mut value = 0usize;
    let mut multiplier = 1usize;

    for (index, &byte) in bytes.iter().take(4).enumerate() {
        let digit = (byte & 0x7f) as usize;
        let next_value = value.saturating_add(digit.saturating_mul(multiplier));
        if next_value > MAX_REMAINING_LENGTH {
            return Err(ProtocolError::RemainingLengthOverflow);
        }
        value = next_value;

        if byte & 0x80 == 0 {
            return Ok((value, index + 1));
        }

        multiplier *= 128;
        if index == 3 {
            return Err(ProtocolError::MalformedRemainingLength);
        }
    }

    Err(ProtocolError::MalformedRemainingLength)
}

#[cfg(test)]
mod tests {
    use super::{decode_remaining_length, encode_remaining_length};
    use crate::ProtocolError;

    #[test]
    fn remaining_length_round_trips() {
        for value in [0, 1, 127, 128, 16383, 16384, 268_435_454] {
            let encoded = encode_remaining_length(value).unwrap();
            let (decoded, used) = decode_remaining_length(&encoded).unwrap();
            assert_eq!(decoded, value);
            assert_eq!(used, encoded.len());
        }
    }

    #[test]
    fn rejects_malformed_and_overflowing_remaining_lengths() {
        assert!(matches!(
            decode_remaining_length(&[0x80, 0x80, 0x80, 0x80]),
            Err(ProtocolError::MalformedRemainingLength)
        ));
        assert!(matches!(
            decode_remaining_length(&[0x80, 0x80, 0x80, 0x80, 0x01]),
            Err(ProtocolError::MalformedRemainingLength)
        ));
        assert!(matches!(
            decode_remaining_length(&[0x80, 0x80, 0x80, 0x80]),
            Err(ProtocolError::MalformedRemainingLength)
        ));
        let malformed = vec![0xFF, 0xFF, 0xFF, 0xFF];
        assert!(decode_remaining_length(&malformed).is_err());
    }
}
