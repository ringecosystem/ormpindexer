use ethabi::ethereum_types::H160;

pub(super) fn parse_u128(value: &str) -> u128 {
    value.parse().expect("numeric database value")
}

pub(super) fn address(value: u64) -> H160 {
    H160::from_low_u64_be(value)
}

pub(super) fn address_hex(value: u64) -> String {
    format!("0x{}", hex::encode(address(value).as_bytes()))
}

pub(super) fn bytes32(value: u8) -> Vec<u8> {
    vec![value; 32]
}

pub(super) fn bytes_hex(value: u8) -> String {
    format!("0x{}", hex::encode(bytes32(value)))
}
