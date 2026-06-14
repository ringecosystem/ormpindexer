use super::rows::{OrmpMessageAcceptedRow, OrmpMessageAssignedRow};

pub const ADDRESS_RELAYER: &[&str] = &[
    "0x114890eb7386f94eae410186f20968bfaf66142a",
    "0xb607762f43f1a72593715497d4a7ddd754c62a6a",
];

pub const ADDRESS_ORACLE: &[&str] = &[
    "0x8d8a2bd991c1d900c59a82a2eeb0df44e0671aab",
    "0x2cdc7178013de451ed99607ac15def6bab8c37e6",
];

pub const LEGACY_B49E_ORACLE: &str = "0xb49e82067a54b3e8c5d9db2f378fdb6892c04d2e";
pub const LEGACY_B49E_ORACLE_FROM_BLOCK: u128 = 22_474_070;
pub const LEGACY_B49E_DARWINIA_FROM_BLOCK: u128 = 6_634_860;
pub const LEGACY_B49E_ARBITRUM_FROM_BLOCK: u128 = 334_644_126;
pub const LEGACY_MIXED_CASE_ACCEPTED_ID: &str =
    "0x5e6f833385d1a3041e8033e64c32c7c931104bc56881ef155fcb6032e87617df";
pub const LEGACY_MIXED_CASE_ACCEPTED_ORACLE: &str = "0x8d8a2Bd991c1d900C59a82a2EEb0DF44e0671aaB";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AssignmentConfig {
    pub oracle_addresses: Vec<String>,
    pub relayer_addresses: Vec<String>,
}

impl AssignmentConfig {
    pub fn legacy_defaults() -> Self {
        Self {
            oracle_addresses: ADDRESS_ORACLE
                .iter()
                .map(|address| address.to_string())
                .collect(),
            relayer_addresses: ADDRESS_RELAYER
                .iter()
                .map(|address| address.to_string())
                .collect(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AssignmentUpdate {
    pub oracle: bool,
    pub relayer: bool,
}

pub fn apply_assignment_to_accepted(
    accepted: &mut OrmpMessageAcceptedRow,
    assigned: &OrmpMessageAssignedRow,
    config: &AssignmentConfig,
) -> AssignmentUpdate {
    if accepted.id != assigned.msg_hash {
        return AssignmentUpdate::default();
    }

    let mut update = AssignmentUpdate::default();

    if contains_address(&config.relayer_addresses, &assigned.relayer) {
        accepted.relayer = Some(assigned.relayer.clone());
        accepted.relayer_assigned = Some(true);
        accepted.relayer_assigned_fee = Some(assigned.relayer_fee);
        update.relayer = true;
    }

    if is_oracle_assignment_for_accepted(accepted, assigned, config) {
        accepted.oracle = Some(accepted_oracle_value(&accepted.id, &assigned.oracle).to_owned());
        accepted.oracle_assigned = Some(true);
        accepted.oracle_assigned_fee = Some(assigned.oracle_fee);
        update.oracle = true;
    }

    update
}

pub fn is_oracle_assignment_for_accepted(
    accepted: &OrmpMessageAcceptedRow,
    assigned: &OrmpMessageAssignedRow,
    config: &AssignmentConfig,
) -> bool {
    (contains_address(&config.oracle_addresses, &assigned.oracle)
        && !assigned.oracle.eq_ignore_ascii_case(LEGACY_B49E_ORACLE))
        || (assigned.oracle.eq_ignore_ascii_case(LEGACY_B49E_ORACLE)
            && ((accepted.chain_id == 1
                && accepted.from_chain_id == 1
                && accepted.to_chain_id == 46
                && accepted.block_number >= LEGACY_B49E_ORACLE_FROM_BLOCK)
                || (accepted.chain_id == 46
                    && accepted.from_chain_id == 46
                    && accepted.block_number >= LEGACY_B49E_DARWINIA_FROM_BLOCK)
                || (accepted.chain_id == 42_161
                    && accepted.from_chain_id == 42_161
                    && accepted.block_number >= LEGACY_B49E_ARBITRUM_FROM_BLOCK)))
}

pub fn accepted_oracle_value<'a>(accepted_id: &str, oracle: &'a str) -> &'a str {
    if accepted_id == LEGACY_MIXED_CASE_ACCEPTED_ID
        && oracle.eq_ignore_ascii_case(LEGACY_MIXED_CASE_ACCEPTED_ORACLE)
    {
        LEGACY_MIXED_CASE_ACCEPTED_ORACLE
    } else {
        oracle
    }
}

fn contains_address(addresses: &[String], candidate: &str) -> bool {
    let candidate = candidate.to_ascii_lowercase();
    addresses
        .iter()
        .any(|address| address.eq_ignore_ascii_case(&candidate))
}
