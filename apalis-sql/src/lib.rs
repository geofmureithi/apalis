use apalis_core::backend::StatType;

pub mod config;
pub mod context;
pub mod from_row;

pub fn stat_type_from_string(s: &str) -> StatType {
    match s {
        "Number" => StatType::Number,
        "Decimal" => StatType::Decimal,
        "Percentage" => StatType::Percentage,
        "Timestamp" => StatType::Timestamp,
        _ => StatType::Number,
    }
}
