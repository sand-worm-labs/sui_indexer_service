use csv::Writer;
use std::error::Error;
use std::fs::OpenOptions;
use sui_types::messages_checkpoint::CheckpointSummary;

pub(crate) fn write_checkpoint_to_csv(
    summary: &CheckpointSummary,
    transaction: usize,
    path: &str,
    first_row: bool,
) -> Result<(), Box<dyn Error>> {
    let file = if first_row {
        // Create or overwrite the file
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?
    } else {
        // Append to the existing file
        OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(path)?
    };

    let mut wtr = Writer::from_writer(file);

    if first_row {
        wtr.write_record(&[
            "epoch",
            "sequence_number",
            "network_total_transactions",
            "content_digest",
            "computation_cost",
            "storage_cost",
            "storage_rebate",
            "non_refundable_storage_fee",
            "timestamp_ms",
            "transactions",
        ])?;
    }

    wtr.write_record(&[
        &summary.epoch.to_string(),
        &summary.sequence_number.to_string(),
        &summary.network_total_transactions.to_string(),
        &summary.content_digest.base58_encode(),
        &summary
            .epoch_rolling_gas_cost_summary
            .computation_cost
            .to_string(),
        &summary
            .epoch_rolling_gas_cost_summary
            .storage_cost
            .to_string(),
        &summary
            .epoch_rolling_gas_cost_summary
            .storage_rebate
            .to_string(),
        &summary
            .epoch_rolling_gas_cost_summary
            .non_refundable_storage_fee
            .to_string(),
        &summary.timestamp_ms.to_string(),
        &transaction.to_string(),
    ])?;

    wtr.flush()?;
    Ok(())
}
