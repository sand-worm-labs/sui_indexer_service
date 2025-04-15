mod write_to_csv;

use anyhow::{Ok, Result};
use async_trait::async_trait;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time;
use sui_data_ingestion_core::{Worker, setup_single_workflow};
use sui_types::crypto::EncodeDecodeBase64;
use sui_types::full_checkpoint_content::CheckpointData;

use write_to_csv::write_checkpoint_to_csv;

static FIRST_WRITE: OnceLock<AtomicBool> = OnceLock::new();
struct CheckpointWriter;
struct TransactionWriter;
struct  EventWriter;

#[async_trait]
impl Worker for CheckpointWriter {
    type Result = ();
    async fn process_checkpoint(&self, checkpoint: &CheckpointData) -> Result<()> {
        let data = checkpoint.checkpoint_summary.data();
        let first_write = FIRST_WRITE.get_or_init(|| AtomicBool::new(true));
        let is_first = first_write.swap(false, Ordering::SeqCst);
        write_checkpoint_to_csv(
            data,
            checkpoint.transactions.len(),
            "checkpoint.csv",
            is_first,
        )
        .unwrap();
        Ok(())
    }
}

#[async_trait]
impl Worker for TransactionWriter {
    type Result = ();
    async fn process_checkpoint(&self, checkpoint: &CheckpointData) -> Result<()> {
        let checkpoint_number = checkpoint.checkpoint_summary.sequence_number;
        let timestamp = checkpoint.checkpoint_summary.timestamp_ms;
        let checkpoint_digest = checkpoint.checkpoint_summary.digest().base58_encode();

        checkpoint.transactions.iter().for_each(|tx| {
            println!("Checkpoint Number: {}", checkpoint_number);
            println!("Timestamp (ms): {}", timestamp);
            println!("Checkpoint Digest: {:?}", checkpoint_digest);
            // // Print sender address
            println!("Sender: {:?} ", tx.transaction.sender_address());
            print!("Ammount  {:?} \n \n ", tx.transaction.data());

            // Print gas object
            println!("Gas: {:?} ", tx.transaction.gas());

            // Print full transaction data
            //println!("Gas price: {:?} \n", tx.transaction.data());

            // Print gas owner address
            println!("Gas owner: {:?} ", tx.transaction.gas_owner());

            // Print full transaction for inspection (likely way too verbose for real use)
            //println!("Full Transaction: {:?} \n", tx.transaction);

            // Encode and print all signatures in base64
            let encoded_signatures: Vec<String> = tx.transaction.tx_signatures()
                .iter()
                .map(|sig| sig.encode_base64())
                .collect();

            println!("Signatures: {:?} \n", encoded_signatures);

            // You can collect sender addresses later if needed
            // let sender_address = tx.transaction.sender_address().to_string();
        });
            
        // println!(
        //     "Checkpoint: {} \n Timestamp: {} \n Addresses: {:?}  {:?}\n \n",
        //     checkpoint_number, timestamp, addresses, checkpoint_digest
        // );
        // checkpoint.transactions.iter().for_each(|tx| {
        //     println!("Transaction: {:?} \n \n ", tx);
        // });
        // let data = checkpoint.checkpoint_summary.data();
        // let first_write = FIRST_WRITE.get_or_init(|| AtomicBool::new(true));
        // let is_first = first_write.swap(false, Ordering::SeqCst);
        // write_checkpoint_to_csv(
        //     data,
        //     checkpoint.transactions.len(),
        //     "checkpoint.csv",
        //     is_first,
        // )
        // .unwrap();
        Ok(())
    }
}

#[async_trait]
impl Worker for EventWriter {
    type Result = ();
    async fn process_checkpoint(&self, checkpoint: &CheckpointData) -> Result<()> {
        let checkpoint_number = checkpoint.checkpoint_summary.sequence_number;
        let timestamp = checkpoint.checkpoint_summary.timestamp_ms;
        let checkpoint_digest = checkpoint.checkpoint_summary.digest().base58_encode();

        checkpoint.transactions.iter().for_each(|tx| {
            let tx_d = tx.transaction.digest().base58_encode();
            tx.events.iter().for_each(|event| {
                println!("Checkpoint Number: {}", checkpoint_number);
                println!("Timestamp (ms): {}", timestamp);
                println!("Checkpoint Digest: {:?}", checkpoint_digest);
                println!("Transation Digest: {:?} ", tx_d);
                event.data.iter().for_each(|d| {
                    println!("package_id: {:?} ", d.package_id);
                    println!("transaction_module: {:?} ", d.transaction_module.as_str());
                    println!("sender: {:?} ", d.sender);
                    println!("type_address: {:?} ", d.type_.address);
                    println!("module: {:?} ", d.type_.module.as_str());
                    println!("name: {:?} \n \n", d.type_.name.as_str());
                });
                //println!("Event: {:?} ", event);
                // // Print sender address
            })

            // You can collect sender addresses later if needed
            // let sender_address = tx.transaction.sender_address().to_string();
        });
            
        // println!(
        //     "Checkpoint: {} \n Timestamp: {} \n Addresses: {:?}  {:?}\n \n",
        //     checkpoint_number, timestamp, addresses, checkpoint_digest
        // );
        // checkpoint.transactions.iter().for_each(|tx| {
        //     println!("Transaction: {:?} \n \n ", tx);
        // });
        // let data = checkpoint.checkpoint_summary.data();
        // let first_write = FIRST_WRITE.get_or_init(|| AtomicBool::new(true));
        // let is_first = first_write.swap(false, Ordering::SeqCst);
        // write_checkpoint_to_csv(
        //     data,
        //     checkpoint.transactions.len(),
        //     "checkpoint.csv",
        //     is_first,
        // )
        // .unwrap();
        Ok(())
    }
}
#[tokio::main]
async fn main() -> Result<()> {
    let (executor, term_sender) = setup_single_workflow(
        EventWriter,
        "https://checkpoints.mainnet.sui.io".to_string(),
        134001767, /* initial checkpoint number */
        5,         /* concurrency */
        None,      /* extra reader options */
    )
    .await?;
    executor.await?;
    Ok(())
}
