//
// Copyright (C) 2018 Kubos Corporation
// Copyright (C) 2022 CUAVA
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// July 2022:
// - Rebranding Cube-OS
// - add feature client to display chunck progress

//! Kubos File Transfer Protocol
//!
//! # Examples
//!
//! ```no_run
//! use file_protocol::*;
//! use std::time::Duration;
//!
//! fn upload() -> Result<(), ProtocolError> {
//!     let config = FileProtocolConfig::new(Some("storage/dir".to_owned()), 1024, 5, 1, None, 2048);
//!     let f_protocol = FileProtocol::new("0.0.0.0", "0.0.0.0:7000", config, 5);
//!
//!     # ::std::fs::File::create("client.txt").unwrap();
//!     let source_path = "client.txt";
//!     let target_path = "service.txt";
//!
//!     // Copy file to upload to temp storage. Calculate the hash and chunk info
//!     let (_filename, hash, num_chunks, mode) = f_protocol.initialize_file(&source_path)?;
//!
//!     // Generate channel id
//!     let channel_id = f_protocol.generate_channel()?;
//!
//!     // Tell our destination the hash and number of chunks to expect
//!     f_protocol.send_metadata(channel_id, &hash, num_chunks)?;
//!
//!     // Send export command for file
//!     f_protocol.send_export(channel_id, &hash, &target_path, mode)?;
//!
//!     // Start the engine to send the file data chunks
//!     Ok(f_protocol.message_engine(
//!         |d| f_protocol.recv(Some(d)),
//!         Duration::from_millis(10),
//!         &State::Transmitting { transmitted_files: 0, total_files: 0 })?)
//! }
//! ```
//!
//! ```no_run
//! extern crate file_protocol;
//!
//! use file_protocol::*;
//! use std::time::Duration;
//!
//! fn download() -> Result<(), ProtocolError> {
//!     let config = FileProtocolConfig::new(None, 1024, 5, 1, None, 2048);
//!     let f_protocol = FileProtocol::new("0.0.0.0", "0.0.0.0:7000", config, 5);
//!
//!     let channel_id = f_protocol.generate_channel()?;
//!     # ::std::fs::File::create("service.txt").unwrap();
//!     let source_path = "service.txt";
//!     let target_path = "client.txt";
//!
//!     // Send our file request to the remote addr and verify that it's
//!     // going to be able to send it
//!     f_protocol.send_import(channel_id)?;
//!
//!     // Wait for the request reply
//!     let reply = match f_protocol.recv(None) {
//!         Ok(message) => message,
//!         Err(error) => return Err(error)
//!     };
//!
//!     let state = f_protocol.process_message(
//!         reply.as_slice(),
//!         &State::StartReceive {
//!             path: target_path.to_string(),
//!         },
//!     )?;
//!
//!     Ok(f_protocol.message_engine(|d| f_protocol.recv(Some(d)), Duration::from_millis(10), &state)?)
//! }
//! ```
//!

// #![deny(missing_docs)]

mod error;
mod parsers;
pub mod protocol;
mod storage;

pub use crate::error::ProtocolError;
pub use crate::protocol::Protocol as FileProtocol;
pub use crate::protocol::ProtocolConfig as FileProtocolConfig;
pub use crate::protocol::State;

pub use crate::parsers::parse_channel_id;

use serde::{Serialize,Deserialize};

/// File protocol message types
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum Message {
    /// TODO: Decide whether or not to keep this
    /// channel_id
    Sync{channel_id: u32, hash: String},
    /// Receiver should prepare a new temporary storage folder with the specified metadata
    Metadata{channel_id: u32, hash: String, num_chunks: u32},
    /// File data chunk message
    ReceiveChunk{channel_id: u32, hash: String, chunk_num: u32, data: Vec<u8>},
    /// Receiver has successfully gotten all data chunks of the requested file
    ACK{channel_id: u32, hash: String},
    /// Receiver is missing the specified file data chunks
    NAK{channel_id: u32, hash: String, missing_chunks: Option<Vec<u32>>},
    /// (Client Only) Message requesting the recipient to receive the specified file
    ReqReceive{channel_id: u32, hash: String, path: String, mode: Option<u32>},
    /// (Client Only) Message requesting the recipient to transmit the specified file
    ReqTransmit{channel_id: u32, path: Option<String>},
    /// (Server Only) Recipient has successfully processed a request to receive a file
    SuccessReceive{channel_id: u32, hash: String},
    /// (Server Only) Recipient has successfully prepared to transmit a file
    SuccessTransmit{channel_id: u32, file_name: String, hash: String, num_chunks: u32, mode: Option<u32>, last: bool},
    /// (Server Only) The transmit or receive request has failed to be completed
    Failure{channel_id: u32, error: String},
    /// Request Cleanup of either whole storage directory or individual file's storage
    Cleanup{channel_id: u32, hash: Option<String>},
}

impl std::fmt::Debug for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Message::ReceiveChunk { channel_id, hash, chunk_num, .. } => {
                f.debug_struct("ReceiveChunk")
                    .field("channel_id", channel_id)
                    .field("hash", hash)
                    .field("chunk_num", chunk_num)
                    .finish()
            }
            Message::ACK { channel_id, hash } => {
                f.debug_struct("ACK")
                    .field("channel_id", channel_id)
                    .field("hash", hash)
                    .finish()
            }
            Message::NAK { channel_id, hash, missing_chunks } => {
                f.debug_struct("NAK")
                    .field("channel_id", channel_id)
                    .field("hash", hash)
                    .field("missing_chunks", missing_chunks)
                    .finish()
            }
            Message::ReqReceive { channel_id, hash, path, mode } => {
                f.debug_struct("ReqReceive")
                    .field("channel_id", channel_id)
                    .field("hash", hash)
                    .field("path", path)
                    .field("mode", mode)
                    .finish()
            }
            Message::ReqTransmit { channel_id, path } => {
                f.debug_struct("ReqTransmit")
                    .field("channel_id", channel_id)
                    .field("path", path)
                    .finish()
            }
            Message::SuccessReceive { channel_id, hash } => {
                f.debug_struct("SuccessReceive")
                    .field("channel_id", channel_id)
                    .field("hash", hash)
                    .finish()
            }
            Message::SuccessTransmit { channel_id, file_name, hash, num_chunks, mode, last } => {
                f.debug_struct("SuccessTransmit")
                    .field("channel_id", channel_id)
                    .field("file_name", file_name)
                    .field("hash", hash)
                    .field("num_chunks", num_chunks)
                    .field("mode", mode)
                    .field("last", last)
                    .finish()
            }
            Message::Failure { channel_id, error } => {
                f.debug_struct("Failure")
                    .field("channel_id", channel_id)
                    .field("error", error)
                    .finish()
            }
            Message::Cleanup { channel_id, hash } => {
                f.debug_struct("Cleanup")
                    .field("channel_id", channel_id)
                    .field("hash", hash)
                    .finish()
            }
            Message::Sync { channel_id, hash } => {
                f.debug_struct("Sync")
                    .field("channel_id", channel_id)
                    .field("hash", hash)
                    .finish()
            }
            Message::Metadata { channel_id, hash, num_chunks } => {
                f.debug_struct("Metadata")
                    .field("channel_id", channel_id)
                    .field("hash", hash)
                    .field("num_chunks", num_chunks)
                    .finish()
            }            
        }
    }
}