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

use failure::Fail;
use std::io;

/// Errors which occur when using FileProtocol
#[derive(Debug, Fail)]
pub enum ProtocolError {
    /// A file in storage was corrupt
    #[fail(display = "File was corrupt: {}", _0)]
    CorruptFile(String),
    /// An error was encountered when finalizing the file
    #[fail(display = "Failed to finalize file: {}", cause)]
    FinalizeError {
        /// The cause of the finalizing failure
        cause: String,
    },
    /// A hash mismatch was found when finalizing the file
    #[fail(display = "File hash mismatch")]
    HashMismatch,
    /// An invalid value was found when parsing a message
    #[fail(display = "Unable to parse {} message: Invalid {} param", _0, _1)]
    InvalidParam(String, String),
    /// A general error was encountered when parsing a message
    #[fail(display = "Unable to parse message: {}", err)]
    MessageParseError {
        /// Underlying error encountered
        err: String,
    },
    /// A value was missing when parsing a message
    #[fail(display = "Unable to parse {} message: No {} param", _0, _1)]
    MissingParam(String, String),
    /// An error was encountered when receiving a message
    #[fail(display = "Failure receiving message: {}", err)]
    ReceiveError {
        /// Underlying error encountered
        err: String,
    },
    /// An error was encounter when sending a message
    #[fail(display = "Failure sending message: {}", err)]
    SendError {
        /// Underlying error encountered
        err: String,
    },
    /// An error was encountered when serializing data
    #[fail(display = "Failed to serialize: {}", err)]
    Serialize {
        /// Underlying serde error
        err: bincode::Error,
    },
    /// An error was encountered when writing to or reading from file storage
    #[fail(display = "Storage failed to {}: {}", action, err)]
    StorageError {
        /// The action which generated the error
        action: String,
        /// The underlying std::io::Error
        err: io::Error,
    },
    /// An error was encountered when parsing file storage data
    #[fail(display = "{}", _0)]
    StorageParseError(String),
    /// A timeout occurred when receiving data
    #[fail(display = "A receive timeout was encountered")]
    ReceiveTimeout,
    /// An error was encountered when transmitting
    #[fail(
        display = "Transmission failure on channel {}: {}",
        channel_id, error_message
    )]
    TransmissionError {
        /// Channel where the error occurred
        channel_id: u32,
        /// Message from underlying error
        error_message: String,
    },
}

impl From<bincode::Error> for ProtocolError {
    fn from(b: bincode::Error) -> ProtocolError {
        ProtocolError::Serialize { err: b }
    }
}