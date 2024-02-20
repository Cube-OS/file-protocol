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

use super::Message;
use crate::error::ProtocolError;

pub fn parse_channel_id(message: &[u8]) -> Result<u32, ProtocolError> {
    
    // Deserialize the first parameter into a Message
    let message: Message = bincode::deserialize::<Message>(message)
        .map_err(|_| ProtocolError::MessageParseError {
            err: "Failed to deserialize Message".to_owned(),
        })?;

    // Extract the channel_id from the Message
    let channel_id = match message {
        Message::ReqReceive { channel_id, .. } => channel_id,
        Message::ReqTransmit { channel_id, .. } => channel_id,
        Message::Cleanup { channel_id, .. } => channel_id,
        Message::Sync { channel_id, .. } => channel_id,
        Message::SuccessReceive { channel_id, .. } => channel_id,
        Message::SuccessTransmit { channel_id, .. } => channel_id,
        Message::Failure { channel_id, .. } => channel_id,
        Message::ACK { channel_id, .. } => channel_id,
        Message::NAK { channel_id, .. } => channel_id,
        Message::ReceiveChunk { channel_id, .. } => channel_id,
        Message::Metadata { channel_id, .. } => channel_id,
    };

    Ok(channel_id)
}