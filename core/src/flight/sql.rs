// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Default [FlightDriver] for Flight SQL

use std::collections::HashMap;
use std::str::FromStr;

use arrow_flight::error::Result;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt};
use arrow_flight::{FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse};
use arrow_schema::ArrowError;
use async_trait::async_trait;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use bytes::Bytes;
use futures::{stream, TryStreamExt};
use prost::Message;
use tonic::metadata::AsciiMetadataKey;
use tonic::transport::Channel;
use tonic::IntoRequest;

use crate::flight::{FlightDriver, FlightMetadata};

pub const QUERY: &str = "flight.sql.query";
pub const USERNAME: &str = "flight.sql.username";
pub const PASSWORD: &str = "flight.sql.password";
pub const HEADER_PREFIX: &str = "flight.sql.header.";

/// Default Flight SQL driver. Requires a [QUERY] to be passed as a table option.
/// If [USERNAME] (and optionally [PASSWORD]) are passed,
/// will perform the `Handshake` using basic authentication.
/// Any additional headers for the `GetFlightInfo` call can be passed as table options
/// using the [HEADER_PREFIX] prefix.
/// If a token is returned by the server with the handshake response, it will be
/// stored as a gRPC authorization header within the returned [FlightMetadata],
/// to be sent with the subsequent `DoGet` requests.
#[derive(Clone, Debug, Default)]
pub struct FlightSqlDriver {}

#[async_trait]
impl FlightDriver for FlightSqlDriver {
    async fn metadata(
        &self,
        channel: Channel,
        options: &HashMap<String, String>,
    ) -> Result<FlightMetadata> {
        let mut client = FlightSqlClient::new(channel);
        let headers = options.iter().filter_map(|(key, value)| {
            key.strip_prefix(HEADER_PREFIX)
                .map(|header_name| (header_name, value))
        });
        for header in headers {
            client.set_header(header.0, header.1)
        }
        if let Some(username) = options.get(USERNAME) {
            let default_password = "".to_string();
            let password = options.get(PASSWORD).unwrap_or(&default_password);
            _ = client.handshake(username, password).await?;
        }
        let info = client.execute(options[QUERY].clone(), None).await?;
        let mut grpc_headers = HashMap::default();
        if let Some(token) = client.token {
            grpc_headers.insert("authorization".into(), format!("Bearer {}", token));
        }
        FlightMetadata::try_new(info, grpc_headers)
    }
}

/////////////////////////////////////////////////////////////////////////
// Shameless copy/paste from arrow-flight FlightSqlServiceClient
// (only cherry-picked the functionality that we actually use).
// This is only needed in order to access the bearer token received
// during handshake, as the standard client does not expose this information.
// The bearer token has to be passed to the clients that perform
// the DoGet operation, since Dremio, Ballista and possibly others
// expect the bearer token they produce with the handshake response
// to be set on all subsequent requests, including DoGet.
//
// TODO: remove this and switch to the official client once
//  https://github.com/apache/arrow-rs/pull/6254 is released,
//  and remove a bunch of cargo dependencies, like base64 or bytes
#[derive(Debug, Clone)]
struct FlightSqlClient {
    token: Option<String>,
    headers: HashMap<String, String>,
    flight_client: FlightServiceClient<Channel>,
}

impl FlightSqlClient {
    /// Creates a new FlightSql client that connects to a server over an arbitrary tonic `Channel`
    fn new(channel: Channel) -> Self {
        Self {
            token: None,
            flight_client: FlightServiceClient::new(channel),
            headers: HashMap::default(),
        }
    }

    /// Perform a `handshake` with the server, passing credentials and establishing a session.
    ///
    /// If the server returns an "authorization" header, it is automatically parsed and set as
    /// a token for future requests. Any other data returned by the server in the handshake
    /// response is returned as a binary blob.
    async fn handshake(
        &mut self,
        username: &str,
        password: &str,
    ) -> std::result::Result<Bytes, ArrowError> {
        let cmd = HandshakeRequest {
            protocol_version: 0,
            payload: Default::default(),
        };
        let mut req = tonic::Request::new(stream::iter(vec![cmd]));
        let val = BASE64_STANDARD.encode(format!("{username}:{password}"));
        let val = format!("Basic {val}")
            .parse()
            .map_err(|_| ArrowError::ParseError("Cannot parse header".to_string()))?;
        req.metadata_mut().insert("authorization", val);
        let req = self.set_request_headers(req)?;
        let resp = self
            .flight_client
            .handshake(req)
            .await
            .map_err(|e| ArrowError::IpcError(format!("Can't handshake {e}")))?;
        if let Some(auth) = resp.metadata().get("authorization") {
            let auth = auth
                .to_str()
                .map_err(|_| ArrowError::ParseError("Can't read auth header".to_string()))?;
            let bearer = "Bearer ";
            if !auth.starts_with(bearer) {
                Err(ArrowError::ParseError("Invalid auth header!".to_string()))?;
            }
            let auth = auth[bearer.len()..].to_string();
            self.token = Some(auth);
        }
        let responses: Vec<HandshakeResponse> = resp
            .into_inner()
            .try_collect()
            .await
            .map_err(|_| ArrowError::ParseError("Can't collect responses".to_string()))?;
        let resp = match responses.as_slice() {
            [resp] => resp.payload.clone(),
            [] => Bytes::new(),
            _ => Err(ArrowError::ParseError(
                "Multiple handshake responses".to_string(),
            ))?,
        };
        Ok(resp)
    }

    async fn execute(
        &mut self,
        query: String,
        transaction_id: Option<Bytes>,
    ) -> std::result::Result<FlightInfo, ArrowError> {
        let cmd = CommandStatementQuery {
            query,
            transaction_id,
        };
        self.get_flight_info_for_command(cmd).await
    }

    async fn get_flight_info_for_command<M: ProstMessageExt>(
        &mut self,
        cmd: M,
    ) -> std::result::Result<FlightInfo, ArrowError> {
        let descriptor = FlightDescriptor::new_cmd(cmd.as_any().encode_to_vec());
        let req = self.set_request_headers(descriptor.into_request())?;
        let fi = self
            .flight_client
            .get_flight_info(req)
            .await
            .map_err(|status| ArrowError::IpcError(format!("{status:?}")))?
            .into_inner();
        Ok(fi)
    }

    fn set_header(&mut self, key: impl Into<String>, value: impl Into<String>) {
        let key: String = key.into();
        let value: String = value.into();
        self.headers.insert(key, value);
    }

    fn set_request_headers<T>(
        &self,
        mut req: tonic::Request<T>,
    ) -> std::result::Result<tonic::Request<T>, ArrowError> {
        for (k, v) in &self.headers {
            let k = AsciiMetadataKey::from_str(k.as_str()).map_err(|e| {
                ArrowError::ParseError(format!("Cannot convert header key \"{k}\": {e}"))
            })?;
            let v = v.parse().map_err(|e| {
                ArrowError::ParseError(format!("Cannot convert header value \"{v}\": {e}"))
            })?;
            req.metadata_mut().insert(k, v);
        }
        if let Some(token) = &self.token {
            let val = format!("Bearer {token}").parse().map_err(|e| {
                ArrowError::ParseError(format!("Cannot convert token to header value: {e}"))
            })?;
            req.metadata_mut().insert("authorization", val);
        }
        Ok(req)
    }
}
