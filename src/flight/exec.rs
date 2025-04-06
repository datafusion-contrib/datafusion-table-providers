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

//! Execution plan for reading flights from Arrow Flight services

use std::any::Any;
use std::error::Error;
use std::fmt::{Debug, Formatter};
use std::str::FromStr;
use std::sync::Arc;

use crate::flight::{FlightMetadata, FlightProperties};
use arrow_array::RecordBatch;
use arrow_flight::error::FlightError;
use arrow_flight::{FlightClient, FlightEndpoint, Ticket};
use arrow_schema::SchemaRef;
use datafusion::arrow::datatypes::ToByteSlice;
use datafusion::common::Result;
use datafusion::common::{project_schema, DataFusionError};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, PlanProperties,
};
use futures::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use tonic::metadata::{AsciiMetadataKey, MetadataMap};
use tonic::transport::Channel;

/// Arrow Flight physical plan that maps flight endpoints to partitions
#[derive(Clone, Debug)]
pub(crate) struct FlightExec {
    config: FlightConfig,
    plan_properties: PlanProperties,
    metadata_map: Arc<MetadataMap>,
}

impl FlightExec {
    /// Creates a FlightExec with the provided [FlightMetadata]
    /// and origin URL (used as fallback location as per the protocol spec).
    pub fn try_new(
        metadata: FlightMetadata,
        projection: Option<&Vec<usize>>,
        origin: &str,
    ) -> Result<Self> {
        let partitions = metadata
            .info
            .endpoint
            .iter()
            .map(|endpoint| FlightPartition::new(endpoint, origin.to_string()))
            .collect();
        let schema = project_schema(&metadata.schema, projection)?;
        let config = FlightConfig {
            origin: origin.into(),
            schema,
            partitions,
            properties: metadata.props,
        };
        Ok(config.into())
    }

    pub(crate) fn config(&self) -> &FlightConfig {
        &self.config
    }
}

impl From<FlightConfig> for FlightExec {
    fn from(config: FlightConfig) -> Self {
        let exec_mode = if config.properties.unbounded_stream {
            ExecutionMode::Unbounded
        } else {
            ExecutionMode::Bounded
        };
        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(config.schema.clone()),
            Partitioning::UnknownPartitioning(config.partitions.len()),
            exec_mode,
        );
        let mut mm = MetadataMap::new();
        for (k, v) in config.properties.grpc_headers.iter() {
            let key = AsciiMetadataKey::from_str(k.as_str()).expect("invalid header name");
            let value = v.parse().expect("invalid header value");
            mm.insert(key, value);
        }
        Self {
            config,
            plan_properties,
            metadata_map: Arc::from(mm),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct FlightConfig {
    origin: String,
    schema: SchemaRef,
    partitions: Arc<[FlightPartition]>,
    properties: FlightProperties,
}

/// The minimum information required for fetching a flight stream.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct FlightPartition {
    locations: Arc<[String]>,
    ticket: FlightTicket,
}

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
struct FlightTicket(Arc<[u8]>);

impl From<Option<&Ticket>> for FlightTicket {
    fn from(ticket: Option<&Ticket>) -> Self {
        let bytes = match ticket {
            Some(t) => t.ticket.to_byte_slice().into(),
            None => [].into(),
        };
        Self(bytes)
    }
}

impl Debug for FlightTicket {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[..{} bytes..]", self.0.len())
    }
}

impl FlightPartition {
    fn new(endpoint: &FlightEndpoint, fallback_location: String) -> Self {
        let locations = if endpoint.location.is_empty() {
            [fallback_location].into()
        } else {
            endpoint
                .location
                .iter()
                .map(|loc| {
                    if loc.uri.starts_with("arrow-flight-reuse-connection://") {
                        fallback_location.clone()
                    } else {
                        loc.uri.clone()
                    }
                })
                .collect()
        };

        Self {
            locations,
            ticket: endpoint.ticket.as_ref().into(),
        }
    }
}

async fn flight_stream(
    partition: FlightPartition,
    schema: SchemaRef,
    grpc_headers: Arc<MetadataMap>,
) -> Result<SendableRecordBatchStream> {
    let mut errors: Vec<Box<dyn Error + Send + Sync>> = vec![];
    for loc in partition.locations.iter() {
        match try_fetch_stream(
            loc,
            partition.ticket.clone(),
            schema.clone(),
            grpc_headers.clone(),
        )
        .await
        {
            Ok(stream) => return Ok(stream),
            Err(e) => errors.push(Box::new(e)),
        }
    }
    let err = errors.into_iter().next_back().unwrap_or_else(|| {
        Box::new(FlightError::ProtocolError(format!(
            "No available location for endpoint {:?}",
            partition.locations
        )))
    });
    Err(DataFusionError::External(err))
}

async fn try_fetch_stream(
    source: impl Into<String>,
    ticket: FlightTicket,
    schema: SchemaRef,
    grpc_headers: Arc<MetadataMap>,
) -> arrow_flight::error::Result<SendableRecordBatchStream> {
    let ticket = Ticket::new(ticket.0.to_vec());
    let channel = Channel::from_shared(source.into())
        .map_err(|e| FlightError::ExternalError(Box::new(e)))?
        .connect()
        .await
        .map_err(|e| FlightError::ExternalError(Box::new(e)))?;
    let mut client = FlightClient::new(channel);
    client.metadata_mut().clone_from(grpc_headers.as_ref());
    let stream = client.do_get(ticket).await?;
    Ok(Box::pin(RecordBatchStreamAdapter::new(
        schema.clone(),
        stream.map(move |rb| {
            let schema = schema.clone();
            rb.map(move |rb| {
                if schema.fields.is_empty() || rb.schema() == schema {
                    rb
                } else if schema.contains(rb.schema_ref()) {
                    rb.with_schema(schema.clone()).unwrap()
                } else {
                    let columns = schema
                        .fields
                        .iter()
                        .map(|field| {
                            rb.column_by_name(field.name())
                                .expect("missing fields in record batch")
                                .clone()
                        })
                        .collect();
                    RecordBatch::try_new(schema.clone(), columns)
                        .expect("cannot impose desired schema on record batch")
                }
            })
            .map_err(|e| DataFusionError::External(Box::new(e)))
        }),
    )))
}

impl DisplayAs for FlightExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => write!(
                f,
                "FlightExec: origin={}, streams={}",
                self.config.origin,
                self.config.partitions.len()
            ),
            DisplayFormatType::Verbose => write!(
                f,
                "FlightExec: origin={}, partitions={:?}, properties={:?}",
                self.config.origin, self.config.partitions, self.config.properties,
            ),
        }
    }
}

impl ExecutionPlan for FlightExec {
    fn name(&self) -> &str {
        "FlightExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let future_stream = flight_stream(
            self.config.partitions[partition].clone(),
            self.schema(),
            self.metadata_map.clone(),
        );
        let stream = futures::stream::once(future_stream).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

#[cfg(test)]
mod tests {
    use crate::flight::exec::{FlightConfig, FlightPartition, FlightTicket};
    use crate::flight::FlightProperties;
    use arrow_schema::{DataType, Field, Schema};
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn test_flight_config_serde() {
        let schema = Arc::new(Schema::new(vec![
            Arc::new(Field::new("f1", DataType::Utf8, true)),
            Arc::new(Field::new("f2", DataType::Int32, false)),
        ]));
        let partitions = [
            FlightPartition {
                locations: ["l1".into(), "l2".into()].into(),
                ticket: FlightTicket("ticket1".as_bytes().into()),
            },
            FlightPartition {
                locations: ["l3".into(), "l4".into()].into(),
                ticket: FlightTicket("ticket2".as_bytes().into()),
            },
        ]
        .into();
        let properties = FlightProperties::new(
            true,
            HashMap::from([("h1".into(), "v1".into()), ("h2".into(), "v2".into())]),
        );
        let config = FlightConfig {
            origin: "http://localhost:50050".into(),
            schema,
            partitions,
            properties,
        };
        let json = serde_json::to_vec(&config).expect("cannot encode config as json");
        let restored = serde_json::from_slice(json.as_slice()).expect("cannot decode json config");
        assert_eq!(config, restored);
    }
}
