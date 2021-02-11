use num_derive::{FromPrimitive, ToPrimitive};
use num_traits::ToPrimitive;

#[derive(Debug)]
pub struct RequestHeader {
    pub api_key: ApiKey,
    pub api_version: u16,
    pub correlation_id: u32,
    pub client_id: Option<String>,
}

#[derive(Debug)]
pub enum Request {
    ApiVersionsRequest(ApiVersionsRequest),
    MetadataRequest(MetadataRequest),
}

#[derive(Debug)]
pub enum Response {
    ApiVersionsResponse(ApiVersionsResponse),
    MetadataResponse(MetadataResponse),
}

#[derive(Debug, FromPrimitive, ToPrimitive)]
pub enum ApiKey {
    Produce = 0,
    Fetch = 1,
    Offsets = 2,
    Metadata = 3,
    LeaderAndIsr = 4,
    StopReplica = 5,
    UpdateMetadata = 6,
    ControlledShutdown = 7,
    OffsetCommit = 8,
    OffsetFetch = 9,
    GroupCoordinator = 10,
    JoinGroup = 11,
    Heartbeat = 12,
    LeaveGroup = 13,
    SyncGroup = 14,
    DescribeGroups = 15,
    ListGroups = 16,
    SaslHandshake = 17,
    ApiVersions = 18,
    CreateTopics = 19,
    DeleteTopics = 20,
    DeleteRecords = 21,
    InitProducerId = 22,
    OffsetForLeaderEpoch = 23,
    AddPartitionsToTxn = 24,
    AddOffsetsToTxn = 25,
    EndTxn = 26,
    WriteTxnMarkers = 27,
    TxnOffsetCommit = 28,
    DescribeAcls = 29,
    CreateAcls = 30,
    DeleteAcls = 31,
    DescribeConfigs = 32,
    AlterConfigs = 33,
    AlterReplicaLogDirs = 34,
    DescribeLogDirs = 35,
    SaslAuthenticate = 36,
    CreatePartitions = 37,
    CreateDelegationToken = 38,
    RenewDelegationToken = 39,
    ExpireDelegationToken = 40,
    DescribeDelegationToken = 41,
    DeleteGroups = 42,
    ElectLeaders = 43,
    IncrementalAlterConfigs = 44,
    AlterPartitionReassignments = 45,
    ListPartitionReassignments = 46,
    OffsetDelete = 47,
}

//
// Requests
//

#[derive(Debug)]
pub struct ApiVersionsRequest {
    pub header: RequestHeader,
}

#[derive(Debug)]
pub struct MetadataRequest {
    pub header: RequestHeader,
    pub topics: Vec<String>,
    pub allow_auto_topic_creation: bool,
    pub include_cluster_authorized_operations: bool,
    pub include_topic_authorized_operations: bool,
}

#[derive(Debug)]
pub struct TaggedField {}

//
// Responses
//

#[derive(Debug)]
pub struct ResponseHeader {
    pub correlation_id: u32,
}

#[derive(Debug)]
pub struct ApiVersion {
    pub api_key: u16,
    pub min_version: u16,
    pub max_version: u16,
}

#[derive(Debug)]
pub struct ApiVersionsResponse {
    pub header: ResponseHeader,
    pub error_code: u16,
    pub api_versions: Vec<ApiVersion>,
    pub throttle_time: u32,
}

#[derive(Debug)]
pub struct BrokerMetadata {
    pub node_id: u32,
    pub host: String,
    pub port: u32,
}

#[derive(Debug)]
pub struct PartitionMetadata {
    pub error: u16,
    pub id: u32,
    pub leader_id: u32,
    pub leader_epoch: u32,
    pub replicas: Vec<u32>,
    pub caught_up_replicas: Vec<u32>,
    pub offline_replicas: Vec<u32>,
}

#[derive(Debug)]
pub struct TopicMetadata {
    pub error: u16,
    pub name: String,
    pub is_internal: bool,
    pub partitions: Vec<PartitionMetadata>,
    pub topic_authorized_operations: u32,
}

#[derive(Debug)]
pub struct MetadataResponse {
    pub header: ResponseHeader,
    pub throttle_time: u32,
    pub brokers: Vec<BrokerMetadata>,
    pub cluster_id: String,
    pub controller_id: u32,
    pub topics: Vec<TopicMetadata>,
    pub cluster_authorized_operations: u32,
}

//
// Constructors
//

const NODE_ID: u32 = 1003;

impl TopicMetadata {
    pub fn new(name: String) -> Self {
        Self {
            error: 0,
            name,
            is_internal: false,
            partitions: vec![PartitionMetadata {
                error: 0,
                id: 0,
                leader_id: NODE_ID,
                leader_epoch: 0,
                replicas: vec![NODE_ID],
                caught_up_replicas: vec![NODE_ID],
                offline_replicas: Vec::<u32>::new(),
            }],
            topic_authorized_operations: 0,
        }
    }
}

impl MetadataResponse {
    pub fn new(req: &MetadataRequest) -> Self {
        // TODO: value customization
        let topics = req
            .topics
            .iter()
            .map(|t| TopicMetadata::new(t.clone()))
            .collect::<Vec<_>>();
        Self {
            header: ResponseHeader {
                correlation_id: req.header.correlation_id,
            },
            throttle_time: 0,
            // We only ever have a broker. That's the whole point of the project.
            brokers: vec![BrokerMetadata {
                node_id: NODE_ID,
                host: "localhost".to_string(),
                port: 9092,
            }],
            cluster_id: "0NHLrMQhQe2sWh6PvXAxcA".to_string(),
            controller_id: NODE_ID,
            topics: topics,
            cluster_authorized_operations: 0,
        }
    }
}

impl ApiVersionsResponse {
    // Create a new ApiVersionsResponse, field values extracted from a Wireshark analysis
    pub fn new(req: &ApiVersionsRequest) -> Self {
        Self {
            header: ResponseHeader {
                correlation_id: req.header.correlation_id,
            },
            error_code: 0,
            throttle_time: 0,
            api_versions: vec![
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::Produce).unwrap(),
                    min_version: 0,
                    max_version: 8,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::Fetch).unwrap(),
                    min_version: 0,
                    max_version: 11,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::Offsets).unwrap(),
                    min_version: 0,
                    max_version: 5,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::Metadata).unwrap(),
                    min_version: 0,
                    max_version: 9,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::LeaderAndIsr).unwrap(),
                    min_version: 0,
                    max_version: 4,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::StopReplica).unwrap(),
                    min_version: 0,
                    max_version: 2,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::UpdateMetadata).unwrap(),
                    min_version: 0,
                    max_version: 6,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::ControlledShutdown).unwrap(),
                    min_version: 0,
                    max_version: 3,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::OffsetCommit).unwrap(),
                    min_version: 0,
                    max_version: 8,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::OffsetFetch).unwrap(),
                    min_version: 0,
                    max_version: 6,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::GroupCoordinator).unwrap(),
                    min_version: 0,
                    max_version: 3,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::JoinGroup).unwrap(),
                    min_version: 0,
                    max_version: 6,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::Heartbeat).unwrap(),
                    min_version: 0,
                    max_version: 4,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::LeaveGroup).unwrap(),
                    min_version: 0,
                    max_version: 4,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::SyncGroup).unwrap(),
                    min_version: 0,
                    max_version: 4,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::DescribeGroups).unwrap(),
                    min_version: 0,
                    max_version: 5,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::ListGroups).unwrap(),
                    min_version: 0,
                    max_version: 3,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::SaslHandshake).unwrap(),
                    min_version: 0,
                    max_version: 1,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::ApiVersions).unwrap(),
                    min_version: 0,
                    max_version: 3,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::CreateTopics).unwrap(),
                    min_version: 0,
                    max_version: 5,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::DeleteTopics).unwrap(),
                    min_version: 0,
                    max_version: 4,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::DeleteRecords).unwrap(),
                    min_version: 0,
                    max_version: 1,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::InitProducerId).unwrap(),
                    min_version: 0,
                    max_version: 2,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::OffsetForLeaderEpoch).unwrap(),
                    min_version: 0,
                    max_version: 3,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::AddPartitionsToTxn).unwrap(),
                    min_version: 0,
                    max_version: 1,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::AddOffsetsToTxn).unwrap(),
                    min_version: 0,
                    max_version: 1,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::EndTxn).unwrap(),
                    min_version: 0,
                    max_version: 1,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::WriteTxnMarkers).unwrap(),
                    min_version: 0,
                    max_version: 0,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::TxnOffsetCommit).unwrap(),
                    min_version: 0,
                    max_version: 2,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::DescribeAcls).unwrap(),
                    min_version: 0,
                    max_version: 1,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::CreateAcls).unwrap(),
                    min_version: 0,
                    max_version: 1,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::DeleteAcls).unwrap(),
                    min_version: 0,
                    max_version: 1,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::DescribeConfigs).unwrap(),
                    min_version: 0,
                    max_version: 2,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::AlterConfigs).unwrap(),
                    min_version: 0,
                    max_version: 1,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::AlterReplicaLogDirs).unwrap(),
                    min_version: 0,
                    max_version: 1,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::DescribeLogDirs).unwrap(),
                    min_version: 0,
                    max_version: 1,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::SaslAuthenticate).unwrap(),
                    min_version: 0,
                    max_version: 1,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::CreatePartitions).unwrap(),
                    min_version: 0,
                    max_version: 1,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::CreateDelegationToken).unwrap(),
                    min_version: 0,
                    max_version: 2,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::RenewDelegationToken).unwrap(),
                    min_version: 0,
                    max_version: 1,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::ExpireDelegationToken).unwrap(),
                    min_version: 0,
                    max_version: 1,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::DescribeDelegationToken).unwrap(),
                    min_version: 0,
                    max_version: 1,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::DeleteGroups).unwrap(),
                    min_version: 0,
                    max_version: 2,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::ElectLeaders).unwrap(),
                    min_version: 0,
                    max_version: 2,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::IncrementalAlterConfigs).unwrap(),
                    min_version: 0,
                    max_version: 1,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::AlterPartitionReassignments).unwrap(),
                    min_version: 0,
                    max_version: 0,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::ListPartitionReassignments).unwrap(),
                    min_version: 0,
                    max_version: 0,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_u16(&ApiKey::OffsetDelete).unwrap(),
                    min_version: 0,
                    max_version: 0,
                },
            ],
        }
    }
}
