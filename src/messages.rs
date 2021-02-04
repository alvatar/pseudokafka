use num_derive::{FromPrimitive, ToPrimitive};
use num_traits::ToPrimitive;

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

// Requests

#[derive(Debug)]
pub struct RequestHeader<'a> {
    pub api_key: ApiKey,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<&'a str>,
}

// Responses

#[derive(Debug)]
pub struct ResponseHeader {
    // pub length: i32,
    pub correlation_id: i32,
}

#[derive(Debug)]
pub struct ApiVersion {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}

#[derive(Debug)]
pub struct ApiVersionsResponse {
    pub header: ResponseHeader,
    pub error_code: i16,
    pub api_versions: Vec<ApiVersion>,
    pub throttle_time: i32,
}

impl ApiVersionsResponse {
    // Create a new ApiVersionsResponse, field values extracted from a Wireshark analysis
    pub fn new(correlation_id: i32) -> Self {
        Self {
            header: ResponseHeader { correlation_id },
            error_code: 0,
            throttle_time: 0,
            api_versions: vec![
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::Produce).unwrap(),
                    min_version: 0,
                    max_version: 8,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::Fetch).unwrap(),
                    min_version: 0,
                    max_version: 11,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::Offsets).unwrap(),
                    min_version: 0,
                    max_version: 5,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::Metadata).unwrap(),
                    min_version: 0,
                    max_version: 9,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::LeaderAndIsr).unwrap(),
                    min_version: 0,
                    max_version: 4,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::StopReplica).unwrap(),
                    min_version: 0,
                    max_version: 2,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::UpdateMetadata).unwrap(),
                    min_version: 0,
                    max_version: 6,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::ControlledShutdown).unwrap(),
                    min_version: 0,
                    max_version: 3,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::OffsetCommit).unwrap(),
                    min_version: 0,
                    max_version: 8,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::OffsetFetch).unwrap(),
                    min_version: 0,
                    max_version: 6,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::GroupCoordinator).unwrap(),
                    min_version: 0,
                    max_version: 3,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::JoinGroup).unwrap(),
                    min_version: 0,
                    max_version: 6,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::Heartbeat).unwrap(),
                    min_version: 0,
                    max_version: 4,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::LeaveGroup).unwrap(),
                    min_version: 0,
                    max_version: 4,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::SyncGroup).unwrap(),
                    min_version: 0,
                    max_version: 4,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::DescribeGroups).unwrap(),
                    min_version: 0,
                    max_version: 5,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::ListGroups).unwrap(),
                    min_version: 0,
                    max_version: 3,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::SaslHandshake).unwrap(),
                    min_version: 0,
                    max_version: 1,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::ApiVersions).unwrap(),
                    min_version: 0,
                    max_version: 3,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::CreateTopics).unwrap(),
                    min_version: 0,
                    max_version: 5,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::DeleteTopics).unwrap(),
                    min_version: 0,
                    max_version: 4,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::DeleteRecords).unwrap(),
                    min_version: 0,
                    max_version: 1,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::InitProducerId).unwrap(),
                    min_version: 0,
                    max_version: 2,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::OffsetForLeaderEpoch).unwrap(),
                    min_version: 0,
                    max_version: 3,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::AddPartitionsToTxn).unwrap(),
                    min_version: 0,
                    max_version: 1,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::AddOffsetsToTxn).unwrap(),
                    min_version: 0,
                    max_version: 1,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::EndTxn).unwrap(),
                    min_version: 0,
                    max_version: 1,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::WriteTxnMarkers).unwrap(),
                    min_version: 0,
                    max_version: 0,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::TxnOffsetCommit).unwrap(),
                    min_version: 0,
                    max_version: 2,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::DescribeAcls).unwrap(),
                    min_version: 0,
                    max_version: 1,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::CreateAcls).unwrap(),
                    min_version: 0,
                    max_version: 1,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::DeleteAcls).unwrap(),
                    min_version: 0,
                    max_version: 1,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::DescribeConfigs).unwrap(),
                    min_version: 0,
                    max_version: 2,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::AlterConfigs).unwrap(),
                    min_version: 0,
                    max_version: 1,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::AlterReplicaLogDirs).unwrap(),
                    min_version: 0,
                    max_version: 1,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::DescribeLogDirs).unwrap(),
                    min_version: 0,
                    max_version: 1,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::SaslAuthenticate).unwrap(),
                    min_version: 0,
                    max_version: 1,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::CreatePartitions).unwrap(),
                    min_version: 0,
                    max_version: 1,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::CreateDelegationToken).unwrap(),
                    min_version: 0,
                    max_version: 2,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::RenewDelegationToken).unwrap(),
                    min_version: 0,
                    max_version: 1,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::ExpireDelegationToken).unwrap(),
                    min_version: 0,
                    max_version: 1,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::DescribeDelegationToken).unwrap(),
                    min_version: 0,
                    max_version: 1,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::DeleteGroups).unwrap(),
                    min_version: 0,
                    max_version: 2,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::ElectLeaders).unwrap(),
                    min_version: 0,
                    max_version: 2,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::IncrementalAlterConfigs).unwrap(),
                    min_version: 0,
                    max_version: 1,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::AlterPartitionReassignments).unwrap(),
                    min_version: 0,
                    max_version: 0,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::ListPartitionReassignments).unwrap(),
                    min_version: 0,
                    max_version: 0,
                },
                ApiVersion {
                    api_key: ToPrimitive::to_i16(&ApiKey::OffsetDelete).unwrap(),
                    min_version: 0,
                    max_version: 0,
                },
            ],
        }
    }
}
