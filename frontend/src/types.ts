export interface PartitionInfo {
  id: number;
  offset: number;
}

export interface TopicInfo {
  partitions: PartitionInfo[];
}

export interface MemberInfo {
  id: string;
  assignment: number[];
}

export interface GroupInfo {
  members: MemberInfo[];
  topic: string;
}

export interface BrokerState {
  topics: Record<string, TopicInfo>;
  groups: Record<string, GroupInfo>;
}

export interface BrokerEvent {
  type: "produce" | "consume" | "commit" | "join" | "leave" | "rebalance" | "snapshot";
  timestamp: number;
  data: Record<string, any>;
  // Snapshot fields (only on type=snapshot)
  topics?: Record<string, TopicInfo>;
  groups?: Record<string, GroupInfo>;
}
