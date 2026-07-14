import type { TopicInfo, GroupInfo } from "../types";

interface Props {
  topics: Record<string, TopicInfo>;
  groups: Record<string, GroupInfo>;
}

const COLORS = ["#3b82f6", "#10b981", "#f59e0b", "#ef4444", "#8b5cf6", "#ec4899"];

export function PartitionBars({ topics, groups }: Props) {
  // Build a map of consumer positions: partition → [{group, member, offset}]
  // (For now we show assignment, not exact offset position)

  return (
    <div className="panel">
      <h2>📊 Partitions</h2>
      {Object.entries(topics).map(([topicName, topic]) => (
        <div key={topicName} className="topic-section">
          <h3>Topic: {topicName}</h3>
          {topic.partitions.map((part) => {
            const maxOffset = Math.max(...topic.partitions.map((p) => p.offset), 1);
            const fillPct = (part.offset / maxOffset) * 100;

            // Find consumers assigned to this partition
            const consumers = findConsumersForPartition(groups, part.id);

            return (
              <div key={part.id} className="partition-row">
                <span className="partition-label">P{part.id}</span>
                <div className="partition-bar">
                  <div
                    className="partition-fill"
                    style={{ width: `${fillPct}%` }}
                  />
                  <span className="partition-offset">{part.offset}</span>
                  {consumers.map((c, i) => (
                    <div
                      key={c.member}
                      className="consumer-marker"
                      style={{
                        backgroundColor: COLORS[i % COLORS.length],
                        right: `${100 - fillPct + 2}%`,
                      }}
                      title={`${c.group}/${c.member}`}
                    >
                      {c.member.slice(0, 3)}
                    </div>
                  ))}
                </div>
              </div>
            );
          })}
        </div>
      ))}
      {Object.keys(topics).length === 0 && (
        <p className="empty">No topics yet. Create one to get started.</p>
      )}
    </div>
  );
}

function findConsumersForPartition(
  groups: Record<string, GroupInfo>,
  partitionId: number
): { group: string; member: string }[] {
  const result: { group: string; member: string }[] = [];
  for (const [groupName, group] of Object.entries(groups)) {
    for (const member of group.members) {
      if (member.assignment?.includes(partitionId)) {
        result.push({ group: groupName, member: member.id });
      }
    }
  }
  return result;
}
