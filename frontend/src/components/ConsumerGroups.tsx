import type { GroupInfo } from "../types";

interface Props {
  groups: Record<string, GroupInfo>;
}

export function ConsumerGroups({ groups }: Props) {
  return (
    <div className="panel">
      <h2>👥 Consumer Groups</h2>
      {Object.entries(groups).map(([groupName, group]) => (
        <div key={groupName} className="group-card">
          <h3>{groupName}</h3>
          <span className="group-topic">topic: {group.topic}</span>
          <div className="members-list">
            {group.members.map((member) => (
              <div key={member.id} className="member-row">
                <span className="member-status">●</span>
                <span className="member-id">{member.id}</span>
                <span className="member-assignment">
                  [{member.assignment?.join(", ") || "none"}]
                </span>
              </div>
            ))}
            {group.members.length === 0 && (
              <span className="empty">No active members</span>
            )}
          </div>
        </div>
      ))}
      {Object.keys(groups).length === 0 && (
        <p className="empty">No consumer groups active.</p>
      )}
    </div>
  );
}
