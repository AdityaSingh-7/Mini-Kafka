import type { BrokerEvent } from "../types";

interface Props {
  events: BrokerEvent[];
}

const EVENT_COLORS: Record<string, string> = {
  produce: "#10b981",
  consume: "#3b82f6",
  commit: "#6b7280",
  join: "#f59e0b",
  leave: "#ef4444",
  rebalance: "#8b5cf6",
};

export function EventLog({ events }: Props) {
  return (
    <div className="panel event-log-panel">
      <h2>📋 Live Events</h2>
      <div className="event-list">
        {events.map((event, i) => (
          <div key={i} className="event-row">
            <span
              className="event-badge"
              style={{ backgroundColor: EVENT_COLORS[event.type] || "#666" }}
            >
              {event.type.toUpperCase()}
            </span>
            <span className="event-detail">{formatEventDetail(event)}</span>
            <span className="event-time">
              {new Date(event.timestamp).toLocaleTimeString()}
            </span>
          </div>
        ))}
        {events.length === 0 && (
          <p className="empty">Waiting for events...</p>
        )}
      </div>
    </div>
  );
}

function formatEventDetail(event: BrokerEvent): string {
  const d = event.data;
  switch (event.type) {
    case "produce":
      return `topic=${d.topic} partition=${d.partition} offset=${d.offset} key=${d.key || "∅"}`;
    case "consume":
      return `topic=${d.topic} partition=${d.partition} offset=${d.offset}`;
    case "commit":
      return `group=${d.group} partition=${d.partition} offset=${d.offset}`;
    case "join":
      return `group=${d.group} member=${d.member} → [${d.assignment}]`;
    case "leave":
      return `group=${d.group} member=${d.member}`;
    case "rebalance":
      return `group=${d.group}`;
    default:
      return JSON.stringify(d);
  }
}
