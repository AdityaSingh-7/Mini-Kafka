import { useEffect, useRef, useState, useCallback } from "react";
import type { BrokerState, BrokerEvent } from "../types";

// Auto-detect: use current host for WebSocket (works locally AND when deployed)
const wsProtocol = window.location.protocol === "https:" ? "wss:" : "ws:";
const WS_URL = `${wsProtocol}//${window.location.host}/ws`;
const API_BASE = `${window.location.protocol}//${window.location.host}`;

export function useWebSocket() {
  const [state, setState] = useState<BrokerState>({ topics: {}, groups: {} });
  const [events, setEvents] = useState<BrokerEvent[]>([]);
  const [connected, setConnected] = useState(false);
  const wsRef = useRef<WebSocket | null>(null);

  useEffect(() => {
    const connect = () => {
      const ws = new WebSocket(WS_URL);
      wsRef.current = ws;

      ws.onopen = () => setConnected(true);
      ws.onclose = () => {
        setConnected(false);
        // Reconnect after 2 seconds
        setTimeout(connect, 2000);
      };

      ws.onmessage = (msg) => {
        const event: BrokerEvent = JSON.parse(msg.data);

        if (event.type === "snapshot") {
          // Initial state from broker
          setState({
            topics: event.topics || {},
            groups: event.groups || {},
          });
        } else {
          // Live event — update state + add to event log
          setEvents((prev) => [event, ...prev].slice(0, 200)); // keep last 200
          applyEvent(setState, event);
        }
      };
    };

    connect();
    return () => wsRef.current?.close();
  }, []);

  const produce = useCallback(async (topic: string, key: string, value: string) => {
    await fetch(`${API_BASE}/api/produce`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ topic, key, value }),
    });
  }, []);

  const createTopic = useCallback(async (topic: string, partitions: number) => {
    await fetch(`${API_BASE}/api/create-topic`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ topic, partitions }),
    });
  }, []);

  const reset = useCallback(async () => {
    await fetch(`${API_BASE}/api/reset`, { method: "POST" });
    // Clear local state
    setState({ topics: {}, groups: {} });
    setEvents([]);
  }, []);

  return { state, events, connected, produce, createTopic, reset };
}

// Apply a live event to update the local state
function applyEvent(
  setState: React.Dispatch<React.SetStateAction<BrokerState>>,
  event: BrokerEvent
) {
  setState((prev) => {
    const next = { ...prev };

    switch (event.type) {
      case "create_topic": {
        const topic = event.data.topic as string;
        const numPartitions = event.data.partitions as number;
        const parts = Array.from({ length: numPartitions }, (_, i) => ({
          id: i,
          offset: 0,
        }));
        next.topics = { ...next.topics, [topic]: { partitions: parts } };
        break;
      }
      case "produce": {
        const topic = event.data.topic as string;
        const partition = event.data.partition as number;
        const offset = event.data.offset as number;
        if (next.topics[topic]) {
          const parts = [...next.topics[topic].partitions];
          if (parts[partition]) {
            parts[partition] = { ...parts[partition], offset: offset + 1 };
          }
          next.topics = { ...next.topics, [topic]: { partitions: parts } };
        } else {
          // Topic not in state yet — create it with this partition
          const parts = Array.from({ length: partition + 1 }, (_, i) => ({
            id: i,
            offset: i === partition ? offset + 1 : 0,
          }));
          next.topics = { ...next.topics, [topic]: { partitions: parts } };
        }
        break;
      }
      case "join": {
        const group = event.data.group as string;
        const member = event.data.member as string;
        const assignment = event.data.assignment as number[];
        const existing = next.groups[group] || { members: [], topic: event.data.topic || "" };
        const members = existing.members.filter((m) => m.id !== member);
        members.push({ id: member, assignment });
        next.groups = { ...next.groups, [group]: { ...existing, members } };
        break;
      }
      case "leave": {
        const group = event.data.group as string;
        const member = event.data.member as string;
        if (next.groups[group]) {
          const members = next.groups[group].members.filter((m) => m.id !== member);
          next.groups = { ...next.groups, [group]: { ...next.groups[group], members } };
        }
        break;
      }
    }

    return next;
  });
}
