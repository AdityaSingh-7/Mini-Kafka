import { useState } from "react";

interface Props {
  onProduce: (topic: string, key: string, value: string) => void;
  onCreateTopic: (topic: string, partitions: number) => void;
  onReset: () => void;
  connected: boolean;
}

export function Controls({ onProduce, onCreateTopic, onReset, connected }: Props) {
  const [topic, setTopic] = useState("orders");
  const [key, setKey] = useState("customer-1");
  const [value, setValue] = useState("order placed");
  const [newTopic, setNewTopic] = useState("orders");
  const [partitions, setPartitions] = useState(3);

  return (
    <div className="panel controls-panel">
      <h2>🎮 Controls</h2>

      <div className="connection-status">
        <span className={`status-dot ${connected ? "connected" : "disconnected"}`}>●</span>
        {connected ? "Connected" : "Disconnected (reconnecting...)"}
      </div>

      <div className="control-section">
        <h3>Create Topic</h3>
        <div className="control-row">
          <input
            placeholder="Topic name"
            value={newTopic}
            onChange={(e) => setNewTopic(e.target.value)}
          />
          <input
            type="number"
            min={1}
            max={10}
            value={partitions}
            onChange={(e) => setPartitions(Number(e.target.value))}
            style={{ width: "60px" }}
          />
          <button onClick={() => onCreateTopic(newTopic, partitions)}>Create</button>
        </div>
      </div>

      <div className="control-section">
        <h3>Produce Message</h3>
        <div className="control-row">
          <input
            placeholder="Topic"
            value={topic}
            onChange={(e) => setTopic(e.target.value)}
          />
          <input
            placeholder="Key"
            value={key}
            onChange={(e) => setKey(e.target.value)}
          />
          <input
            placeholder="Value"
            value={value}
            onChange={(e) => setValue(e.target.value)}
          />
          <button onClick={() => onProduce(topic, key, value)}>Produce</button>
        </div>
        <button
          className="burst-btn"
          onClick={() => {
            for (let i = 0; i < 10; i++) {
              const randomKey = `user-${Math.floor(Math.random() * 1000)}`;
              onProduce(topic, randomKey, `${value} #${i}`);
            }
          }}
        >
          Burst (10 messages, random keys)
        </button>
      </div>

      <div className="control-section">
        <h3>Reset</h3>
        <button className="reset-btn" onClick={onReset}>
          🗑️ Reset Broker (delete all data, restart)
        </button>
      </div>
    </div>
  );
}
