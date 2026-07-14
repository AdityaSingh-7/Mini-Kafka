import { useWebSocket } from "./hooks/useWebSocket";
import { PartitionBars } from "./components/PartitionBars";
import { ConsumerGroups } from "./components/ConsumerGroups";
import { EventLog } from "./components/EventLog";
import { Controls } from "./components/Controls";
import "./App.css";

function App() {
  const { state, events, connected, produce, createTopic, reset } = useWebSocket();

  return (
    <div className="app">
      <header className="app-header">
        <h1>Mini Kafka — Live Dashboard</h1>
        <span className="subtitle">Real-time message broker visualization</span>
      </header>

      <div className="dashboard">
        <div className="left-column">
          <Controls
            onProduce={produce}
            onCreateTopic={createTopic}
            onReset={reset}
            connected={connected}
          />
          <ConsumerGroups groups={state.groups} />
        </div>

        <div className="center-column">
          <PartitionBars topics={state.topics} groups={state.groups} />
        </div>

        <div className="right-column">
          <EventLog events={events} />
        </div>
      </div>
    </div>
  );
}

export default App;
