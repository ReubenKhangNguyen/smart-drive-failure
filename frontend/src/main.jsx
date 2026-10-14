import React, { useEffect, useState } from "react";
import { createRoot } from "react-dom/client";
import "./styles.css";

const API = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";
const PAGES = [
  ["overview", "Overview"],
  ["analytics", "Data Analytics"],
  ["smart", "SMART Analysis"],
  ["prediction", "Failure Prediction"],
  ["cluster", "Cluster Performance"],
];

function useApi(path) {
  const [state, setState] = useState({ loading: true, data: null, error: null });
  useEffect(() => {
    let active = true;
    setState({ loading: true, data: null, error: null });
    fetch(`${API}${path}`)
      .then(async (response) => {
        const body = await response.json();
        if (!response.ok) throw new Error(body.detail || "The API request failed.");
        return body;
      })
      .then((data) => active && setState({ loading: false, data, error: null }))
      .catch((error) => active && setState({ loading: false, data: null, error: error.message }));
    return () => { active = false; };
  }, [path]);
  return state;
}

function JsonPanel({ title, endpoint }) {
  const { loading, data, error } = useApi(endpoint);
  return <section className="panel"><h2>{title}</h2>{loading && <p>Loading Gold export…</p>}{error && <p className="error">{error}</p>}{data && <pre>{JSON.stringify(data, null, 2)}</pre>}</section>;
}

function Overview() { return <JsonPanel title="System overview" endpoint="/api/overview" />; }
function Analytics() { return <><JsonPanel title="Failures by drive model" endpoint="/api/analytics/failure-by-model" /><JsonPanel title="Failure trend" endpoint="/api/analytics/failure-over-time" /></>; }
function Smart() { return <JsonPanel title="SMART metric comparison" endpoint="/api/smart/comparison" />; }
function Cluster() { return <JsonPanel title="CSV, Parquet and Spark benchmark" endpoint="/api/benchmarks" />; }

function Prediction() {
  const [serial, setSerial] = useState("");
  const [submitted, setSubmitted] = useState("");
  const result = useApi(submitted ? `/api/predictions/${encodeURIComponent(submitted)}` : "/api/drives?limit=20");
  return <section className="panel"><h2>Failure prediction</h2><form onSubmit={(event) => { event.preventDefault(); setSubmitted(serial); }}><input value={serial} onChange={(event) => setSerial(event.target.value)} placeholder="Drive serial number" required /><button>Find prediction</button></form>{result.loading && <p>Loading…</p>}{result.error && <p className="error">{result.error}</p>}{result.data && <pre>{JSON.stringify(result.data, null, 2)}</pre>}</section>;
}

function App() {
  const [page, setPage] = useState("overview");
  const content = { overview: <Overview />, analytics: <Analytics />, smart: <Smart />, prediction: <Prediction />, cluster: <Cluster /> }[page];
  return <main><header><div><p className="eyebrow">BIG DATA · BACKBLAZE DRIVE STATS</p><h1>SMART Drive Failure</h1></div><span className="status">React + FastAPI</span></header><nav>{PAGES.map(([id, label]) => <button key={id} className={page === id ? "active" : ""} onClick={() => setPage(id)}>{label}</button>)}</nav>{content}</main>;
}

createRoot(document.getElementById("root")).render(<App />);
