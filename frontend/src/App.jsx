import { useEffect, useState, useCallback } from "react";
import { LineChart, Line, BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid } from "recharts";
import { Play, ShoppingCart, DollarSign, Users, Package, ShieldCheck, Loader2, TrendingUp, AlertTriangle } from "lucide-react";

const get = async (u) => {
  const r = await fetch(u);
  if (!r.ok) throw new Error((await r.json().catch(() => ({}))).detail || r.statusText);
  return r.json();
};
const num = (n) => Number(n).toLocaleString("en-US", { maximumFractionDigits: 0 });
const brl = (n) => "R$ " + num(n);
const sum = (o) => Object.values(o || {}).reduce((a, b) => a + (typeof b === "number" ? b : 0), 0);
const stageRecords = (s) =>
  s.stage === "silver" ? Object.values(s.detail).reduce((a, m) => a + m.valid_records, 0) : sum(s.detail);

function Card({ icon: Icon, label, value }) {
  return (
    <div className="rounded-lg border border-white/10 bg-[#1b1e24] p-4 hover:border-white/25 transition">
      <div className="flex items-center gap-2 text-xs text-zinc-400"><Icon size={14} />{label}</div>
      <div className="mt-2 text-2xl font-semibold">{value}</div>
    </div>
  );
}
function Panel({ title, children }) {
  return (
    <section className="rounded-lg border border-white/10 bg-[#1b1e24] p-4">
      <h2 className="mb-3 text-sm font-medium text-zinc-300">{title}</h2>{children}
    </section>
  );
}
const axis = { stroke: "#71717a", fontSize: 11 };
const tip = { contentStyle: { background: "#14161a", border: "1px solid #333", fontSize: 12 } };

export default function App() {
  const [d, setD] = useState(null);
  const [status, setStatus] = useState({ running: false, last_run: null });
  const [err, setErr] = useState("");
  const [toast, setToast] = useState("");

  const load = useCallback(async () => {
    try {
      const [summary, trend, cats, top, quality] = await Promise.all([
        get("/api/dashboard/summary"), get("/api/dashboard/revenue-trend"),
        get("/api/dashboard/category-revenue"), get("/api/dashboard/top-products?limit=10"),
        get("/api/data/quality"),
      ]);
      setD({ summary, trend, cats, top, quality }); setErr("");
    } catch (e) { setD(null); setErr(e.message); }
  }, []);
  const refreshStatus = useCallback(async () => {
    try { setStatus(await get("/api/pipeline/status")); } catch { setErr("Cannot reach the backend on port 8000."); }
  }, []);

  useEffect(() => { load(); refreshStatus(); }, [load, refreshStatus]);
  useEffect(() => {
    if (!status.running) return;
    const t = setInterval(async () => {
      const s = await get("/api/pipeline/status").catch(() => null);
      if (s) { setStatus(s); if (!s.running) { load(); setToast(`Pipeline ${s.last_run?.status}`); } }
    }, 2000);
    return () => clearInterval(t);
  }, [status.running, load]);
  useEffect(() => { if (toast) { const t = setTimeout(() => setToast(""), 4000); return () => clearTimeout(t); } }, [toast]);

  const run = async () => {
    const r = await fetch("/api/pipeline/run", { method: "POST" });
    if (r.status === 202) setStatus((s) => ({ ...s, running: true }));
    else setToast((await r.json().catch(() => ({}))).detail || "Could not start pipeline");
  };

  const q = d && Object.entries(d.quality);
  const score = q && (q.reduce((a, [, m]) => a + m.valid_records, 0) / q.reduce((a, [, m]) => a + m.input_records, 0)) * 100;
  const trend = d ? d.trend.filter((m) => m.orders >= 10) : [];
  const partial = d ? d.trend.filter((m) => m.orders < 10).map((m) => m.month) : [];
  const last = status.last_run;

  return (
    <div className="min-h-screen">
      <header className="flex items-center justify-between border-b border-white/10 px-6 py-3">
        <div className="flex items-center gap-2 font-semibold"><TrendingUp size={18} className="text-orange-400" />RetailFlow
          <span className="ml-2 rounded bg-white/10 px-2 py-0.5 text-xs font-normal text-zinc-400">local · Parquet</span></div>
        <div className="flex items-center gap-4 text-xs text-zinc-400">
          {last && <span>Last run: <b className={last.status === "SUCCESS" ? "text-emerald-400" : "text-red-400"}>{last.status}</b> · {last.end_time?.slice(0, 19).replace("T", " ")} UTC</span>}
          <button onClick={run} disabled={status.running}
            className="flex items-center gap-2 rounded-md bg-orange-500 px-3 py-1.5 text-sm font-medium text-black hover:bg-orange-400 disabled:opacity-50">
            {status.running ? <Loader2 size={14} className="animate-spin" /> : <Play size={14} />}
            {status.running ? "Running…" : "Run pipeline"}
          </button>
        </div>
      </header>

      <main className="mx-auto max-w-7xl space-y-4 p-6">
        {err && !d && (
          <div className="flex items-center gap-3 rounded-lg border border-amber-500/30 bg-amber-500/10 p-4 text-sm">
            <AlertTriangle size={16} />{err} — start the backend, then click “Run pipeline” if no data exists yet.
          </div>
        )}
        {!d && !err && <div className="h-24 animate-pulse rounded-lg bg-white/5" />}
        {d && (<>
          <div className="grid grid-cols-2 gap-3 md:grid-cols-6">
            <Card icon={ShoppingCart} label="Total orders" value={num(d.summary.total_orders)} />
            <Card icon={DollarSign} label="Total revenue" value={brl(d.summary.total_revenue)} />
            <Card icon={Users} label="Customers" value={num(d.summary.total_customers)} />
            <Card icon={Package} label="Products" value={num(d.summary.total_products)} />
            <Card icon={DollarSign} label="Avg order value" value={"R$ " + d.summary.avg_order_value.toFixed(2)} />
            <Card icon={ShieldCheck} label="Data quality" value={score.toFixed(2) + "%"} />
          </div>
          {partial.length > 0 && <p className="text-xs text-zinc-500">Charts exclude partial months with fewer than 10 orders: {partial.join(", ")} (dataset starts/ends mid-stream).</p>}
          <div className="grid gap-4 lg:grid-cols-2">
            <Panel title="Monthly revenue (non-cancelled orders)">
              <ResponsiveContainer width="100%" height={260}><LineChart data={trend}>
                <CartesianGrid stroke="#2a2d34" /><XAxis dataKey="month" {...axis} /><YAxis {...axis} /><Tooltip {...tip} />
                <Line dataKey="revenue" stroke="#fb923c" dot={false} strokeWidth={2} /></LineChart></ResponsiveContainer>
            </Panel>
            <Panel title="Orders per month">
              <ResponsiveContainer width="100%" height={260}><BarChart data={trend}>
                <CartesianGrid stroke="#2a2d34" /><XAxis dataKey="month" {...axis} /><YAxis {...axis} /><Tooltip {...tip} />
                <Bar dataKey="orders" fill="#38bdf8" /></BarChart></ResponsiveContainer>
            </Panel>
            <Panel title="Top categories by revenue">
              <ResponsiveContainer width="100%" height={300}><BarChart data={d.cats.slice(0, 10)} layout="vertical" margin={{ left: 60 }}>
                <CartesianGrid stroke="#2a2d34" /><XAxis type="number" {...axis} /><YAxis type="category" dataKey="category" {...axis} width={120} /><Tooltip {...tip} />
                <Bar dataKey="revenue" fill="#a78bfa" /></BarChart></ResponsiveContainer>
            </Panel>
            <Panel title="Top 10 products by units sold">
              <table className="w-full text-xs"><thead className="text-left text-zinc-500"><tr><th>Product</th><th>Category</th><th className="text-right">Units</th><th className="text-right">Revenue</th></tr></thead>
                <tbody>{d.top.map((p) => (<tr key={p.product_id} className="border-t border-white/5">
                  <td className="py-1.5 font-mono">{p.product_id.slice(0, 8)}…</td><td>{p.category}</td>
                  <td className="text-right">{num(p.units_sold)}</td><td className="text-right">{brl(p.revenue)}</td></tr>))}</tbody></table>
            </Panel>
          </div>
          <Panel title="Pipeline stages (last run)">
            <table className="w-full text-xs"><thead className="text-left text-zinc-500"><tr><th>Stage</th><th>Status</th><th className="text-right">Records</th><th className="text-right">Duration</th></tr></thead>
              <tbody>{(last?.stages || []).map((s) => (<tr key={s.stage} className="border-t border-white/5">
                <td className="py-1.5 capitalize">{s.stage}</td><td className="text-emerald-400">{s.status}</td>
                <td className="text-right">{num(stageRecords(s))}</td><td className="text-right">{s.duration_s}s</td></tr>))}</tbody></table>
          </Panel>
          <Panel title="Data quality by table (Silver)">
            <table className="w-full text-xs"><thead className="text-left text-zinc-500"><tr><th>Table</th><th className="text-right">Input</th><th className="text-right">Valid</th><th className="text-right">Rejected</th><th className="text-right">Duplicates</th><th className="text-right">Null</th></tr></thead>
              <tbody>{q.map(([t, m]) => (<tr key={t} className="border-t border-white/5"><td className="py-1.5">{t}</td>
                <td className="text-right">{num(m.input_records)}</td><td className="text-right">{num(m.valid_records)}</td>
                <td className="text-right">{num(m.rejected_records)}</td><td className="text-right">{num(m.duplicate_records)}</td>
                <td className="text-right">{num(m.null_records)}</td></tr>))}</tbody></table>
          </Panel>
          <RejectedPanel quality={d.quality} />
        </>)}
      </main>
      {toast && <div className="fixed bottom-4 right-4 rounded-md border border-white/10 bg-[#1b1e24] px-4 py-2 text-sm shadow-lg">{toast}</div>}
    </div>
  );
}

function RejectedPanel({ quality }) {
  const tables = Object.entries(quality).filter(([, m]) => m.rejected_records > 0).map(([t]) => t);
  const [sel, setSel] = useState("");
  const t = tables.includes(sel) ? sel : tables[0] || "";
  const [r, setR] = useState(null);
  useEffect(() => { if (t) get(`/api/data/rejected?table=${t}&limit=8`).then(setR).catch(() => setR(null)); }, [t]);
  if (!tables.length) return <Panel title="Rejected records"><p className="text-xs text-zinc-500">No rejected records in the last run.</p></Panel>;
  const cols = r && r.rows[0] ? Object.keys(r.rows[0]).filter((c) => !c.startsWith("_") || c === "_reject_reason").slice(0, 5).concat([]) : [];
  return (
    <Panel title="Rejected records and reasons">
      <div className="mb-3 flex flex-wrap items-center gap-2 text-xs">
        {tables.map((x) => (<button key={x} onClick={() => setSel(x)}
          className={`rounded px-2 py-1 ${x === t ? "bg-orange-500 text-black" : "bg-white/10"}`}>{x}</button>))}
        {r && Object.entries(r.by_reason).map(([k, v]) => (<span key={k} className="rounded bg-red-500/15 px-2 py-1 text-red-300">{k}: {num(v)}</span>))}
      </div>
      {r && (<div className="overflow-x-auto"><table className="w-full text-xs">
        <thead className="text-left text-zinc-500"><tr>{cols.map((c) => <th key={c} className="pr-3">{c}</th>)}</tr></thead>
        <tbody>{r.rows.map((row, i) => (<tr key={i} className="border-t border-white/5">
          {cols.map((c) => <td key={c} className="py-1.5 pr-3 whitespace-nowrap">{String(row[c] ?? "null").slice(0, 28)}</td>)}</tr>))}</tbody>
      </table><p className="mt-2 text-xs text-zinc-500">Showing {r.rows.length} of {num(r.total)} rejected rows.</p></div>)}
    </Panel>
  );
}
