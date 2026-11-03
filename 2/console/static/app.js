/* Bank Architecture Lab — request flow visualizer */
(() => {
  const reduced = window.matchMedia("(prefers-reduced-motion: reduce)");
  const $ = (sel) => document.querySelector(sel);
  const actionEl = $("#action");
  const toWrap = $("#toWrap");
  const amountWrap = $("#amountWrap");
  const liveStatus = $("#liveStatus");
  const sendBtn = $("#sendBtn");
  const balances = { A: new Map(), B: new Map() };
  let pollTimer;

  if (window.gsap && window.MotionPathPlugin) {
    gsap.registerPlugin(MotionPathPlugin);
  }

  function setLive(msg) {
    liveStatus.textContent = msg;
  }

  function toggleFields() {
    const action = actionEl.value;
    const needsAmount = action !== "balance";
    const needsTo = action === "transfer";
    amountWrap.classList.toggle("hidden", !needsAmount);
    toWrap.classList.toggle("hidden", !needsTo);
  }

  function nodeRect(id) {
    return document.querySelector(`#${id} .node-card`);
  }

  function clearActive() {
    document.querySelectorAll(".node-card.active, .flow-path.lit").forEach((el) => {
      el.classList.remove("active", "lit");
    });
  }

  function activate(ids) {
    ids.forEach((id) => {
      const card = nodeRect(id);
      if (card) card.classList.add("active");
    });
  }

  function lit(pathIds) {
    pathIds.forEach((id) => {
      const p = document.getElementById(id);
      if (p) p.classList.add("lit");
    });
  }

  function logEvent(text) {
    const li = document.createElement("li");
    li.textContent = `${new Date().toLocaleTimeString()} · ${text}`;
    const list = $("#eventLog");
    list.prepend(li);
    while (list.children.length > 12) list.lastChild.remove();
  }

  function renderTable(tbodyId, rows, shard) {
    const tbody = document.getElementById(tbodyId);
    const prev = balances[shard];
    tbody.innerHTML = "";
    rows.forEach((row) => {
      const old = prev.get(row.account_id);
      const delta = old == null ? 0 : row.balance - old;
      const tr = document.createElement("tr");
      if (delta !== 0) tr.classList.add("flash");
      const deltaHtml =
        delta === 0
          ? "—"
          : `<span class="${delta > 0 ? "delta-up" : "delta-down"}">${
              delta > 0 ? "+" : ""
            }${delta.toFixed(2)}</span>`;
      tr.innerHTML = `<td>${row.account_id}</td><td>${row.balance.toFixed(
        2
      )}</td><td>${deltaHtml}</td>`;
      tbody.appendChild(tr);
      prev.set(row.account_id, row.balance);
    });
  }

  function renderStatus(snap) {
    const row = $("#statusRow");
    const gwOk = !snap.gateway?.error;
    const coordOk = snap.coordinator?.status === "ok";
    const dbOk = !!snap.databases?.ok;
    row.innerHTML = `
      <span class="pill ${gwOk ? "ok" : "bad"}"><span class="dot" aria-hidden="true"></span>Gateway</span>
      <span class="pill ${coordOk ? "ok" : "bad"}"><span class="dot" aria-hidden="true"></span>Coordinator</span>
      <span class="pill ${dbOk ? "ok" : "bad"}"><span class="dot" aria-hidden="true"></span>Databases</span>
      <span class="pill"><span class="dot" aria-hidden="true"></span>Epoch ${
        snap.gateway?.epoch ?? "—"
      }</span>
    `;

    const shards = snap.gateway?.shards || {};
    ["A1", "A2", "B1", "B2"].forEach((rid) => {
      const card = nodeRect(`node-${rid.toLowerCase()}`);
      if (!card) return;
      card.classList.remove("ok", "down");
      const shard = rid[0];
      const reps = shards[shard]?.replicas || [];
      const hit = reps.find((r) => (r.url || "").includes(`bank-${rid.toLowerCase()}`));
      if (hit) card.classList.add(hit.healthy ? "ok" : "down");
    });
  }

  function renderTx(list) {
    const root = $("#txList");
    if (!list?.length) {
      root.innerHTML = `<div class="tx-item">No cross-shard transactions yet.</div>`;
      return;
    }
    root.innerHTML = list
      .map(
        (t) => `<div class="tx-item"><strong>${t.status}</strong> · ${t.from_account} → ${
          t.to_account
        } · ${t.amount} · <code>${t.transaction_id.slice(0, 8)}</code></div>`
      )
      .join("");
  }

  async function refreshSnapshot() {
    const res = await fetch("/api/snapshot");
    const snap = await res.json();
    renderStatus(snap);
    renderTable("tableA", snap.databases?.A || [], "A");
    renderTable("tableB", snap.databases?.B || [], "B");
    renderTx(snap.recent_2pc || []);
    return snap;
  }

  function replicaNodeId(replicaId) {
    if (!replicaId) return "node-a1";
    return `node-${String(replicaId).toLowerCase()}`;
  }

  function shardOfAccount(account) {
    const m = /^A(\d{4})$/i.exec(account || "");
    if (!m) return null;
    const n = Number(m[1]);
    if (n >= 1 && n <= 1000) return "A";
    if (n >= 1001 && n <= 2000) return "B";
    return null;
  }

  function buildHopPlan(payload, result) {
    const action = payload.action;
    const fromShard = shardOfAccount(payload.account);
    const toShard = shardOfAccount(payload.to_account);
    const replica = result.replica_id || result.gateway_replica;
    const hops = [{ node: "node-client", path: null }];

    hops.push({ node: "node-gateway", path: "path-client-gw" });

    if (action === "transfer" && fromShard && toShard && fromShard !== toShard) {
      hops.push({ node: "node-coordinator", path: "path-gw-coord" });
      hops.push({
        node: replicaNodeId(fromShard === "A" ? "A1" : "B1"),
        path: fromShard === "A" ? "path-coord-a" : "path-coord-b",
      });
      hops.push({
        node: fromShard === "A" ? "node-db-a" : "node-db-b",
        path: fromShard === "A" ? "path-a1-db" : "path-b1-db",
      });
      hops.push({
        node: replicaNodeId(toShard === "A" ? "A1" : "B1"),
        path: toShard === "A" ? "path-coord-a" : "path-coord-b",
      });
      hops.push({
        node: toShard === "A" ? "node-db-a" : "node-db-b",
        path: toShard === "A" ? "path-a1-db" : "path-b1-db",
      });
      hops.push({ node: "node-db-coord", path: null });
      return hops;
    }

    const rid = result.replica_id || (fromShard === "B" ? "B1" : "A1");
    const node = replicaNodeId(rid);
    const pathToRep =
      rid === "A1"
        ? "path-gw-a1"
        : rid === "A2"
          ? "path-gw-a2"
          : rid === "B1"
            ? "path-gw-b1"
            : "path-gw-b2";
    const pathToDb =
      rid.startsWith("A")
        ? rid === "A2"
          ? "path-a2-db"
          : "path-a1-db"
        : rid === "B2"
          ? "path-b2-db"
          : "path-b1-db";
    const db = rid.startsWith("A") ? "node-db-a" : "node-db-b";
    hops.push({ node, path: pathToRep });
    hops.push({ node: db, path: pathToDb });
    return hops;
  }

  async function animateHops(hops) {
    clearActive();
    const packet = $("#packet");
    if (reduced.matches || !window.gsap) {
      hops.forEach((h) => {
        activate([h.node]);
        if (h.path) lit([h.path]);
      });
      gsap.set(packet, { opacity: 0 });
      return;
    }

    gsap.set(packet, { opacity: 1 });
    const tl = gsap.timeline();
    for (const hop of hops) {
      activate([hop.node]);
      if (hop.path) {
        lit([hop.path]);
        tl.to(packet, {
          duration: 0.55,
          ease: "power2.inOut",
          motionPath: {
            path: `#${hop.path}`,
            align: `#${hop.path}`,
            autoRotate: false,
            alignOrigin: [0.5, 0.5],
          },
        });
      } else {
        const card = nodeRect(hop.node);
        if (card) {
          const b = card.getBBox();
          tl.to(packet, {
            duration: 0.35,
            ease: "power2.out",
            attr: { cx: b.x + b.width / 2, cy: b.y + b.height / 2 },
          });
        }
      }
      tl.to(nodeRect(hop.node), { scale: 1.03, transformOrigin: "50% 50%", duration: 0.12, yoyo: true, repeat: 1 }, "<");
    }
    tl.to(packet, { opacity: 0, duration: 0.2 });
    await tl.then();
  }

  async function sendRequest(payload) {
    sendBtn.disabled = true;
    setLive("Sending…");
    clearActive();
    activate(["node-client"]);
    try {
      const res = await fetch("/api/transaction", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const data = await res.json();
      const hops = buildHopPlan(payload, data);
      await animateHops(hops);

      const ok = data.status === "success" || data.decision === "COMMIT";
      const summary = ok
        ? `Success · ${payload.action} · replica ${data.replica_id || data.mode || "n/a"}`
        : `Result · ${data.status || res.status} · ${data.message || data.decision || ""}`;
      setLive(summary);
      logEvent(summary + (data.transaction_id ? ` · tx ${data.transaction_id.slice(0, 8)}` : ""));
      await refreshSnapshot();
    } catch (err) {
      setLive(`Error · ${err.message}`);
      logEvent(`Error · ${err.message}`);
    } finally {
      sendBtn.disabled = false;
    }
  }

  $("#txForm").addEventListener("submit", (e) => {
    e.preventDefault();
    const action = actionEl.value;
    const payload = {
      action,
      account: $("#account").value.trim(),
    };
    if (action !== "balance") payload.amount = Number($("#amount").value);
    if (action === "transfer") payload.to_account = $("#to_account").value.trim();
    sendRequest(payload);
  });

  actionEl.addEventListener("change", toggleFields);
  $("#refreshBtn").addEventListener("click", () => refreshSnapshot().then(() => setLive("Snapshot refreshed.")));

  document.querySelectorAll("[data-preset]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const kind = btn.getAttribute("data-preset");
      if (kind === "local") {
        actionEl.value = "transfer";
        $("#account").value = "A0100";
        $("#to_account").value = "A0200";
        $("#amount").value = "10";
      } else {
        actionEl.value = "transfer";
        $("#account").value = "A0500";
        $("#to_account").value = "A1500";
        $("#amount").value = "20";
      }
      toggleFields();
    });
  });

  function startPolling() {
    clearInterval(pollTimer);
    pollTimer = setInterval(() => {
      if (!document.hidden) refreshSnapshot().catch(() => {});
    }, 2500);
  }

  toggleFields();
  refreshSnapshot()
    .then(() => setLive("Live. Components healthy — send a request."))
    .catch((err) => setLive(`Snapshot failed: ${err.message}`));
  startPolling();
  document.addEventListener("visibilitychange", () => {
    if (document.hidden) clearInterval(pollTimer);
    else startPolling();
  });
})();
