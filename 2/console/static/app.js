/* Bank Architecture Lab — request flow visualizer + Docker status */
(() => {
  const reduced = window.matchMedia("(prefers-reduced-motion: reduce)");
  const $ = (sel) => document.querySelector(sel);
  const actionEl = $("#action");
  const toWrap = $("#toWrap");
  const amountWrap = $("#amountWrap");
  const liveStatus = $("#liveStatus");
  const sendBtn = $("#sendBtn");
  const balances = { A: new Map(), B: new Map() };
  const coordTxPrev = new Map(); // tx_id -> status|updated_at
  const coordPartPrev = new Map(); // tx|shard -> vote|ack
  let pollTimer;
  let dockerPollTimer;
  let activeTab = "lab";

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
    document.querySelectorAll(".node-card.active, .node-card.failed-hop, .flow-path.lit").forEach((el) => {
      el.classList.remove("active", "failed-hop", "lit");
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

  function shortTx(id) {
    return String(id || "").slice(0, 8);
  }

  function fmtTime(iso) {
    if (!iso) return "—";
    try {
      return new Date(iso).toLocaleTimeString();
    } catch {
      return iso;
    }
  }

  function renderCoordDb(coord) {
    const meta = $("#coordDbMeta");
    const counts = $("#coordDbCounts");
    const txBody = $("#coordTxTable");
    const partBody = $("#coordPartTable");
    if (!meta || !txBody || !partBody) return;

    if (!coord || coord.ok === false) {
      meta.textContent = coord?.error ? `error · ${coord.error}` : "unavailable";
      counts.innerHTML = "";
      txBody.innerHTML = `<tr><td colspan="6">Coord DB unavailable.</td></tr>`;
      partBody.innerHTML = `<tr><td colspan="6">—</td></tr>`;
      return;
    }

    meta.textContent = `${coord.total ?? 0} total transactions`;
    const by = coord.by_status || {};
    const pills = Object.keys(by)
      .sort()
      .map(
        (k) =>
          `<span class="pill"><span class="dot" aria-hidden="true"></span>${escapeHtml(
            k
          )} ${by[k]}</span>`
      );
    counts.innerHTML =
      pills.join("") ||
      `<span class="pill"><span class="dot" aria-hidden="true"></span>empty</span>`;

    const txs = coord.transactions || [];
    if (!txs.length) {
      txBody.innerHTML = `<tr><td colspan="6">No distributed transactions yet. Run a cross-shard transfer.</td></tr>`;
    } else {
      txBody.innerHTML = txs
        .map((t) => {
          const key = t.transaction_id;
          const sig = `${t.status}|${t.updated_at}`;
          const prev = coordTxPrev.get(key);
          const flash = prev != null && prev !== sig;
          coordTxPrev.set(key, sig);
          const st = String(t.status || "").toLowerCase();
          return `<tr class="${flash ? "flash" : ""}">
            <td class="mono">${escapeHtml(shortTx(t.transaction_id))}</td>
            <td>${escapeHtml(t.from_account)} → ${escapeHtml(t.to_account)}</td>
            <td>${Number(t.amount).toFixed(2)}</td>
            <td>${escapeHtml(t.from_shard)} → ${escapeHtml(t.to_shard)}</td>
            <td><span class="status-chip ${escapeHtml(st)}">${escapeHtml(
              t.status
            )}</span></td>
            <td>${escapeHtml(fmtTime(t.updated_at))}</td>
          </tr>`;
        })
        .join("");
    }

    const parts = coord.participants || [];
    if (!parts.length) {
      partBody.innerHTML = `<tr><td colspan="6">No participants yet.</td></tr>`;
    } else {
      // Keep participant rows aligned with currently shown txs.
      const shown = new Set(txs.map((t) => t.transaction_id));
      const filtered = parts.filter((p) => shown.has(p.transaction_id));
      partBody.innerHTML = filtered
        .map((p) => {
          const key = `${p.transaction_id}|${p.shard_id}`;
          const sig = `${p.vote || ""}|${p.ack || ""}`;
          const prev = coordPartPrev.get(key);
          const flash = prev != null && prev !== sig;
          coordPartPrev.set(key, sig);
          const vote = p.vote || "—";
          const voteClass =
            String(vote).toUpperCase() === "YES"
              ? "vote-yes"
              : String(vote).toUpperCase() === "NO"
                ? "vote-no"
                : "";
          return `<tr class="${flash ? "flash" : ""}">
            <td class="mono">${escapeHtml(shortTx(p.transaction_id))}</td>
            <td>${escapeHtml(p.shard_id)}</td>
            <td>${escapeHtml(p.operation)}</td>
            <td>${escapeHtml(p.account)}</td>
            <td class="${voteClass}">${escapeHtml(vote)}</td>
            <td>${escapeHtml(p.ack || "—")}</td>
          </tr>`;
        })
        .join("");
    }
  }

  async function refreshSnapshot() {
    const res = await fetch("/api/snapshot");
    const snap = await res.json();
    renderStatus(snap);
    renderTable("tableA", snap.databases?.A || [], "A");
    renderTable("tableB", snap.databases?.B || [], "B");
    renderCoordDb(snap.coord_db || { transactions: snap.recent_2pc || [], ok: true });
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

  function replicaPathIds(rid) {
    const pathToRep =
      rid === "A1"
        ? "path-gw-a1"
        : rid === "A2"
          ? "path-gw-a2"
          : rid === "B1"
            ? "path-gw-b1"
            : "path-gw-b2";
    const pathToDb = rid.startsWith("A")
      ? rid === "A2"
        ? "path-a2-db"
        : "path-a1-db"
      : rid === "B2"
        ? "path-b2-db"
        : "path-b1-db";
    const db = rid.startsWith("A") ? "node-db-a" : "node-db-b";
    return { pathToRep, pathToDb, db };
  }

  function replicaIdFromResult(result, fallbackShard) {
    if (result.replica_id) return String(result.replica_id).toUpperCase();
    const url = result.gateway_replica || result.replica || "";
    const m = /bank-([ab][12])/i.exec(url);
    if (m) return m[1].toUpperCase();
    return fallbackShard === "B" ? "B1" : "A1";
  }

  function markFailed(hops, nodeId) {
    const hop = hops.find((h) => h.node === nodeId) || hops[hops.length - 1];
    if (hop) hop.failed = true;
    return hops;
  }

  function crossReplica(shard) {
    return shard === "A" ? "A1" : "B1";
  }

  function pushCoordDb(hops, label, fromCoordinator = true) {
    hops.push({
      node: "node-db-coord",
      path: fromCoordinator ? "path-coord-db" : "path-coord-db",
      label,
      kind: "coord-db",
    });
  }

  function pushBackToCoordinator(hops, label) {
    hops.push({
      node: "node-coordinator",
      path: "path-db-coord-back",
      label,
    });
  }

  function expandRouteStep(step, hops, ctx) {
    const { fromShard, toShard, failAt, decision } = ctx;
    const fromRid = crossReplica(fromShard);
    const toRid = crossReplica(toShard);
    const decide = decision === "ABORT" ? "ABORT" : "COMMIT";

    switch (step) {
      case "client":
        hops.push({ node: "node-client", path: null, label: "Client request" });
        break;
      case "gateway":
        hops.push({
          node: "node-gateway",
          path: "path-client-gw",
          label: "Gateway routes request",
          failed: failAt === "gateway",
        });
        break;
      case "coordinator":
        hops.push({
          node: "node-coordinator",
          path: "path-gw-coord",
          label: "Coordinator starts 2PC",
          failed: failAt === "coordinator",
        });
        break;
      case "db_coord_create":
        pushCoordDb(hops, "Coord DB · create tx (INITIATED)");
        pushBackToCoordinator(hops, "Coordinator · begin PREPARE");
        break;
      case "replica_from":
        hops.push({
          node: replicaNodeId(fromRid),
          path: fromShard === "A" ? "path-coord-a" : "path-coord-b",
          label: `PREPARE debit on shard ${fromShard}`,
        });
        break;
      case "db_from":
        hops.push({
          node: fromShard === "A" ? "node-db-a" : "node-db-b",
          path: fromShard === "A" ? "path-a1-db" : "path-b1-db",
          label: `Shard ${fromShard} DB · PREPARE TRANSACTION`,
        });
        break;
      case "db_coord_vote_from":
        hops.push({
          node: "node-coordinator",
          path: null,
          label: "Coordinator collects vote",
        });
        pushCoordDb(hops, `Coord DB · store vote (${fromShard})`);
        pushBackToCoordinator(hops, "Coordinator continues PREPARE");
        break;
      case "replica_to":
        hops.push({
          node: replicaNodeId(toRid),
          path: toShard === "A" ? "path-coord-a" : "path-coord-b",
          label: `PREPARE credit on shard ${toShard}`,
        });
        break;
      case "db_to":
        hops.push({
          node: toShard === "A" ? "node-db-a" : "node-db-b",
          path: toShard === "A" ? "path-a1-db" : "path-b1-db",
          label: `Shard ${toShard} DB · PREPARE TRANSACTION`,
        });
        break;
      case "db_coord_vote_to":
        hops.push({
          node: "node-coordinator",
          path: null,
          label: "Coordinator collects vote",
        });
        pushCoordDb(hops, `Coord DB · store vote (${toShard})`);
        break;
      case "db_coord_decision":
        pushBackToCoordinator(hops, "Coordinator decides");
        pushCoordDb(hops, `Coord DB · durable ${decide}_DECIDED`);
        pushBackToCoordinator(hops, `Coordinator sends ${decide}`);
        break;
      case "replica_from_decide":
        hops.push({
          node: replicaNodeId(fromRid),
          path: fromShard === "A" ? "path-coord-a" : "path-coord-b",
          label: `${decide} on shard ${fromShard}`,
        });
        break;
      case "replica_to_decide":
        hops.push({
          node: replicaNodeId(toRid),
          path: toShard === "A" ? "path-coord-a" : "path-coord-b",
          label: `${decide} on shard ${toShard}`,
        });
        break;
      case "db_coord_complete":
        hops.push({
          node: "node-coordinator",
          path: null,
          label: "Coordinator records acks",
        });
        pushCoordDb(hops, "Coord DB · COMPLETED");
        break;
      case "db_coord_pending":
        pushCoordDb(hops, "Coord DB · COMMIT_DECIDED (waiting acks)");
        break;
      case "replica": {
        const rid = replicaIdFromResult(ctx.result, fromShard);
        const { pathToRep } = replicaPathIds(rid);
        hops.push({
          node: replicaNodeId(rid),
          path: pathToRep,
          label: `Replica ${rid}`,
          failed: failAt === "replica",
        });
        break;
      }
      case "db": {
        const rid = replicaIdFromResult(ctx.result, fromShard);
        const { pathToDb, db } = replicaPathIds(rid);
        hops.push({ node: db, path: pathToDb, label: "Shard database" });
        break;
      }
      default:
        break;
    }
  }

  function buildHopPlan(payload, result) {
    const action = payload.action;
    const fromShard = shardOfAccount(payload.account);
    const toShard = shardOfAccount(payload.to_account);
    const route = Array.isArray(result._route) ? result._route : null;
    const failAt = result._fail_at || null;
    const decision = String(result.decision || "").toUpperCase();
    const hops = [];

    const wantsCross =
      action === "transfer" &&
      fromShard &&
      toShard &&
      fromShard !== toShard;

    const ctx = { fromShard, toShard, failAt, decision, result };

    if (route) {
      for (const step of route) {
        expandRouteStep(step, hops, ctx);
        if (
          (step === "gateway" && failAt === "gateway") ||
          (step === "coordinator" && failAt === "coordinator") ||
          (step === "replica" && failAt === "replica")
        ) {
          break;
        }
      }
      if (!hops.length) hops.push({ node: "node-client", path: null });
      return hops;
    }

    // Fallback if older response without hints.
    hops.push({ node: "node-client", path: null, label: "Client request" });
    hops.push({ node: "node-gateway", path: "path-client-gw", label: "Gateway" });
    const msg = String(result.message || "").toLowerCase();
    if (msg.includes("gateway unreachable")) {
      return markFailed(hops, "node-gateway");
    }
    if (wantsCross) {
      hops.push({ node: "node-coordinator", path: "path-gw-coord", label: "Coordinator" });
      if (msg.includes("coordinator unreachable")) {
        return markFailed(hops, "node-coordinator");
      }
    }
    return hops;
  }

  async function animateHops(hops) {
    clearActive();
    document.querySelectorAll(".node-card.failed-hop").forEach((el) => {
      el.classList.remove("failed-hop");
    });
    const packet = $("#packet");
    if (reduced.matches || !window.gsap) {
      hops.forEach((h) => {
        activate([h.node]);
        if (h.path) lit([h.path]);
        if (h.label) setLive(h.label);
        if (h.failed) {
          const card = nodeRect(h.node);
          if (card) card.classList.add("failed-hop");
        }
      });
      gsap.set(packet, { opacity: 0 });
      return;
    }

    gsap.set(packet, { opacity: 1 });
    const tl = gsap.timeline();
    for (const hop of hops) {
      if (hop.label) {
        tl.add(() => setLive(hop.label));
      }
      activate([hop.node]);
      if (hop.path) {
        lit([hop.path]);
        tl.to(packet, {
          duration: hop.kind === "coord-db" ? 0.45 : 0.5,
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
            duration: 0.3,
            ease: "power2.out",
            attr: { cx: b.x + b.width / 2, cy: b.y + b.height / 2 },
          });
        }
      }
      const card = nodeRect(hop.node);
      if (card) {
        if (hop.failed) {
          tl.add(() => card.classList.add("failed-hop"));
          tl.to(card, {
            scale: 1.05,
            transformOrigin: "50% 50%",
            duration: 0.16,
            yoyo: true,
            repeat: 3,
          }, "<");
        } else {
          tl.to(
            card,
            {
              scale: hop.kind === "coord-db" ? 1.06 : 1.03,
              transformOrigin: "50% 50%",
              duration: 0.12,
              yoyo: true,
              repeat: hop.kind === "coord-db" ? 2 : 1,
            },
            "<"
          );
        }
      }
    }
    tl.to(packet, { opacity: hopFailedOpacity(hops), duration: 0.25 });
    await tl.then();
  }

  function hopFailedOpacity(hops) {
    return hops.some((h) => h.failed) ? 0.15 : 0;
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
      let data;
      try {
        data = await res.json();
      } catch {
        data = { status: "error", message: `bad response (${res.status})`, _fail_at: "gateway", _route: ["client", "gateway"] };
      }
      if (!data._route) {
        // Network-level failure before JSON hints (should be rare with console proxy).
        if (!res.ok && /gateway unreachable/i.test(data.message || "")) {
          data._fail_at = "gateway";
          data._route = ["client", "gateway"];
        }
      }

      const hops = buildHopPlan(payload, data);
      await animateHops(hops);

      const ok = data._ok === true || data.status === "success" || data.decision === "COMMIT";
      const failLabel = data._fail_at ? `stopped at ${data._fail_at}` : null;
      const summary = ok
        ? `Success · ${payload.action} · replica ${data.replica_id || data.mode || "n/a"}`
        : `Blocked · ${failLabel || data.status || res.status} · ${data.message || data.decision || ""}`;
      setLive(summary);
      logEvent(summary + (data.transaction_id ? ` · tx ${data.transaction_id.slice(0, 8)}` : ""));
      await refreshSnapshot();
    } catch (err) {
      const hops = buildHopPlan(payload, {
        status: "error",
        message: `gateway unreachable: ${err.message}`,
        _fail_at: "gateway",
        _route: ["client", "gateway"],
      });
      await animateHops(hops);
      setLive(`Blocked · stopped at gateway · ${err.message}`);
      logEvent(`Blocked · gateway · ${err.message}`);
    } finally {
      sendBtn.disabled = false;
    }
  }

  function escapeHtml(value) {
    return String(value ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;");
  }

  function controlButtons(item) {
    if (!item.controllable) {
      return `<span class="muted-note">—</span>`;
    }
    const running = item.state === "running";
    const isolated = !!item.isolated;
    const id = escapeHtml(item.id);
    const name = escapeHtml(item.name || item.service || id);

    if (!running) {
      return `<div class="action-row">
        <button class="btn-tiny" type="button" data-docker-action="start" data-id="${id}" data-name="${name}">Start</button>
      </div>`;
    }

    return `<div class="action-row">
      <button class="btn-tiny danger" type="button" data-docker-action="stop" data-id="${id}" data-name="${name}">Stop</button>
      ${
        isolated
          ? `<button class="btn-tiny" type="button" data-docker-action="rejoin" data-id="${id}" data-name="${name}">Rejoin</button>`
          : `<button class="btn-tiny warn" type="button" data-docker-action="isolate" data-id="${id}" data-name="${name}">Isolate</button>`
      }
    </div>`;
  }

  function setDockerActionLive(msg, isError = false) {
    let el = $("#dockerActionLive");
    if (!el) {
      el = document.createElement("div");
      el.id = "dockerActionLive";
      el.className = "live";
      el.setAttribute("role", "status");
      $("#panel-docker .docker-panel")?.appendChild(el);
    }
    el.classList.remove("hidden");
    el.textContent = msg;
    el.style.color = isError ? "var(--color-destructive)" : "";
  }

  async function runDockerAction(containerId, action, name) {
    setDockerActionLive(`Working · ${action} ${name}…`);
    document
      .querySelectorAll("[data-docker-action]")
      .forEach((btn) => (btn.disabled = true));
    try {
      const res = await fetch(`/api/docker/containers/${encodeURIComponent(containerId)}/action`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action }),
      });
      const data = await res.json();
      if (!res.ok || data.ok === false) {
        throw new Error(data.error || `HTTP ${res.status}`);
      }
      setDockerActionLive(data.message || `${action} ok · ${name}`);
      await refreshDocker();
    } catch (err) {
      setDockerActionLive(`Failed · ${action} ${name} · ${err.message}`, true);
      await refreshDocker().catch(() => {});
    } finally {
      document
        .querySelectorAll("[data-docker-action]")
        .forEach((btn) => (btn.disabled = false));
    }
  }

  function renderDocker(data) {
    const err = $("#dockerError");
    const sub = $("#dockerSub");
    const counts = $("#dockerCounts");
    const updated = $("#dockerUpdated");

    if (!data?.ok) {
      err.classList.remove("hidden");
      err.textContent = `Docker status unavailable · ${data?.error || "unknown error"}${
        data?.hint ? ` · ${data.hint}` : ""
      }`;
      counts.innerHTML = "";
      return;
    }

    err.classList.add("hidden");
    err.textContent = "";
    sub.textContent = `Project “${data.project}” · network ${data.network || "—"} · live compose resources.`;
    updated.textContent = `Updated ${new Date().toLocaleTimeString()}`;

    const c = data.counts || {};
    counts.innerHTML = `
      <span class="pill"><span class="dot" aria-hidden="true"></span>Total ${c.total ?? 0}</span>
      <span class="pill ok"><span class="dot" aria-hidden="true"></span>Running ${c.running ?? 0}</span>
      <span class="pill ok"><span class="dot" aria-hidden="true"></span>Healthy ${c.healthy ?? 0}</span>
      <span class="pill ${c.isolated ? "bad" : ""}"><span class="dot" aria-hidden="true"></span>Isolated ${
        c.isolated ?? 0
      }</span>
      <span class="pill ${c.exited ? "bad" : ""}"><span class="dot" aria-hidden="true"></span>Exited ${
        c.exited ?? 0
      }</span>
    `;

    const tbody = $("#dockerContainers");
    if (!data.containers?.length) {
      tbody.innerHTML = `<tr><td colspan="6">No containers found for this project.</td></tr>`;
    } else {
      tbody.innerHTML = data.containers
        .map((item) => {
          const kind = item.kind || item.state || "unknown";
          const ports = (item.ports || []).join(", ") || "—";
          const statusExtra = item.isolated ? `${item.status || ""} · network isolated` : item.status || "—";
          return `<tr>
            <td><strong>${escapeHtml(item.service || "—")}</strong></td>
            <td class="mono">${escapeHtml(item.name)}</td>
            <td><span class="state-badge ${escapeHtml(kind)}">${escapeHtml(kind)}</span></td>
            <td>${escapeHtml(statusExtra)}</td>
            <td class="mono">${escapeHtml(ports)}</td>
            <td>${controlButtons(item)}</td>
          </tr>`;
        })
        .join("");
    }

    const volBody = $("#dockerVolumes");
    if (!data.volumes?.length) {
      volBody.innerHTML = `<tr><td colspan="2">No volumes.</td></tr>`;
    } else {
      volBody.innerHTML = data.volumes
        .map(
          (v) => `<tr>
            <td class="mono">${escapeHtml(v.name)}</td>
            <td>${escapeHtml(v.driver || "—")}</td>
          </tr>`
        )
        .join("");
    }

    const netBody = $("#dockerNetworks");
    if (!data.networks?.length) {
      netBody.innerHTML = `<tr><td colspan="3">No networks.</td></tr>`;
    } else {
      netBody.innerHTML = data.networks
        .map(
          (n) => `<tr>
            <td class="mono">${escapeHtml(n.name)}</td>
            <td>${escapeHtml(n.driver || "—")}</td>
            <td>${escapeHtml(n.scope || "—")}</td>
          </tr>`
        )
        .join("");
    }
  }

  async function refreshDocker() {
    const res = await fetch("/api/docker");
    const data = await res.json();
    renderDocker(data);
    return data;
  }

  function startLabPolling() {
    clearInterval(pollTimer);
    pollTimer = setInterval(() => {
      if (!document.hidden && activeTab === "lab") {
        refreshSnapshot().catch(() => {});
      }
    }, 2500);
  }

  function startDockerPolling() {
    clearInterval(dockerPollTimer);
    dockerPollTimer = setInterval(() => {
      if (!document.hidden && activeTab === "docker") {
        refreshDocker().catch(() => {});
      }
    }, 3000);
  }

  function setTab(name) {
    activeTab = name;
    document.querySelectorAll(".tab").forEach((btn) => {
      const on = btn.dataset.tab === name;
      btn.classList.toggle("active", on);
      btn.setAttribute("aria-selected", on ? "true" : "false");
    });
    document.querySelectorAll(".tab-panel").forEach((panel) => {
      const on = panel.id === `panel-${name}`;
      panel.classList.toggle("hidden", !on);
      if (on) panel.removeAttribute("hidden");
      else panel.setAttribute("hidden", "");
    });

    if (name === "lab") {
      refreshSnapshot().catch(() => {});
      startLabPolling();
      clearInterval(dockerPollTimer);
    } else {
      refreshDocker().catch(() => {});
      startDockerPolling();
      clearInterval(pollTimer);
    }
  }

  document.querySelectorAll(".tab").forEach((btn) => {
    btn.addEventListener("click", () => setTab(btn.dataset.tab));
  });

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
  $("#refreshBtn").addEventListener("click", () =>
    refreshSnapshot().then(() => setLive("Snapshot refreshed."))
  );
  $("#dockerRefreshBtn").addEventListener("click", () => refreshDocker());

  $("#dockerContainers").addEventListener("click", (e) => {
    const btn = e.target.closest("[data-docker-action]");
    if (!btn) return;
    const action = btn.getAttribute("data-docker-action");
    const id = btn.getAttribute("data-id");
    const name = btn.getAttribute("data-name") || id;
    if (!action || !id) return;
    runDockerAction(id, action, name);
  });

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

  toggleFields();
  refreshSnapshot()
    .then(() => setLive("Live. Components healthy — send a request."))
    .catch((err) => setLive(`Snapshot failed: ${err.message}`));
  startLabPolling();
  document.addEventListener("visibilitychange", () => {
    if (document.hidden) {
      clearInterval(pollTimer);
      clearInterval(dockerPollTimer);
    } else if (activeTab === "lab") {
      startLabPolling();
    } else {
      startDockerPolling();
    }
  });
})();
