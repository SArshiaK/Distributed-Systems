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
  let labPollTimer;
  let bonusPollTimer;
  let healPollTimer;
  let healEventCount = 0;
  let healSlotSig = "";
  let healReqSig = "";
  let healBusy = false;
  const healUi = {
    focusSlot: null,
    focusGen: null,
    step: "watch",
    message: "",
    crashedAt: 0,
  };
  let activeTab = "lab";
  let labStepCount = 0;
  let labReqCount = 0;
  let labPhaseSig = "";
  let bonusMigLogCount = 0;
  let bonusLogClearedAt = 0;
  const PHASE_ORDER = [
    "INIT",
    "PROVISION_TARGET",
    "SNAPSHOT",
    "CATCHUP",
    "FENCE_SOURCE",
    "FINAL_CATCHUP",
    "WAIT_INFLIGHT_2PC",
    "ACTIVATE_MAP",
    "VERIFY",
    "CLEANUP_SOURCE",
    "DONE",
  ];

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
    return String(value == null ? "" : value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
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
    const tbody = $("#dockerContainers");
    const volBody = $("#dockerVolumes");
    const netBody = $("#dockerNetworks");

    try {
      if (!data || data.ok === false) {
        const msg = `Docker status unavailable · ${(data && data.error) || "unknown error"}${
          data && data.hint ? ` · ${data.hint}` : ""
        }`;
        if (err) {
          err.classList.remove("hidden");
          err.textContent = msg;
        }
        if (counts) {
          counts.innerHTML = `<span class="pill bad"><span class="dot" aria-hidden="true"></span>Error</span>`;
        }
        if (updated) updated.textContent = "Failed";
        if (tbody) {
          tbody.innerHTML = `<tr><td colspan="6">${escapeHtml(msg)}</td></tr>`;
        }
        return;
      }

      if (err) {
        err.classList.add("hidden");
        err.textContent = "";
      }
      if (sub) {
        sub.textContent = `Project “${data.project || "?"}” · network ${
          data.network || "—"
        } · live compose resources.`;
      }
      if (updated) updated.textContent = `Updated ${new Date().toLocaleTimeString()}`;

      const c = data.counts || {};
      if (counts) {
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
      }

      const containers = Array.isArray(data.containers) ? data.containers : [];
      if (tbody) {
        if (!containers.length) {
          tbody.innerHTML = `<tr><td colspan="6">No containers found for this project.</td></tr>`;
        } else {
          tbody.innerHTML = containers
            .map((item) => {
              const kind = item.kind || item.state || "unknown";
              const portList = Array.isArray(item.ports) ? item.ports : [];
              const ports = portList.join(", ") || "—";
              const statusExtra = item.isolated
                ? `${item.status || ""} · network isolated`
                : item.status || "—";
              let serviceLabel = item.service || "—";
              if (item.managed) {
                serviceLabel = item.orphan
                  ? `${item.service || "heal"} · orphan heal`
                  : `${item.service || "heal"} · heal`;
              }
              const rowClass = item.orphan ? " class=\"docker-orphan\"" : "";
              return `<tr${rowClass}>
                <td><strong>${escapeHtml(serviceLabel)}</strong></td>
                <td class="mono">${escapeHtml(item.name)}</td>
                <td><span class="state-badge ${escapeHtml(kind)}">${escapeHtml(kind)}</span></td>
                <td>${escapeHtml(statusExtra)}</td>
                <td class="mono">${escapeHtml(ports)}</td>
                <td>${controlButtons(item)}</td>
              </tr>`;
            })
            .join("");
        }
      }

      const volumes = Array.isArray(data.volumes) ? data.volumes : [];
      if (volBody) {
        volBody.innerHTML = volumes.length
          ? volumes
              .map(
                (v) => `<tr>
                  <td class="mono">${escapeHtml(v.name)}</td>
                  <td>${escapeHtml(v.driver || "—")}</td>
                </tr>`
              )
              .join("")
          : `<tr><td colspan="2">No volumes.</td></tr>`;
      }

      const networks = Array.isArray(data.networks) ? data.networks : [];
      if (netBody) {
        netBody.innerHTML = networks.length
          ? networks
              .map(
                (n) => `<tr>
                  <td class="mono">${escapeHtml(n.name)}</td>
                  <td>${escapeHtml(n.driver || "—")}</td>
                  <td>${escapeHtml(n.scope || "—")}</td>
                </tr>`
              )
              .join("")
          : `<tr><td colspan="3">No networks.</td></tr>`;
      }
    } catch (e) {
      if (err) {
        err.classList.remove("hidden");
        err.textContent = `Docker render failed · ${e.message || e}`;
      }
      if (tbody) {
        tbody.innerHTML = `<tr><td colspan="6">Render error: ${escapeHtml(
          e.message || e
        )}</td></tr>`;
      }
    }
  }

  async function refreshDocker() {
    const updated = $("#dockerUpdated");
    const tbody = $("#dockerContainers");
    const err = $("#dockerError");
    const hasRows = !!(tbody && tbody.querySelector("tr td strong"));
    if (updated) updated.textContent = hasRows ? "Refreshing…" : "Loading…";
    if (tbody && !tbody.children.length) {
      tbody.innerHTML = `<tr><td colspan="6">Loading containers…</td></tr>`;
    }
    try {
      const res = await fetch("/api/docker", { cache: "no-store" });
      const data = await res.json();
      if (!res.ok && data && data.ok !== true) {
        throw new Error(data.error || `HTTP ${res.status}`);
      }
      renderDocker(data);
      return data;
    } catch (e) {
      // Keep the last good table during transient poll/network blips.
      if (hasRows) {
        if (err) {
          err.classList.remove("hidden");
          err.textContent = `Refresh failed · ${e.message || e} (showing last data)`;
        }
        if (updated) updated.textContent = "Refresh failed";
        return null;
      }
      renderDocker({
        ok: false,
        error: e.message || String(e),
        hint: "Check that console has /var/run/docker.sock mounted.",
      });
      throw e;
    }
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

  function startExperimentsPolling(fast = false) {
    clearInterval(labPollTimer);
    labPollTimer = setInterval(() => {
      if (!document.hidden && activeTab === "experiments") {
        refreshLabStatus().catch(() => {});
      }
    }, fast ? 350 : 2000);
  }

  function setLabButtonsDisabled(disabled) {
    document.querySelectorAll("[data-lab-run]").forEach((btn) => {
      btn.disabled = disabled;
    });
  }

  function badgeClass(passed, running, isCurrent) {
    if (running && isCurrent) return "preparing";
    if (passed === true && isCurrent) return "completed";
    if (passed === false && isCurrent) return "aborted";
    return "";
  }

  function badgeText(passed, running, isCurrent) {
    if (running && isCurrent) return "live";
    if (passed === true && isCurrent) return "PROVEN";
    if (passed === false && isCurrent) return "FAILED";
    return "idle";
  }

  function renderLabPhases(phases) {
    const el = $("#labPhases");
    if (!el) return;
    const sig = JSON.stringify(phases || []);
    if (sig === labPhaseSig) return;
    labPhaseSig = sig;
    if (!phases?.length) {
      el.innerHTML = `<li class="muted-note">Phases appear when a demo starts.</li>`;
      return;
    }
    el.innerHTML = phases
      .map((p) => {
        const st = escapeHtml(p.status || "");
        return `<li class="${st}">
          <span class="phase-label">${escapeHtml(p.label || p.key)}</span>
          ${
            p.detail
              ? `<span class="phase-detail">${escapeHtml(p.detail)}</span>`
              : ""
          }
        </li>`;
      })
      .join("");
  }

  function renderLabProof(data) {
    const el = $("#labProof");
    if (!el) return;
    const proof = data.proof || {};
    const passed = data.passed;
    const exp = data.experiment;
    const boxes = [];

    if (exp === 3 || proof.account) {
      boxes.push(`<div class="proof-box ${
        proof.after != null && proof.after === proof.expected ? "ok" : ""
      }">
        <strong>Balance proof · ${escapeHtml(proof.account || "—")}</strong>
        <div class="mono">before: ${proof.before ?? "—"}</div>
        <div class="mono">formula: ${escapeHtml(proof.formula || "—")}</div>
        <div class="mono">after: ${proof.after ?? "waiting…"}</div>
        <div class="mono">expected: ${proof.expected ?? "—"}</div>
      </div>`);
      const hits = data.metrics?.replica_hits || {};
      boxes.push(`<div class="proof-box">
        <strong>Replica traffic (live)</strong>
        <div class="mono">A1 handled: ${hits.A1 ?? 0}</div>
        <div class="mono">A2 handled: ${hits.A2 ?? 0}</div>
        <div class="mono">done: ${data.metrics?.done ?? 0} / ${data.metrics?.total ?? 0}</div>
      </div>`);
    }

    if (exp === 6 || proof.from_account) {
      const before = proof.before || {};
      const after = proof.after || {};
      boxes.push(`<div class="proof-box ${
        proof.final_status === "COMPLETED" ? "ok" : ""
      }">
        <strong>2PC crash recovery</strong>
        <div class="mono">${escapeHtml(proof.from_account || "")} → ${escapeHtml(
          proof.to_account || ""
        )} · ${proof.amount ?? ""}</div>
        <div class="mono">tx: ${escapeHtml((proof.tx_id || "—").toString().slice(0, 8))}</div>
        <div class="mono">after kill: ${escapeHtml(proof.status_after_kill || "—")}</div>
        <div class="mono">final: ${escapeHtml(proof.final_status || "—")}</div>
      </div>`);
      boxes.push(`<div class="proof-box ${
        after[proof.from_account] != null ? "ok" : ""
      }">
        <strong>Money movement</strong>
        <div class="mono">${escapeHtml(proof.from_account || "src")}: ${
          before[proof.from_account] ?? "—"
        } → ${after[proof.from_account] ?? "…"}</div>
        <div class="mono">${escapeHtml(proof.to_account || "dst")}: ${
          before[proof.to_account] ?? "—"
        } → ${after[proof.to_account] ?? "…"}</div>
      </div>`);
    }

    if (exp === 7 || proof.money_before != null) {
      const conserved =
        proof.money_after != null && proof.money_before === proof.money_after;
      boxes.push(`<div class="proof-box ${conserved ? "ok" : ""}">
        <strong>Conservation of money</strong>
        <div class="mono">before: ${proof.money_before ?? "—"}</div>
        <div class="mono">after: ${proof.money_after ?? "waiting…"}</div>
        <div class="mono">jobs: ${proof.jobs ?? data.metrics?.total ?? "—"}</div>
        <div class="mono">COMMIT ${data.metrics?.success ?? proof.successes ?? 0} · ABORT ${
          data.metrics?.abort ?? proof.aborts ?? 0
        }</div>
      </div>`);
      const sb = proof.sample_before || {};
      const sa = proof.sample_after || {};
      const keys = Object.keys(sb);
      if (keys.length) {
        boxes.push(`<div class="proof-box">
          <strong>Sample account balances</strong>
          ${keys
            .map(
              (k) =>
                `<div class="mono">${escapeHtml(k)}: ${sb[k]} → ${
                  sa[k] ?? "…"
                }</div>`
            )
            .join("")}
        </div>`);
      }
    }

    if (!boxes.length) {
      el.innerHTML = `<p class="muted-note">Before / after values and formulas show up here.</p>`;
      return;
    }

    if (passed === true) {
      boxes.unshift(`<div class="proof-box ok"><strong>Verdict</strong><div class="mono">Demonstrated on the live Docker stack — requirements satisfied.</div></div>`);
    } else if (passed === false) {
      boxes.unshift(`<div class="proof-box bad"><strong>Verdict</strong><div class="mono">${escapeHtml(
        data.error || "failed"
      )}</div></div>`);
    }

    el.innerHTML = boxes.join("");
  }

  function renderLabRequests(requests) {
    const tbody = $("#labReqTable");
    const meta = $("#labReqMeta");
    if (!tbody) return;
    const rows = requests || [];
    if (meta) meta.textContent = `${rows.length} request${rows.length === 1 ? "" : "s"}`;
    if (!rows.length) {
      tbody.innerHTML = `<tr><td colspan="8">No requests yet. Start a demo.</td></tr>`;
      labReqCount = 0;
      return;
    }
    const newest = rows.slice().reverse().slice(0, 80);
    const grew = rows.length > labReqCount;
    labReqCount = rows.length;
    tbody.innerHTML = newest
      .map((r, idx) => {
        const accounts = r.to_account
          ? `${r.account} → ${r.to_account}`
          : r.account || "—";
        let resultCls = "pending-cell";
        let resultText = "…";
        if (r.ok === true) {
          resultCls = "ok-cell";
          resultText = "OK";
        } else if (r.ok === false) {
          resultCls = "fail-cell";
          resultText = "FAIL";
        } else if (r.detail && /COMMIT|ABORT|PENDING/i.test(r.detail)) {
          resultText = String(r.detail).split("·")[0].trim();
          resultCls = /COMMIT/i.test(resultText)
            ? "ok-cell"
            : /ABORT|FAIL/i.test(resultText)
              ? "fail-cell"
              : "pending-cell";
        }
        return `<tr class="${grew && idx < 3 ? "fresh" : ""}">
          <td class="mono">${r.seq ?? ""}</td>
          <td>${escapeHtml(r.kind || "—")}</td>
          <td>${escapeHtml(accounts)}</td>
          <td>${r.amount != null ? Number(r.amount).toFixed(2) : "—"}</td>
          <td class="mono">${escapeHtml(r.replica || "—")}</td>
          <td class="${resultCls}">${escapeHtml(resultText)}</td>
          <td class="mono">${r.ms != null ? r.ms : "—"}</td>
          <td>${escapeHtml(r.detail || "")}</td>
        </tr>`;
      })
      .join("");
  }

  function renderLabMetrics(metrics, running, passed) {
    const el = $("#labMetrics");
    if (!el) return;
    const m = metrics || {};
    const pills = [];
    if (m.total != null) {
      pills.push(
        `<span class="pill"><span class="dot" aria-hidden="true"></span>Progress ${m.done ?? 0}/${m.total}</span>`
      );
    }
    if (m.replica_hits) {
      pills.push(
        `<span class="pill ok"><span class="dot" aria-hidden="true"></span>A1 ${m.replica_hits.A1 ?? 0}</span>`
      );
      pills.push(
        `<span class="pill ok"><span class="dot" aria-hidden="true"></span>A2 ${m.replica_hits.A2 ?? 0}</span>`
      );
    }
    if (m.success != null || m.abort != null) {
      pills.push(
        `<span class="pill ok"><span class="dot" aria-hidden="true"></span>COMMIT ${m.success ?? 0}</span>`
      );
      pills.push(
        `<span class="pill ${m.abort ? "bad" : ""}"><span class="dot" aria-hidden="true"></span>ABORT ${m.abort ?? 0}</span>`
      );
    }
    if (m.tx_status) {
      pills.push(
        `<span class="pill"><span class="dot" aria-hidden="true"></span>Tx ${escapeHtml(
          String(m.tx_status)
        )}</span>`
      );
    }
    if (m.coordinator) {
      pills.push(
        `<span class="pill ${m.coordinator === "killed" ? "bad" : "ok"}"><span class="dot" aria-hidden="true"></span>Coordinator ${escapeHtml(
          String(m.coordinator)
        )}</span>`
      );
    }
    if (m.money_before != null) {
      pills.push(
        `<span class="pill"><span class="dot" aria-hidden="true"></span>Money ${m.money_before}${
          m.money_after != null ? ` → ${m.money_after}` : ""
        }</span>`
      );
    }
    if (!pills.length) {
      el.innerHTML = running
        ? `<span class="pill"><span class="dot" aria-hidden="true"></span>Collecting live metrics…</span>`
        : "";
      return;
    }
    el.innerHTML = pills.join("");
  }

  function renderLabStatus(data) {
    const row = $("#labRunStatus");
    const meta = $("#labLogMeta");
    const list = $("#labStepLog");
    const headline = $("#labHeadline");
    const title = $("#labHeadlineTitle");
    if (!row || !list) return;

    const running = !!data.running;
    const exp = data.experiment;
    const passed = data.passed;

    if (title) {
      title.textContent = exp
        ? `Experiment ${exp} · ${data.title || "demo"}`
        : "Demo stage";
    }
    if (headline) {
      headline.textContent = data.headline || "Pick an experiment to demonstrate.";
      headline.classList.remove("pass", "fail", "run");
      if (running) headline.classList.add("run");
      else if (passed === true) headline.classList.add("pass");
      else if (passed === false) headline.classList.add("fail");
    }

    row.innerHTML = `
      <span class="pill ${running ? "" : passed === true ? "ok" : passed === false ? "bad" : ""}">
        <span class="dot" aria-hidden="true"></span>
        ${
          running
            ? `LIVE DEMO · experiment ${exp}`
            : passed === true
              ? `PROVEN on live stack · exp ${exp}`
              : passed === false
                ? `FAILED · exp ${exp}`
                : "Ready for presentation"
        }
      </span>
    `;

    [3, 6, 7].forEach((id) => {
      const badge = document.getElementById(`labBadge${id}`);
      const card = document.querySelector(`.exp-card[data-exp="${id}"]`);
      const current = exp === id;
      if (badge) {
        badge.className = `status-chip ${badgeClass(passed, running, current)}`;
        badge.textContent = badgeText(passed, running, current);
      }
      if (card) card.classList.toggle("active-run", running && current);
    });

    setLabButtonsDisabled(running);
    renderLabMetrics(data.metrics, running, passed);
    renderLabPhases(data.phases);
    renderLabProof(data);
    renderLabRequests(data.requests);

    const steps = data.steps || [];
    if (steps.length !== labStepCount) {
      labStepCount = steps.length;
      list.innerHTML = steps
        .slice()
        .reverse()
        .map((s) => {
          const cls = s.level === "pass" ? "pass" : s.level === "fail" ? "fail" : "";
          const ts = s.t ? new Date(s.t * 1000).toLocaleTimeString() : "";
          return `<li class="${cls}">${escapeHtml(ts)} · ${escapeHtml(s.message)}</li>`;
        })
        .join("");
    }

    if (meta) {
      meta.textContent = running
        ? `Recording live · poll 350ms`
        : exp
          ? `Finished · leave this screen visible for review`
          : "Idle";
    }
  }

  async function refreshLabStatus() {
    const res = await fetch("/api/lab/status");
    const data = await res.json();
    renderLabStatus(data);
    if (data.running) startExperimentsPolling(true);
    else if (activeTab === "experiments") startExperimentsPolling(false);
    return data;
  }

  async function clearLabLiveBoard() {
    try {
      const res = await fetch("/api/lab/clear", { method: "POST" });
      const data = await res.json();
      if (!res.ok || data.ok === false) {
        throw new Error(data.error || `HTTP ${res.status}`);
      }
      labStepCount = 0;
      labReqCount = 0;
      labPhaseSig = "";
      await refreshLabStatus();
    } catch (err) {
      const row = $("#labRunStatus");
      if (row) {
        row.innerHTML = `<span class="pill bad"><span class="dot" aria-hidden="true"></span>${escapeHtml(
          err.message || String(err)
        )}</span>`;
      }
    }
  }

  async function runLabExperiment(id) {
    setLabButtonsDisabled(true);
    labStepCount = 0;
    labReqCount = 0;
    labPhaseSig = "";
    const list = $("#labStepLog");
    if (list) list.innerHTML = "";
    const reqBody = $("#labReqTable");
    if (reqBody) reqBody.innerHTML = `<tr><td colspan="8">Starting live demo…</td></tr>`;
    const proof = $("#labProof");
    if (proof) proof.innerHTML = `<p class="muted-note">Capturing baseline…</p>`;
    const phases = $("#labPhases");
    if (phases) phases.innerHTML = `<li class="active"><span class="phase-label">Starting…</span></li>`;
    try {
      const res = await fetch("/api/lab/run", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ experiment: Number(id) }),
      });
      const data = await res.json();
      if (!res.ok || data.ok === false) {
        throw new Error(data.error || `HTTP ${res.status}`);
      }
      startExperimentsPolling(true);
      await refreshLabStatus();
    } catch (err) {
      setLabButtonsDisabled(false);
      $("#labRunStatus").innerHTML = `
        <span class="pill bad"><span class="dot" aria-hidden="true"></span>${escapeHtml(
          err.message
        )}</span>`;
    }
  }

  function fmtOwn(own) {
    if (!own || own.ok === false) {
      return `<span class="pill bad"><span class="dot" aria-hidden="true"></span>${escapeHtml(
        own?.error || "unavailable"
      )}</span>`;
    }
    const fence =
      own.fenced_start != null
        ? `fence ${own.fenced_start}-${own.fenced_end}`
        : "no fence";
    return `
      <span class="pill"><span class="dot" aria-hidden="true"></span>epoch ${own.epoch ?? "—"}</span>
      <span class="pill"><span class="dot" aria-hidden="true"></span>owned ${own.owned_start}-${own.owned_end}</span>
      <span class="pill ${own.write_enabled ? "ok" : "bad"}"><span class="dot" aria-hidden="true"></span>writes ${
        own.write_enabled ? "ON" : "OFF"
      }</span>
      <span class="pill ${own.fenced_start != null ? "bad" : ""}"><span class="dot" aria-hidden="true"></span>${escapeHtml(
        fence
      )}</span>
    `;
  }

  function renderBonusDb(letter, db) {
    const tbody = document.getElementById(`bonusTable${letter}`);
    const meta = document.getElementById(`bonusDb${letter}Meta`);
    const chBody = document.getElementById(`bonusChange${letter}`);
    const ownRow = document.getElementById(`bonusOwn${letter}`);
    if (!tbody) return;
    if (!db?.ok) {
      if (meta) meta.textContent = db?.error || "unavailable";
      tbody.innerHTML = `<tr><td colspan="2">DB ${letter} unavailable</td></tr>`;
      if (chBody) chBody.innerHTML = "";
      return;
    }
    const mig = db.range_1501_2000 || {};
    const low = db.range_1001_1500 || {};
    if (meta) {
      meta.textContent = `n=${db.total_count} sum=${Number(db.total_sum).toFixed(2)} · mig ${
        mig.count ?? 0
      }/${Number(mig.sum || 0).toFixed(0)} · lowB ${low.count ?? 0}`;
    }
    const rows = db.accounts || [];
    tbody.innerHTML = rows.length
      ? rows
          .map(
            (r) =>
              `<tr><td>${escapeHtml(r.account_id)}</td><td>${Number(r.balance).toFixed(
                2
              )}</td></tr>`
          )
          .join("")
      : `<tr><td colspan="2">no watched rows</td></tr>`;
    const changes = db.changelog || [];
    if (chBody) {
      chBody.innerHTML = changes.length
        ? changes
            .map(
              (c) =>
                `<tr><td class="mono">${c.seq}</td><td>${escapeHtml(c.op)}</td><td>${escapeHtml(
                  c.account_id
                )}</td><td>${Number(c.balance).toFixed(2)}</td></tr>`
            )
            .join("")
        : `<tr><td colspan="4">no changelog yet</td></tr>`;
    }
    if (ownRow && letter !== "A") {
      // filled from ownership admin below
    }
  }

  function renderBonusLive(data) {
    const top = $("#bonusTopStatus");
    const headline = $("#bonusHeadline");
    const phaseMeta = $("#bonusPhaseMeta");
    const phasesEl = $("#bonusPhases");
    const migLog = $("#bonusMigLog");
    if (!top) return;

    const mig = data.migrator || {};
    const job = mig.job || {};
    const phase = job.phase || mig.phase || "idle";
    const traffic = data.traffic || {};
    const sm = data.shard_map || {};
    const runningMig = !!mig.running;
    const stoppedMig = !!mig.stopped && !runningMig;
    const trafficOn = !!traffic.running;
    const migLabel = runningMig
      ? `LIVE · ${phase}`
      : stoppedMig
        ? `STOPPED · ${phase}`
        : phase;

    top.innerHTML = `
      <span class="pill ${runningMig ? "" : mig.passed ? "ok" : stoppedMig ? "bad" : ""}"><span class="dot" aria-hidden="true"></span>Migration ${
        escapeHtml(migLabel)
      }</span>
      <span class="pill ${trafficOn ? "ok" : ""}"><span class="dot" aria-hidden="true"></span>Traffic ${
        trafficOn ? "ON" : "OFF"
      } · sent ${traffic.stats?.sent ?? 0}</span>
      <span class="pill"><span class="dot" aria-hidden="true"></span>Map epoch ${
        sm.epoch ?? "—"
      }</span>
      <span class="pill"><span class="dot" aria-hidden="true"></span>Shards ${Object.keys(
        sm.shards || {}
      ).join(",") || "—"}</span>
      <span class="pill ${ (data.inflight_2pc || []).length ? "bad" : "ok" }"><span class="dot" aria-hidden="true"></span>In-flight 2PC ${(
        data.inflight_2pc || []
      ).length}</span>
    `;

    const events = (mig.events || []).filter((e) => !bonusLogClearedAt || (e.t || 0) > bonusLogClearedAt);

    if (headline) {
      const last = events.length ? events[events.length - 1].message : "Waiting…";
      headline.textContent = last;
      headline.classList.remove("pass", "fail", "run");
      if (runningMig) headline.classList.add("run");
      else if (phase === "DONE" || mig.passed) headline.classList.add("pass");
      else if (mig.passed === false) headline.classList.add("fail");
    }
    if (phaseMeta) {
      phaseMeta.textContent = runningMig
        ? `live · ${phase}`
        : `phase ${phase}`;
    }

    if (phasesEl) {
      const idx = PHASE_ORDER.indexOf(phase);
      phasesEl.innerHTML = PHASE_ORDER.map((p, i) => {
        let st = "";
        if (phase === "DONE" || (idx >= 0 && i < idx)) st = "done";
        else if (p === phase) st = runningMig ? "active" : phase === "DONE" ? "done" : "active";
        if (mig.passed === false && p === phase) st = "fail";
        return `<li class="${st}"><span class="phase-label">${p}</span></li>`;
      }).join("");
    }

    if (migLog && events.length !== bonusMigLogCount) {
      bonusMigLogCount = events.length;
      migLog.innerHTML = events.length
        ? events
            .slice()
            .reverse()
            .slice(0, 40)
            .map((e) => {
              const cls = e.level === "pass" ? "pass" : e.level === "fail" ? "fail" : "";
              const ts = e.t ? new Date(e.t * 1000).toLocaleTimeString() : "";
              return `<li class="${cls}">${escapeHtml(ts)} · ${escapeHtml(e.message)}</li>`;
            })
            .join("")
        : "";
    }

    renderBonusDb("A", data.databases?.A);
    renderBonusDb("B", data.databases?.B);
    renderBonusDb("C", data.databases?.C);
    const ownA = $("#bonusOwnA");
    const ownB = $("#bonusOwnB");
    const ownC = $("#bonusOwnC");
    if (ownA) ownA.innerHTML = `<span class="pill ok"><span class="dot" aria-hidden="true"></span>stable shard A</span>`;
    if (ownB) ownB.innerHTML = fmtOwn(data.ownership?.B || data.databases?.B?.ownership);
    if (ownC) ownC.innerHTML = fmtOwn(data.ownership?.C || data.databases?.C?.ownership);

    const reqStream = $("#bonusReqStream");
    const reqMeta = $("#bonusReqMeta");
    const reqs = (traffic.requests || []).slice().reverse();
    if (reqMeta) {
      reqMeta.textContent = `${reqs.length} · ok ${traffic.stats?.ok ?? 0} · fail ${
        traffic.stats?.fail ?? 0
      } · fence ${traffic.stats?.fenced ?? 0}`;
    }
    if (reqStream) {
      if (!reqs.length) {
        reqStream.innerHTML =
          `<div class="bonus-req-empty">Start traffic to stream live requests here.</div>`;
      } else {
        reqStream.innerHTML = reqs
          .slice(0, 100)
          .map((r) => {
            const acc = r.to_account ? `${r.account} → ${r.to_account}` : r.account || "—";
            const served = [r.shard, r.replica].filter(Boolean).join(" / ") || "—";
            let state = "pending";
            let label = r.detail || "…";
            if (r.ok && !r.pending) {
              state = "ok";
              label = r.decision || "OK";
            } else if (r.pending) {
              state = "pending";
              label = "PENDING";
            } else if (r.ok === false) {
              state = "fail";
              label = "FAIL";
            }
            const ts = r.t ? new Date(r.t * 1000).toLocaleTimeString() : "";
            return `<article class="bonus-req-card ${state}">
              <div class="bonus-req-top">
                <span class="bonus-req-op">${escapeHtml(r.kind || "op")}</span>
                <span class="bonus-req-time">${escapeHtml(ts)}</span>
              </div>
              <div class="bonus-req-accounts">${escapeHtml(acc)}</div>
              <div class="bonus-req-meta">
                <span>${escapeHtml(served)}</span>
                <span>${r.ms != null ? r.ms + " ms" : "—"}</span>
                <span class="bonus-req-result ${state}">${escapeHtml(label)}</span>
              </div>
              <div class="bonus-req-detail">${escapeHtml(r.detail || "")}${
                r.tx ? " · tx " + escapeHtml(r.tx) : ""
              }</div>
            </article>`;
          })
          .join("");
      }
    }

    const inflight = data.inflight_2pc || [];
    const recent = data.recent_2pc || [];
    const infBody = $("#bonusInflightTable");
    const recentBody = $("#bonusRecent2pcTable");
    const p2meta = $("#bonus2pcMeta");
    if (p2meta) p2meta.textContent = `${inflight.length} in-flight · ${recent.length} recent`;
    if (infBody) {
      infBody.innerHTML = inflight.length
        ? inflight
            .map(
              (t) => `<tr>
                <td class="mono">${escapeHtml(String(t.transaction_id).slice(0, 8))}</td>
                <td>${escapeHtml(t.from_account)} → ${escapeHtml(t.to_account)}</td>
                <td>${escapeHtml(t.from_shard)} → ${escapeHtml(t.to_shard)}</td>
                <td><span class="status-chip ${escapeHtml(String(t.status).toLowerCase())}">${escapeHtml(
                  t.status
                )}</span></td>
                <td>${escapeHtml((t.updated_at || "").split("T").pop() || "")}</td>
              </tr>`
            )
            .join("")
        : `<tr><td colspan="5">None — no blocked/open 2PC on migrating accounts</td></tr>`;
    }
    if (recentBody) {
      recentBody.innerHTML = recent.length
        ? recent
            .map(
              (t) => `<tr>
                <td class="mono">${escapeHtml(String(t.transaction_id).slice(0, 8))}</td>
                <td>${escapeHtml(t.from_account)} → ${escapeHtml(t.to_account)}</td>
                <td>${escapeHtml(t.from_shard)} → ${escapeHtml(t.to_shard)}</td>
                <td><span class="status-chip ${escapeHtml(String(t.status).toLowerCase())}">${escapeHtml(
                  t.status
                )}</span></td>
              </tr>`
            )
            .join("")
        : `<tr><td colspan="4">—</td></tr>`;
    }
  }

  function startBonusPolling() {
    clearInterval(bonusPollTimer);
    bonusPollTimer = setInterval(() => {
      if (!document.hidden && activeTab === "reshard") {
        refreshBonusLive().catch(() => {});
      }
    }, 300);
  }

  async function refreshBonusLive() {
    const res = await fetch("/api/bonus/live");
    const data = await res.json();
    renderBonusLive(data);
    return data;
  }

  function healShortHost(s) {
    return String(s.dns || s.url || s.slot_id)
      .replace(/^https?:\/\//, "")
      .replace(/:8000$/, "");
  }

  function healDeriveStep(data, slots) {
    const events = data.events || [];
    const last = events.length ? events[events.length - 1] : null;
    const msg = last?.message || "";

    if (healUi.focusSlot) {
      const focused = slots.find((s) => s.slot_id === healUi.focusSlot);
      if (
        focused &&
        focused.status === "healthy" &&
        healUi.focusGen != null &&
        Number(focused.generation) > Number(healUi.focusGen) &&
        Date.now() - healUi.crashedAt > 800
      ) {
        healUi.step = "done";
        healUi.message = `Recovered ${focused.slot_id} → ${healShortHost(focused)}`;
        return "done";
      }
    }

    if (data.replacing || data.pending) {
      if (/registered|register/i.test(msg)) return "register";
      if (/creating|create|replacement/i.test(msg)) return "create";
      return "create";
    }
    if (healUi.focusSlot) {
      const focused = slots.find((s) => s.slot_id === healUi.focusSlot);
      if (focused && focused.status !== "healthy") return "crash";
      if (Date.now() - healUi.crashedAt < 12000 && healUi.step !== "done") {
        if (/registered/i.test(msg)) return "register";
        if (/creating|replacement/i.test(msg)) return "create";
        return healUi.step === "watch" ? "crash" : healUi.step;
      }
    }
    if (/injected crash|unhealthy streak/i.test(msg) && Date.now() - healUi.crashedAt < 8000) {
      return "crash";
    }
    return healUi.step === "done" && Date.now() - healUi.crashedAt < 8000 ? "done" : "watch";
  }

  function renderHealFlow(step) {
    const order = ["watch", "crash", "create", "register", "done"];
    const idx = order.indexOf(step);
    document.querySelectorAll("#healFlowSteps [data-step]").forEach((el) => {
      const s = el.getAttribute("data-step");
      const i = order.indexOf(s);
      el.classList.toggle("active", s === step);
      el.classList.toggle("hot", s === step && (step === "crash" || step === "create"));
      el.classList.toggle("done", i >= 0 && idx >= 0 && i < idx);
    });
    const meta = $("#healFlowMeta");
    if (meta) meta.textContent = step;
  }

  function renderHeal(data) {
    const top = $("#healTopStatus");
    if (!top) return;
    if (data?.ok === false) {
      top.innerHTML = `<span class="pill bad"><span class="dot" aria-hidden="true"></span>${escapeHtml(
        data.error || "healer unavailable"
      )}</span>`;
      return;
    }

    const traffic = data.traffic || {};
    const summary = data.summary || {};
    const events = data.events || [];
    const slots = [];
    Object.values(data.slots || {}).forEach((rows) => slots.push(...rows));

    const step = healDeriveStep(data, slots);
    healUi.step = step;
    renderHealFlow(step);

    const phaseLabel =
      step === "crash"
        ? "Crash detected"
        : step === "create"
          ? "Creating replacement…"
          : step === "register"
            ? "Registering with Gateway…"
            : step === "done"
              ? "Recovered"
              : "Watching";

    top.innerHTML = `
      <span class="pill ${step === "watch" || step === "done" ? "ok" : "bad"}"><span class="dot" aria-hidden="true"></span>${escapeHtml(
        phaseLabel
      )}</span>
      <span class="pill"><span class="dot" aria-hidden="true"></span>A ${summary.A?.healthy ?? 0}/2</span>
      <span class="pill"><span class="dot" aria-hidden="true"></span>B ${summary.B?.healthy ?? 0}/2</span>
      <span class="pill ${traffic.running ? "ok" : ""}"><span class="dot" aria-hidden="true"></span>Traffic ${
        traffic.running ? "ON" : "OFF"
      } · ${traffic.stats?.ok ?? 0}/${traffic.stats?.sent ?? 0} ok</span>
    `;

    const headline = $("#healHeadline");
    if (headline) {
      headline.textContent =
        healUi.message ||
        (events.length ? events[events.length - 1].message : "Start traffic, then press Crash.");
    }

    const sig = slots
      .map((s) => {
        const forcedDown =
          healUi.focusSlot === s.slot_id &&
          step !== "done" &&
          step !== "watch" &&
          Date.now() - healUi.crashedAt < 15000;
        const st = forcedDown && s.status === "healthy" && step !== "done" ? "healing" : s.status;
        return `${s.slot_id}|${st}|${s.dns}|${s.generation}|${healBusy ? 1 : 0}`;
      })
      .join(";");

    const grid = $("#healSlotsTable");
    if (grid && sig !== healSlotSig) {
      healSlotSig = sig;
      grid.innerHTML = slots
        .map((s) => {
          const forcedDown =
            healUi.focusSlot === s.slot_id &&
            step !== "done" &&
            step !== "watch" &&
            Date.now() - healUi.crashedAt < 15000;
          const healing = forcedDown && s.status === "healthy" && step !== "done";
          const up = s.status === "healthy" && !forcedDown;
          const label = healing ? "healing" : up ? "up" : "down";
          const host = healShortHost(s);
          const canCrash = up && !healBusy && !data.replacing;
          return `<div class="heal-card ${up ? "up" : healing ? "healing" : "down"}" data-slot="${escapeHtml(
            s.slot_id
          )}">
            <div class="heal-card-top">
              <strong>${escapeHtml(s.slot_id)} · shard ${escapeHtml(s.shard)}</strong>
              <span class="status-chip ${up ? "ok" : "bad"}">${label}</span>
            </div>
            <div class="mono heal-card-host">${escapeHtml(host)}</div>
            <button class="btn btn-ghost" type="button" data-heal-crash="${escapeHtml(
              s.slot_id
            )}" ${canCrash ? "" : "disabled"}>${healBusy && healUi.focusSlot === s.slot_id ? "Working…" : "Crash"}</button>
          </div>`;
        })
        .join("");
    }

    const reqMeta = $("#healReqMeta");
    if (reqMeta) {
      reqMeta.textContent = traffic.running
        ? `traffic on · ${traffic.stats?.sent ?? 0} sent`
        : "traffic off";
    }

    const logEl = $("#healEventLog");
    if (logEl && events.length !== healEventCount) {
      healEventCount = events.length;
      const important = events.filter((e) => ["pass", "fail", "phase"].includes(e.level));
      const show = (important.length ? important : events).slice().reverse().slice(0, 10);
      logEl.innerHTML = show.length
        ? show
            .map((e) => {
              const cls = e.level === "pass" ? "pass" : e.level === "fail" ? "fail" : "";
              return `<li class="${cls}">${escapeHtml(e.message)}</li>`;
            })
            .join("")
        : `<li class="muted-note">No events yet.</li>`;
    }

    const stream = $("#healReqStream");
    const streamMeta = $("#healStreamMeta");
    const reqs = traffic.requests || [];
    if (streamMeta) streamMeta.textContent = String(traffic.stats?.sent ?? reqs.length);
    const rsig = reqs
      .slice(-40)
      .map((r) => `${r.id}|${r.ok}|${r.replica || ""}`)
      .join(";");
    if (stream && rsig !== healReqSig) {
      healReqSig = rsig;
      if (!reqs.length) {
        stream.innerHTML = `<div class="bonus-req-empty">Start traffic to stream requests here.</div>`;
      } else {
        stream.innerHTML = reqs
          .slice()
          .reverse()
          .slice(0, 40)
          .map((r) => {
            const cls = r.ok ? "ok" : "bad";
            const ts = r.t ? new Date(r.t * 1000).toLocaleTimeString() : "";
            const rep = String(r.replica || "—")
              .replace(/^https?:\/\//, "")
              .replace(/:8000$/, "");
            return `<div class="bonus-req ${cls}">
              <span class="mono">${escapeHtml(ts)}</span>
              <strong>${escapeHtml(r.action || "?")}</strong>
              ${escapeHtml(r.account || "")}
              <span class="mono">${escapeHtml(rep)}</span>
              <span>${r.ok ? "ok" : "fail"}</span>
            </div>`;
          })
          .join("");
      }
    }
  }

  async function refreshHeal() {
    const res = await fetch("/api/heal/status");
    const data = await res.json();
    renderHeal(data);
    return data;
  }

  function startHealPolling() {
    clearInterval(healPollTimer);
    healPollTimer = setInterval(() => {
      if (!document.hidden && activeTab === "heal") {
        refreshHeal().catch(() => {});
      }
    }, 500);
  }

  async function healCrash(slotId) {
    if (!slotId || healBusy) return;
    healBusy = true;
    healSlotSig = ""; // force card redraw
    const snapshot = await refreshHeal().catch(() => null);
    const slots = [];
    Object.values(snapshot?.slots || {}).forEach((rows) => slots.push(...rows));
    const before = slots.find((s) => s.slot_id === slotId);
    healUi.focusSlot = slotId;
    healUi.focusGen = before ? Number(before.generation || 1) : 1;
    healUi.step = "crash";
    healUi.crashedAt = Date.now();
    healUi.message = `Crashing ${slotId}…`;
    renderHealFlow("crash");
    const headline = $("#healHeadline");
    if (headline) headline.textContent = healUi.message;

    try {
      const res = await fetch("/api/heal/inject/crash", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ slot_id: slotId, mode: "stop" }),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok || data.ok === false) {
        healUi.message = `Crash failed: ${data.error || res.status}`;
        alert(healUi.message);
        healUi.step = "watch";
        healUi.focusSlot = null;
      } else {
        healUi.message = `Stopped ${slotId}. Healer is replacing it…`;
        healUi.step = "crash";
      }
    } catch (err) {
      healUi.message = `Crash failed: ${err.message || err}`;
      alert(healUi.message);
      healUi.focusSlot = null;
      healUi.step = "watch";
    } finally {
      healBusy = false;
      healSlotSig = "";
      await refreshHeal().catch(() => {});
    }
  }

  function setTab(name) {
    activeTab = name;
    document.body.dataset.tab = name;
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

    clearInterval(pollTimer);
    clearInterval(dockerPollTimer);
    clearInterval(labPollTimer);
    clearInterval(bonusPollTimer);
    clearInterval(healPollTimer);

    if (name === "lab") {
      refreshSnapshot().catch(() => {});
      startLabPolling();
    } else if (name === "docker") {
      const focusDocker = () => {
        const el = $("#panel-docker .docker-panel") || $("#dockerCounts");
        el?.scrollIntoView({ behavior: "instant", block: "start" });
      };
      focusDocker();
      refreshDocker()
        .catch(() => {})
        .finally(() => {
          requestAnimationFrame(focusDocker);
          setTimeout(focusDocker, 50);
        });
      startDockerPolling();
    } else if (name === "experiments") {
      refreshLabStatus().catch(() => {});
      startExperimentsPolling(false);
    } else if (name === "reshard") {
      refreshBonusLive().catch(() => {});
      startBonusPolling();
    } else if (name === "heal") {
      refreshHeal().catch(() => {});
      startHealPolling();
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
  // dockerRefreshBtn bound below with optional chaining
  $("#labRefreshBtn")?.addEventListener("click", () => refreshLabStatus());
  $("#labClearBtn")?.addEventListener("click", () => clearLabLiveBoard());
  $("#labClearReqBtn")?.addEventListener("click", () => clearLabLiveBoard());

  $("#bonusTrafficStart")?.addEventListener("click", async () => {
    await fetch("/api/bonus/traffic/start", { method: "POST" });
    refreshBonusLive().catch(() => {});
  });
  $("#bonusTrafficStop")?.addEventListener("click", async () => {
    await fetch("/api/bonus/traffic/stop", { method: "POST" });
    refreshBonusLive().catch(() => {});
  });
  $("#bonusClearLogs")?.addEventListener("click", async () => {
    await fetch("/api/bonus/logs/clear", { method: "POST" });
    bonusMigLogCount = 0;
    bonusLogClearedAt = Date.now() / 1000;
    const stream = $("#bonusReqStream");
    if (stream) {
      stream.innerHTML =
        `<div class="bonus-req-empty">Logs cleared. Start traffic to stream again.</div>`;
    }
    const migLog = $("#bonusMigLog");
    if (migLog) migLog.innerHTML = "";
    refreshBonusLive().catch(() => {});
  });
  $("#bonusResetState")?.addEventListener("click", async () => {
    const btn = $("#bonusResetState");
    if (btn) btn.disabled = true;
    try {
      await fetch("/api/bonus/traffic/stop", { method: "POST" });
      const res = await fetch("/api/bonus/reset", { method: "POST" });
      const data = await res.json();
      bonusMigLogCount = 0;
      bonusLogClearedAt = Date.now() / 1000;
      const stream = $("#bonusReqStream");
      if (stream) {
        stream.innerHTML =
          `<div class="bonus-req-empty">Reset done. B/C restored. Start traffic when ready.</div>`;
      }
      const migLog = $("#bonusMigLog");
      if (migLog) migLog.innerHTML = "";
      if (!res.ok || data.ok === false) {
        alert(`Reset failed: ${data.error || res.status}`);
      }
      await refreshBonusLive();
    } finally {
      if (btn) btn.disabled = false;
    }
  });
  $("#bonusMigrateStart")?.addEventListener("click", async () => {
    bonusMigLogCount = 0;
    bonusLogClearedAt = 0;
    await fetch("/api/bonus/migrate/start", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ force_new: true }),
    });
    refreshBonusLive().catch(() => {});
  });
  $("#bonusMigrateStop")?.addEventListener("click", async () => {
    const res = await fetch("/api/bonus/migrate/stop", { method: "POST" });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || data.ok === false) {
      alert(`Stop failed: ${data.error || res.status}`);
    }
    refreshBonusLive().catch(() => {});
  });
  $("#bonusMigrateResume")?.addEventListener("click", async () => {
    bonusMigLogCount = 0;
    const res = await fetch("/api/bonus/migrate/resume", { method: "POST" });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || data.ok === false) {
      alert(`Resume failed: ${data.error || res.status}`);
    }
    refreshBonusLive().catch(() => {});
  });

  $("#healTrafficStart")?.addEventListener("click", async () => {
    await fetch("/api/heal/enable", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ enabled: true }),
    });
    await fetch("/api/heal/traffic/start", { method: "POST" });
    healUi.message = "Traffic started. Crash a replica when ready.";
    refreshHeal().catch(() => {});
  });
  $("#healTrafficStop")?.addEventListener("click", async () => {
    await fetch("/api/heal/traffic/stop", { method: "POST" });
    healUi.message = "Traffic stopped.";
    refreshHeal().catch(() => {});
  });
  $("#healDemoCrashA")?.addEventListener("click", async () => {
    const btn = $("#healDemoCrashA");
    if (btn) btn.disabled = true;
    try {
      await fetch("/api/heal/enable", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ enabled: true }),
      });
      await fetch("/api/heal/traffic/start", { method: "POST" });
      healUi.message = "Traffic on — crashing A-0…";
      await refreshHeal().catch(() => {});
      await new Promise((r) => setTimeout(r, 1200));
      await healCrash("A-0");
    } finally {
      if (btn) btn.disabled = false;
    }
  });
  $("#panel-heal")?.addEventListener("click", (e) => {
    const btn = e.target.closest("[data-heal-crash]");
    if (!btn || btn.disabled) return;
    e.preventDefault();
    healCrash(btn.getAttribute("data-heal-crash"));
  });

  $("#labExpCards")?.addEventListener("click", (e) => {
    const btn = e.target.closest("[data-lab-run]");
    if (!btn) return;
    runLabExperiment(btn.getAttribute("data-lab-run"));
  });

  $("#dockerContainers")?.addEventListener("click", (e) => {
    const btn = e.target.closest("[data-docker-action]");
    if (!btn) return;
    const action = btn.getAttribute("data-docker-action");
    const id = btn.getAttribute("data-id");
    const name = btn.getAttribute("data-name") || id;
    if (!action || !id) return;
    runDockerAction(id, action, name);
  });
  $("#dockerRefreshBtn")?.addEventListener("click", () => {
    refreshDocker().catch(() => {});
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
      clearInterval(labPollTimer);
      clearInterval(bonusPollTimer);
      clearInterval(healPollTimer);
    } else if (activeTab === "lab") {
      startLabPolling();
    } else if (activeTab === "docker") {
      startDockerPolling();
    } else if (activeTab === "experiments") {
      startExperimentsPolling(false);
    } else if (activeTab === "reshard") {
      startBonusPolling();
    } else if (activeTab === "heal") {
      startHealPolling();
    }
  });
})();
