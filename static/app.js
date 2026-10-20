(() => {
  const $ = (s, r = document) => r.querySelector(s);
  const $$ = (s, r = document) => [...r.querySelectorAll(s)];

  let sessionId = localStorage.getItem("sa_session") || null;
  let modelId = null;
  let topicsDirty = [];

  const md = (text) => {
    if (window.marked) return marked.parse(text || "");
    const esc = (text || "").replace(/&/g, "&amp;").replace(/</g, "&lt;");
    return esc.split(/\n{2,}/).map(p => `<p>${p.replace(/\n/g, "<br>")}</p>`).join("");
  };

  const fmtEta = (sec) => {
    if (sec == null) return "";
    if (sec < 60) return `~${Math.ceil(sec)} сек`;
    return `~${Math.ceil(sec / 60)} мин`;
  };

  const fmtTime = (ts) => new Date(ts * 1000).toLocaleTimeString("ru-RU");

  // ---------- статус-бар ----------
  const statusBar = $("#statusBar");
  $("#statusHead").addEventListener("click", () => statusBar.classList.toggle("collapsed"));

  function pushEvent(ev) {
    const log = $("#statusLog");
    const div = document.createElement("div");
    div.className = `ev ${ev.status}`;
    div.innerHTML = `<span class="t">${fmtTime(ev.ts)}</span><span class="m"></span>`;
    div.querySelector(".m").textContent = ev.message;
    log.appendChild(div);
    log.scrollTop = log.scrollHeight;

    $("#statusText").textContent = ev.message;
    $("#statusSpinner").classList.toggle("hidden", ev.status !== "running");
    if (ev.progress != null) $("#statusProgressFill").style.width = `${ev.progress}%`;
    $("#statusEta").textContent = ev.status === "running" && ev.eta ? `осталось ${fmtEta(ev.eta)}` : "";

    if (ev.stage === "ingest" && ev.status === "done") onIngestDone();
    if (ev.stage === "topics" && ev.status === "done" && ev.message.includes("Найдено тем")) loadTopics();
    if (ev.stage === "cards" && ev.topic && ev.topic.startsWith("card:")) loadCards();
    if (ev.stage === "finish") loadCards();
    if (ev.stage === "cleanup" && ev.status === "done") resetUi();
  }

  function connectStream() {
    if (!sessionId) return;
    const es = new EventSource(`/api/stream?session_id=${sessionId}`);
    es.onmessage = (e) => { try { pushEvent(JSON.parse(e.data)); } catch {} };
    es.onerror = () => { es.close(); setTimeout(connectStream, 4000); };
  }

  // ---------- вкладки ----------
  $$(".tab").forEach(btn => btn.addEventListener("click", () => {
    $$(".tab").forEach(b => b.classList.toggle("active", b === btn));
    $$(".tab-page").forEach(p => p.classList.toggle("active", p.id === `tab-${btn.dataset.tab}`));
    if (btn.dataset.tab === "corpus") loadCorpusNav();
    if (btn.dataset.tab === "cards") loadCards();
  }));

  // ---------- загрузка ----------
  const dz = $("#dropzone"), fi = $("#fileInput");
  dz.addEventListener("click", () => fi.click());
  dz.addEventListener("dragover", e => { e.preventDefault(); dz.classList.add("dragover"); });
  dz.addEventListener("dragleave", () => dz.classList.remove("dragover"));
  dz.addEventListener("drop", e => { e.preventDefault(); dz.classList.remove("dragover"); uploadFiles(e.dataTransfer.files); });
  fi.addEventListener("change", () => uploadFiles(fi.files));

  async function uploadFiles(files) {
    const pdfs = [...files].filter(f => f.name.toLowerCase().endsWith(".pdf"));
    if (!pdfs.length) return;
    const fd = new FormData();
    fd.append("session_id", sessionId);
    pdfs.forEach(f => fd.append("files", f));
    pdfs.forEach(f => {
      const li = document.createElement("li");
      li.innerHTML = `<span>${f.name}</span><span class="st">загрузка…</span>`;
      $("#fileList").appendChild(li);
    });
    const res = await fetch("/api/upload", { method: "POST", body: fd });
    if (!res.ok) { alert("Ошибка загрузки: " + (await res.text())); return; }
    $$("#fileList .st").forEach(s => { s.textContent = "обрабатывается…"; s.classList.add("ok"); });
  }

  function onIngestDone() {
    $("#btnExtractTopics").disabled = false;
    $("#corpusInfo").textContent = "Корпус готов. Можно извлекать темы.";
    $$("#fileList .st").forEach(s => { s.textContent = "готово"; s.classList.add("ok"); });
  }

  $("#btnExtractTopics").addEventListener("click", async () => {
    $("#btnExtractTopics").disabled = true;
    const res = await fetch("/api/graph/start", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ session_id: sessionId, model_id: modelId })
    });
    if (!res.ok) {
      const d = await res.json().catch(() => ({}));
      alert(d.detail || `Ошибка запуска (${res.status})`);
      $("#btnExtractTopics").disabled = false;
    }
  });

  // ---------- темы ----------
  async function loadTopics() {
    const res = await fetch(`/api/topics?session_id=${sessionId}`);
    const data = await res.json();
    topicsDirty = data.topics || [];
    renderTopics();
    if (topicsDirty.length) $('[data-tab="topics"]').click();
  }

  function renderTopics() {
    const list = $("#topicsList");
    list.innerHTML = "";
    topicsDirty.forEach((t, idx) => {
      const node = $("#tplTopic").content.cloneNode(true);
      const title = $(".topic-title", node);
      const view = $(".topic-summary-view", node);
      const summary = $(".topic-summary", node);
      title.value = t.title;
      title.addEventListener("input", () => topicsDirty[idx].title = title.value);

      const renderView = () => {
        const text = (topicsDirty[idx].summary || "").trim();
        view.innerHTML = text
          ? md(text)
          : `<span class="hint">Краткое описание — нажмите, чтобы добавить (Markdown)</span>`;
      };
      const grow = () => {
        summary.style.height = "auto";
        summary.style.height = `${summary.scrollHeight + 4}px`;
      };
      view.addEventListener("click", () => {
        view.classList.add("hidden");
        summary.classList.remove("hidden");
        summary.value = topicsDirty[idx].summary || "";
        grow();
        summary.focus();
      });
      summary.addEventListener("input", () => { topicsDirty[idx].summary = summary.value; grow(); });
      summary.addEventListener("blur", () => {
        summary.classList.add("hidden");
        view.classList.remove("hidden");
        renderView();
      });
      renderView();

      $(".topic-del", node).addEventListener("click", () => { topicsDirty.splice(idx, 1); renderTopics(); });
      list.appendChild(node);
    });
  }

  $("#btnAddTopic").addEventListener("click", () => {
    topicsDirty.push({ title: "", summary: "", pages: [] });
    renderTopics();
    const inputs = $$("#topicsList .topic-title");
    inputs[inputs.length - 1]?.focus();
  });

  $("#btnConfirmTopics").addEventListener("click", async () => {
    const topics = topicsDirty.filter(t => t.title.trim());
    if (!topics.length) { alert("Список тем пуст"); return; }
    $("#btnConfirmTopics").disabled = true;
    await fetch("/api/topics/confirm", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ session_id: sessionId, model_id: modelId, topics })
    });
    $("#btnConfirmTopics").disabled = false;
    $('[data-tab="cards"]').click();
  });

  // ---------- очистка ----------
  $("#btnCleanup").addEventListener("click", async () => {
    if (!confirm("Удалить загруженные PDF, корпус, темы и карточки этой сессии?")) return;
    const res = await fetch("/api/cleanup", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ session_id: sessionId })
    });
    if (!res.ok) {
      const d = await res.json().catch(() => ({}));
      alert(d.detail || "Не удалось очистить данные");
      return;
    }
    resetUi();
  });

  function resetUi() {
    topicsDirty = [];
    renderTopics();
    cardNodes.clear();
    $("#cardsList").innerHTML = "";
    corpusDocs = [];
    $("#corpusNav").innerHTML = "";
    $("#corpusContent").innerHTML = `<p class="hint">Корпус пуст — загрузите PDF.</p>`;
    $("#fileList").innerHTML = "";
    $("#btnExtractTopics").disabled = true;
    $("#corpusInfo").textContent = "";
    $("#statusLog").innerHTML = "";
    $("#statusText").textContent = "Ожидание…";
    $("#statusEta").textContent = "";
    $("#statusProgressFill").style.width = "0";
    $("#statusSpinner").classList.add("hidden");
    $('[data-tab="upload"]').click();
  }

  // ---------- карточки ----------
  const cardNodes = new Map(); // topic -> {id, node}

  async function loadCards() {
    const res = await fetch(`/api/cards?session_id=${sessionId}`);
    const data = await res.json();
    const cards = data.cards || [];
    const current = new Set(cards.map(c => c.topic));
    for (const [topic, entry] of [...cardNodes]) {
      if (!current.has(topic)) { entry.node.remove(); cardNodes.delete(topic); }
    }
    for (const c of cards) {
      const entry = cardNodes.get(c.topic);
      if (entry && entry.id === c.id) continue;
      const node = await renderCard(c.id);
      if (entry) entry.node.replaceWith(node);
      cardNodes.set(c.topic, { id: c.id, node });
    }
  }

  async function renderCard(cardId) {
    const res = await fetch(`/api/cards/${cardId}`);
    const card = await res.json();
    const node = $("#tplCard").content.cloneNode(true);
    const root = $(".card", node);
    $(".card-title", node).textContent = card.topic;
    const badge = $(".card-badge", node);
    if (card.has_errors) { badge.textContent = "есть ошибки в теории"; badge.classList.add("err"); }
    else { badge.textContent = "теория корректна"; badge.classList.add("ok"); }

    $(".sec-original", node).innerHTML = md(card.original_md);
    $(".sec-simple", node).innerHTML = md(card.simplified_md);

    const errWrap = $(".sec-errors-wrap", node);
    if (card.has_errors && card.errors && card.errors.length) {
      $(".sec-errors", node).innerHTML = card.errors.map(e =>
        `<div class="example-item"><div><b>Было:</b> ${e.quote || ""}</div>
         <div class="rel"><b>Ошибка:</b> ${e.issue || ""}</div>
         <div class="snip"><b>Правильно:</b> ${e.correction || ""}</div></div>`).join("");
    } else {
      errWrap.style.display = "none";
    }

    const exBox = $(".sec-examples", node);
    if (card.examples && card.examples.length) {
      exBox.innerHTML = card.examples.map(e =>
        `<div class="example-item">
           ${e.url ? `<a href="${e.url}" target="_blank" rel="noopener">${e.title || e.url}</a>` : `<b>${e.title || "Пример"}</b>`}
           ${e.relevance ? `<div class="rel">${e.relevance}</div>` : ""}
           ${e.snippet ? `<div class="snip">${e.snippet}</div>` : ""}
         </div>`).join("");
    } else {
      exBox.innerHTML = `<p class="hint">Примеры не найдены.</p>`;
    }

    $(".card-head", node).addEventListener("click", () => root.classList.toggle("open"));

    const log = $(".chat-log", node);
    for (const m of card.chat || []) addChatMsg(log, m.role, m.content);

    const input = $(".chat-input", node);
    const send = async () => {
      const text = input.value.trim();
      if (!text) return;
      input.value = "";
      addChatMsg(log, "user", text);
      const thinking = addChatMsg(log, "assistant", "…");
      const r = await fetch(`/api/cards/${cardId}/chat${modelId ? `?model_id=${modelId}` : ""}`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ message: text })
      });
      const d = await r.json();
      thinking.innerHTML = r.ok ? md(d.answer) : `Ошибка: ${d.detail || r.status}`;
      log.scrollTop = log.scrollHeight;
    };
    $(".chat-send", node).addEventListener("click", send);
    input.addEventListener("keydown", e => { if (e.key === "Enter") send(); });

    $("#cardsList").appendChild(node);
  }

  function addChatMsg(log, role, content) {
    const div = document.createElement("div");
    div.className = `chat-msg ${role}`;
    div.innerHTML = role === "assistant" ? md(content) : "";
    if (role !== "assistant") div.textContent = content;
    log.appendChild(div);
    log.scrollTop = log.scrollHeight;
    return div;
  }

  // ---------- корпус ----------
  let corpusDocs = [];

  async function loadCorpusNav() {
    const res = await fetch(`/api/corpus?session_id=${sessionId}`);
    const data = await res.json();
    corpusDocs = data.docs || [];
    const nav = $("#corpusNav");
    nav.innerHTML = "";
    corpusDocs.forEach((d, i) => {
      const li = document.createElement("li");
      li.textContent = `${d.source} — стр. ${d.page}`;
      li.addEventListener("click", () => {
        $$("#corpusNav li").forEach(x => x.classList.toggle("active", x === li));
        $("#corpusContent").innerHTML = md(`# ${d.source} — стр. ${d.page}\n\n${d.content}`);
      });
      nav.appendChild(li);
    });
    if (!corpusDocs.length) $("#corpusContent").innerHTML = `<p class="hint">Корпус пуст — загрузите PDF.</p>`;
  }

  // ---------- init ----------
  async function init() {
    const mres = await fetch("/api/models");
    const mdata = await mres.json();
    const sel = $("#modelSelect");
    sel.innerHTML = "";
    mdata.models.forEach(m => {
      const opt = document.createElement("option");
      opt.value = m.id;
      opt.textContent = m.label;
      sel.appendChild(opt);
    });
    modelId = mdata.default;
    sel.value = modelId;
    sel.addEventListener("change", () => modelId = sel.value);

    if (!sessionId) {
      const sres = await fetch("/api/session", { method: "POST" });
      sessionId = (await sres.json()).session_id;
      localStorage.setItem("sa_session", sessionId);
    }
    connectStream();

    const cres = await fetch(`/api/corpus?session_id=${sessionId}`);
    const cdata = await cres.json();
    if ((cdata.docs || []).length) {
      $("#btnExtractTopics").disabled = false;
      $("#corpusInfo").textContent = `В корпусе ${cdata.docs.length} страниц.`;
    }
    const tres = await fetch(`/api/topics?session_id=${sessionId}`);
    const tdata = await tres.json();
    if ((tdata.topics || []).length) { topicsDirty = tdata.topics; renderTopics(); }
    loadCards();
  }

  init();
})();
