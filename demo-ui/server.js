
//serveur backend (Node.js + Express) qui : envoie les ventes à Apache Kafka  , lit les résultats  ,envoie les données au frontend (ta page)

"use strict";

const express = require("express");
const path = require("path");
const { exec } = require("child_process");
const { Kafka } = require("kafkajs");


//CONFIGURATION
const PORT = Number(process.env.PORT) || 3000;
const BROKERS = (process.env.KAFKA_BROKERS || "localhost:9092")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

/** 0 = illimité (démo uniquement — risque mémoire si topic énorme) */
const MAX_VENTES_FEED = (() => {
  const v = process.env.VENTES_FEED_MAX;
  if (v === "0") return 0;
  const n = Number(v);
  return Number.isFinite(n) && n > 0 ? n : 50000;
})();

const MAX_TOTAL_FEED = (() => {
  const v = process.env.VENTES_TOTAL_FEED_MAX;
  if (v === "0") return 0;
  const n = Number(v);
  return Number.isFinite(n) && n > 0 ? n : 50000;
})();

//CRÉER SERVEUR EXPRESS
const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));


//cONNEXION À KAFKA
const kafka = new Kafka({
  clientId: "demo-ui",
  brokers: BROKERS,
});


//PRODUCER (envoie messages à Kafka)
const producer = kafka.producer();


//CONSUMERS (lisent Kafka)
/** @type {import('kafkajs').Consumer | null} */
let consumerTotaux = null;
/** @type {import('kafkajs').Consumer | null} */
let consumerVentes = null;


//STOCKAGE DES DONNÉES
/** @type {Map<string, string>} */
const totals = new Map();
const recentVentes = [];
/** Historique brut de tous les messages lus sur ventes_total (ordre Kafka) */
const historiqueTotaux = [];
/** @type {Set<import('http').ServerResponse>} */
const sseClients = new Set();

let snapshotDebounceTimer = null;

//ENVOYER DONNÉES AU FRONTEND (temps réel)

function broadcast(event, data) {
  const payload = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const res of sseClients) {
    try {
      res.write(payload);
    } catch (_) {}
  }
}

function scheduleSnapshotBroadcast() {
  if (snapshotDebounceTimer) clearTimeout(snapshotDebounceTimer);
  snapshotDebounceTimer = setTimeout(() => {
    snapshotDebounceTimer = null;
    broadcast("snapshot", snapshotPayload());
  }, 120);
}

function parseTotalValue(value) {
  const idx = value.lastIndexOf(" : ");
  if (idx === -1) return null;
  return {
    produit: value.slice(0, idx),
    total: value.slice(idx + 3),
  };
}

function snapshotPayload() {
  return {
    totals: Object.fromEntries(totals),
    recent: [...recentVentes],
    recentTotaux: [...historiqueTotaux],
  };
}


//API → récupérer données (chargement)
app.get("/api/snapshot", (req, res) => {
  res.json(snapshotPayload());
});


//API → connexion temps réel (SSE)
app.get("/api/stream", (req, res) => {
  res.setHeader("Content-Type", "text/event-stream; charset=utf-8");
  res.setHeader("Cache-Control", "no-cache, no-transform");
  res.setHeader("Connection", "keep-alive");
  if (typeof res.flushHeaders === "function") res.flushHeaders();
  sseClients.add(res);

  //// envoyer données initiales
  res.write(`event: snapshot\ndata: ${JSON.stringify(snapshotPayload())}\n\n`);

  // // garder connexion active
  const keepAlive = setInterval(() => {
    try {
      res.write(":ping\n\n");
    } catch (_) {}
  }, 20000);
  req.on("close", () => {
    clearInterval(keepAlive);
    sseClients.delete(res);
  });
});


// API → envoyer une vente
app.post("/api/vente", async (req, res) => {

  //récupérer données
  const { produit, quantite, prix } = req.body || {};

  //vérifier données
  if (!produit || quantite == null || prix == null) {
    return res
      .status(400)
      .json({ error: "produit, quantite et prix requis" });
  }
  const q = Number(quantite);
  const p = Number(prix);
  if (Number.isNaN(q) || Number.isNaN(p)) {
    return res.status(400).json({ error: "quantite et prix numériques" });
  }

  //créer message Kafka
  const id = Date.now();
  const value = `vente_id=${id};produit=${produit};quantité=${q};prix=${p}`;
  try {

    //envoyer vers Kafka (topic ventes)
    await producer.send({
      topic: "ventes",
      messages: [{ key: String(produit), value }],
    });
    res.json({ ok: true });
  } catch (e) {
    console.error(e);
    res.status(503).json({ error: e.message || "Kafka indisponible" });
  }
});


//LIRE topic ventes_total
function runConsumerTotaux() {
  if (!consumerTotaux) return Promise.resolve();
  return consumerTotaux.run({
    eachMessage: async ({ message }) => {
      const val = message.value?.toString();
      if (!val) return;
      historiqueTotaux.push(val);
      if (MAX_TOTAL_FEED > 0 && historiqueTotaux.length > MAX_TOTAL_FEED) {
        historiqueTotaux.shift();
      }
      const parsed = parseTotalValue(val);
      if (parsed) {
        totals.set(parsed.produit, parsed.total);
        broadcast("total", parsed);
      }
      scheduleSnapshotBroadcast();
    },
  });
}


//LIRE topic ventes
function runConsumerVentes() {
  if (!consumerVentes) return Promise.resolve();
  return consumerVentes.run({
    eachMessage: async ({ message }) => {
      const raw = message.value?.toString();
      if (!raw) return;
      recentVentes.push(raw);
      if (MAX_VENTES_FEED > 0 && recentVentes.length > MAX_VENTES_FEED) {
        recentVentes.shift();
      }
      scheduleSnapshotBroadcast();
    },
  });
}

//OUVRIR NAVIGATEUR AUTO
function openBrowser(url) {
  if (process.env.NO_OPEN === "1") return;
  const cmd =
    process.platform === "win32"
      ? `start "" "${url}"`
      : process.platform === "darwin"
        ? `open "${url}"`
        : `xdg-open "${url}"`;
  exec(cmd, (err) => {
    if (err) console.error("Ouverture navigateur:", err.message);
  });
}

async function start() {
  await producer.connect();
  // Nouveau groupe + fromBeginning : tout l’historique du topic ventes_total
  consumerTotaux = kafka.consumer({
    groupId: `demo-ui-totaux-${Date.now()}`,
  });
  await consumerTotaux.connect();
  await consumerTotaux.subscribe({
    topics: ["ventes_total"],
    fromBeginning: true,
  });
  runConsumerTotaux().catch((e) => console.error("consumer totaux", e));

  // Nouveau groupe à chaque démarrage + fromBeginning : tout l’historique du topic ventes
  consumerVentes = kafka.consumer({
    groupId: `demo-ui-ventes-${Date.now()}`,
  });
  await consumerVentes.connect();
  await consumerVentes.subscribe({ topics: ["ventes"], fromBeginning: true });
  runConsumerVentes().catch((e) => console.error("consumer ventes", e));

  app.listen(PORT, () => {
    const url = `http://localhost:${PORT}`;
    console.log("Demo UI — %s", url);
    console.log(
      "(Ctrl+C pour arrêter. NO_OPEN=1 pour ne pas ouvrir le navigateur.)"
    );
    setTimeout(() => openBrowser(url), 500);
  });
}

start().catch((e) => {
  console.error(e);
  process.exit(1);
});
