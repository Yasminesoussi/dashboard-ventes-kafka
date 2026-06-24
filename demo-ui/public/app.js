//Connexion au serveur (SSE)
//Serveur envoie données (snapshot)
//Tableau + listes s’affichent
// envoies une vente
//Backend → Kafka
//Kafka Streams calcule
//Nouveau total arrive
//Page se met à jour automatiquement


(function () {
  //AU DÉMARRAGE : récupérer les éléments HTML
  const totalsBody = document.getElementById("totals-body");
  const totauxFeed = document.getElementById("totaux-feed");
  const feed = document.getElementById("feed");
  const statusEl = document.getElementById("sse-status");
  const form = document.getElementById("vente-form");
  const feedback = document.getElementById("form-feedback");
  const btnRefresh = document.getElementById("btn-refresh");


  //Variables globales
  let es = null;
  let reconnectTimer = null;
  /** @type {Record<string, string>} */
  let totalsState = {};


  //Fonction pour afficher état connexion
  function setStatus(text, isError) {
    statusEl.textContent = text;
    statusEl.className = "status" + (isError ? " error" : "");
  }

  //Fonction pour afficher tableau
  function renderTotals(map) {
    const entries = Object.entries(map || {}).sort((a, b) =>
      a[0].localeCompare(b[0], "fr")
    );
    if (!entries.length) {
      totalsBody.innerHTML =
        '<tr><td colspan="2" class="empty">Aucune donnée — envoyez une vente ou rafraîchissez.</td></tr>';
      return;
    }
    totalsBody.innerHTML = entries
      .map(
        ([produit, total]) =>
          `<tr><td>${escapeHtml(produit)}</td><td>${escapeHtml(
            String(total)
          )}</td></tr>`
      )
      .join("");
  }

  function escapeHtml(s) {
    return String(s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }


  //Ajouter ligne dans historique
  function appendFeedLine(targetEl, text) {
    const empty = targetEl.querySelector(".empty-placeholder");
    if (empty) empty.remove();
    const li = document.createElement("li");
    const t = new Date().toLocaleTimeString("fr-FR");
    li.innerHTML = `<span class="time">${t}</span>${escapeHtml(text)}`;
    targetEl.insertBefore(li, targetEl.firstChild);
  }


  //Quand serveur envoie les données (snapshot)
  function applySnapshot(data) {
    //mettre à jour totaux
    totalsState = { ...(data.totals || {}) };
    //afficher tableau
    renderTotals(totalsState);
//afficher historique ventes_total
    totauxFeed.innerHTML = "";
    if (data.recentTotaux && data.recentTotaux.length) {
      data.recentTotaux
        .slice()
        .reverse()
        .forEach((line) => appendFeedLine(totauxFeed, line));
    } else {
      totauxFeed.innerHTML =
        '<li class="empty-placeholder empty">Aucun message sur ventes_total pour l’instant.</li>';
    }

    //afficher historique ventes
    feed.innerHTML = "";
    if (data.recent && data.recent.length) {
      data.recent.slice().reverse().forEach((line) => appendFeedLine(feed, line));
    } else {
      feed.innerHTML =
        '<li class="empty-placeholder empty">Aucun message encore — utilisez le formulaire « Simuler une vente ».</li>';
    }
  }


  //Connexion temps réel
  function connectSse() {
    if (reconnectTimer) {
      clearTimeout(reconnectTimer);
      reconnectTimer = null;
    }
    if (es) {
      es.close();
      es = null;
    }

    //crée une connexion  le serveur envoie des données en continu
    setStatus("Connexion au flux…", false);
    //connexion serveur
    es = new EventSource("/api/stream");

    //recevoir données initiales
    es.addEventListener("snapshot", (e) => {
      try {
        applySnapshot(JSON.parse(e.data));
        setStatus("Flux connecté", false);
      } catch (_) {}
    });
// recevoir mise à jour (nouveau total)
    es.addEventListener("total", (e) => {
      try {
        const row = JSON.parse(e.data);
        // // mise à jour produit
        totalsState[row.produit] = row.total;
        // rafraîchir tableau
        renderTotals(totalsState);
      } catch (_) {}
    });

    //si erreur → reconnecter

    es.onerror = () => {
      setStatus("SSE interrompu — reconnexion…", true);
      es.close();
      es = null;
      reconnectTimer = setTimeout(connectSse, 2500);
    };
  }

  btnRefresh.addEventListener("click", () => {
    fetch("/api/snapshot")
      .then((r) => r.json())
      .then((data) => applySnapshot(data))
      .catch(() => {});
  });


  //Quand tu envoies une vente
  form.addEventListener("submit", (ev) => {
    ev.preventDefault();   //ne recharge pas la page
    feedback.textContent = "";
    //récupérer données
    const produit = document.getElementById("produit").value.trim();
    const quantite = document.getElementById("quantite").value;
    const prix = document.getElementById("prix").value;

    //envoyer au serveur
    fetch("/api/vente", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ produit, quantite, prix }),
    })
      .then(async (r) => {
        const j = await r.json().catch(() => ({}));
        if (!r.ok) throw new Error(j.error || r.statusText);

        //message utilisateur
        feedback.textContent = "Vente envoyée sur le topic ventes.";
        feedback.className = "msg-ok";
      })
      .catch((err) => {
        feedback.textContent = err.message || "Erreur d’envoi";
        feedback.className = "msg-err";
      });
  });


  //// 🟢 🔟 Lancer tout au démarrage
  connectSse();
})();
