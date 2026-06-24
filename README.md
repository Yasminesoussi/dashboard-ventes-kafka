# Kafka Sales Stream Dashboard

Application de demonstration autour d'Apache Kafka et Kafka Streams.

Le projet permet de simuler des ventes depuis une interface web, d'envoyer ces ventes dans un topic Kafka, de calculer le total des ventes par produit avec Kafka Streams, puis d'afficher les resultats en temps reel dans le navigateur.

## Fonctionnalites

- Envoi de ventes depuis une interface web.
- Production des messages dans le topic Kafka `ventes`.
- Traitement temps reel avec Kafka Streams.
- Aggregation du total des ventes par produit.
- Publication des resultats dans le topic `ventes_total`.
- Affichage en direct des ventes et des totaux via Server-Sent Events.

## Architecture

```text
Navigateur
   |
   | formulaire de vente
   v
Node.js / Express / KafkaJS
   |
   | produit dans le topic ventes
   v
Apache Kafka
   |
   | lu par Kafka Streams
   v
Application Java Kafka Streams
   |
   | ecrit dans ventes_total
   v
Node.js / Express
   |
   | SSE
   v
Interface web temps reel
```

## Structure du projet

```text
.
+-- demo-ui/
|   +-- server.js
|   +-- package.json
|   +-- public/
|       +-- index.html
|       +-- app.js
|       +-- styles.css
+-- kafka-streams-app/
|   +-- pom.xml
|   +-- src/main/java/com/monprojet/KafkaStreamsApp.java
+-- kafka_2.13-4.2.0/
    +-- distribution locale Apache Kafka
```

## Technologies utilisees

- Apache Kafka 4.2.0
- Kafka Streams
- Java 17
- Maven
- Node.js
- Express
- KafkaJS
- HTML, CSS et JavaScript

## Prerequis

- Java 17 ou plus
- Maven
- Node.js et npm
- Apache Kafka disponible localement sur `localhost:9092`

## Installation

Installer les dependances Node.js :

```bash
cd demo-ui
npm install
```

Compiler l'application Kafka Streams :

```bash
cd ../kafka-streams-app
mvn clean compile
```

## Lancement

Ouvrir plusieurs terminaux.

### 1. Demarrer Kafka

Depuis le dossier Kafka :

```bash
cd kafka_2.13-4.2.0
.\bin\windows\kafka-server-start.bat .\config\kraft\server.properties
```

Si Kafka n'a jamais ete initialise en mode KRaft sur la machine, generer un identifiant de cluster puis formater le stockage :

```bash
.\bin\windows\kafka-storage.bat random-uuid
.\bin\windows\kafka-storage.bat format -t <CLUSTER_ID> -c .\config\kraft\server.properties
```

### 2. Creer les topics

Dans un autre terminal :

```bash
cd kafka_2.13-4.2.0
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --create --if-not-exists --topic ventes
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --create --if-not-exists --topic ventes_total
```

### 3. Lancer l'application Kafka Streams

```bash
cd kafka-streams-app
mvn exec:java -Dexec.mainClass="com.monprojet.KafkaStreamsApp"
```

L'application lit le topic `ventes`, calcule le total par produit et ecrit les resultats dans `ventes_total`.

### 4. Lancer l'interface web

```bash
cd demo-ui
npm start
```

L'interface est disponible sur :

```text
http://localhost:3000
```

## Format des messages Kafka

Message envoye dans `ventes` :

```text
vente_id=1710000000000;produit=Ordinateur;quantite=2;prix=1200
```

Message produit dans `ventes_total` :

```text
Ordinateur : 2400.00
```

## API

### Envoyer une vente

```http
POST /api/vente
Content-Type: application/json
```

Exemple :

```json
{
  "produit": "Ordinateur",
  "quantite": 2,
  "prix": 1200
}
```

### Recuperer l'etat courant

```http
GET /api/snapshot
```

### Flux temps reel

```http
GET /api/stream
```

Le flux utilise Server-Sent Events pour mettre l'interface a jour automatiquement.

## Variables d'environnement

| Variable | Description | Valeur par defaut |
| --- | --- | --- |
| `PORT` | Port du serveur web | `3000` |
| `KAFKA_BROKERS` | Liste des brokers Kafka | `localhost:9092` |
| `VENTES_FEED_MAX` | Nombre maximum de ventes gardees en memoire | `50000` |
| `VENTES_TOTAL_FEED_MAX` | Nombre maximum de totaux gardes en memoire | `50000` |
| `NO_OPEN` | Mettre `1` pour ne pas ouvrir le navigateur automatiquement | non defini |

Exemple :

```bash
NO_OPEN=1 PORT=4000 npm start
```

## Conseils avant de pousser sur Git

Il est recommande de ne pas versionner :

- `node_modules/`
- `target/`
- les logs Kafka
- les donnees Kafka locales dans `kraft-logs/`
- la distribution complete Kafka si elle peut etre telechargee separement

Un repository plus propre contient surtout :

- le code Java dans `kafka-streams-app/`
- le code Node.js et frontend dans `demo-ui/`
- le `README.md`
- un `.gitignore`

## Nom de repository propose

`kafka-sales-stream-dashboard`

## Description courte

Dashboard temps reel de ventes avec Apache Kafka, Kafka Streams, Node.js et une interface web.
