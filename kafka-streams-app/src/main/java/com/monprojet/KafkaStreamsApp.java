package com.monprojet;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;


//Ce programme utilise Apache Kafka pour : 1/ Lire des ventes depuis un topic  2/Calculer le total par produit  3/Envoyer le résultat dans un autre topic
public class KafkaStreamsApp {
    public static void main(String[] args) {

        //  Configuration Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ventes-total-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // Valeur par défaut String : utilisé seulement si non précisé par Consumed/Grouped/Materialized
      
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        //  Lire le topic "ventes"
        KStream<String, String> ventes =
                builder.stream("ventes", Consumed.with(Serdes.String(), Serdes.String()));

        //  Transformer : extraire produit et calculer total
        KStream<String, Double> ventesTransformees = ventes.map((key, value) -> {
            // format: vente_id=1;produit=Ordinateur;quantité=2;prix=1200
            String[] parts = value.split(";");
            String produit = parts[1].split("=")[1];
            double quantite = Double.parseDouble(parts[2].split("=")[1]);
            double prix = Double.parseDouble(parts[3].split("=")[1]);
            double total = quantite * prix;
            return new org.apache.kafka.streams.KeyValue<>(produit, total);
        });

        //  Grouper par produit
        KGroupedStream<String, Double> grouped =
                ventesTransformees.groupByKey(Grouped.with(Serdes.String(), Serdes.Double()));

        //  Calculer la somme des ventes par produit
        KTable<String, Double> totalParProduit =
                grouped.reduce(Double::sum, Materialized.with(Serdes.String(), Serdes.Double()));

        // Valeur = "NomProduit : montant" pour que l’affichage montre toujours le nom (pas seulement le nombre)
        totalParProduit
                .toStream()
                .map((produit, total) ->
                        new KeyValue<>(produit, produit + " : " + String.format("%.2f", total)))
                .to("ventes_total", Produced.with(Serdes.String(), Serdes.String()));

        //  Lancer l'application Kafka Streams
        //Kafka Streams est une bibliothèque  de lire des données en temps réel depuis serveur Kafka, les transformer, puis envoyer le résultat automatiquement.
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Ajouter un shutdown hook pour fermer proprement
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        System.out.println("Application Kafka Streams démarrée !");

        //  Maintenir l'application active en continu
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}