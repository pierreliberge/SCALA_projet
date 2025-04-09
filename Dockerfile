FROM openjdk:11-jre-slim

# Copier le fichier JAR dans le conteneur
COPY ./target/scala_spark_mosef_vazelle-1.1-jar-with-dependencies.jar /app/scala-spark.jar

# Créer un répertoire pour les données et les résultats
RUN mkdir /data

# Définir les variables d'environnement
ENV MASTER_URL="local[1]"
ENV SRC_PATH="/data/rappelconso0.csv"
ENV DST_PATH="/data/output"
ENV FORMAT="csv"

# On supprime la directive VOLUME qui utilisait /tmp/output
# VOLUME ["/tmp/output"]

# Utiliser la shell form pour que les variables d'environnement soient interprétées
ENTRYPOINT /bin/sh -c "/usr/local/openjdk-11/bin/java -jar /app/scala-spark.jar ${MASTER_URL} ${SRC_PATH} ${DST_PATH} ${FORMAT}"

# Commande par défaut (possibilité de remplacer lors de l'exécution)
CMD ["local[1]", "/data/rappelconso0.csv", "/data/output", "csv"]
