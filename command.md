# Commandes du projet Olist

Exécute les commandes depuis la racine du projet.

```bash
cd ~/projects/data_lakehouse
```

## Airflow

Construire l’image et démarrer Airflow lors de la première installation.

```bash
AIRFLOW_UID=$(id -u) docker compose -f airflow/docker-compose.yaml up -d --build
```

Démarrer Airflow avec l’image déjà construite.

```bash
AIRFLOW_UID=$(id -u) docker compose -f airflow/docker-compose.yaml up -d
```

Ouvre [localhost:8080](http://localhost:8080). Utilise `airflow` comme
identifiant et comme mot de passe.

Vérifier l’état du conteneur et suivre les logs du service.

```bash
docker compose -f airflow/docker-compose.yaml ps
docker compose -f airflow/docker-compose.yaml logs -f airflow
```

`Ctrl+C` quitte le suivi des logs. Airflow continue de tourner.

Arrêter Airflow. Attends la fin d’un traitement en cours avant de l’arrêter.

```bash
docker compose -f airflow/docker-compose.yaml down
```

Cette commande garde les données Olist et le volume qui contient l’historique
et les logs Airflow.

## Exécuter un DAG

Activer le DAG incrémental pour qu’il vérifie `data/incoming/` au début
de chaque heure UTC.

```bash
docker compose -f airflow/docker-compose.yaml exec airflow airflow dags unpause olist_incremental_orders
```

Déclencher une vérification immédiate, par exemple pour tester un batch
sans attendre la prochaine heure.

```bash
docker compose -f airflow/docker-compose.yaml exec airflow airflow dags trigger olist_incremental_orders
```

Consulter l’historique des exécutions ou ouvrir **Grid** dans l’interface.
Une tâche `apply_orders_batches` marquée **Skipped** indique qu’aucun batch
n’était à traiter.

```bash
docker compose -f airflow/docker-compose.yaml exec airflow airflow dags list-runs olist_incremental_orders
```

Lancer le DAG manuel de reconstruction complète. Il relit les CSV de
`data/raw/` et reconstruit Bronze, Silver et Gold. Une fois des batches
incrémentaux appliqués, utilise un répertoire de données séparé pour un
full refresh afin de préserver les mises à jour.

```bash
docker compose -f airflow/docker-compose.yaml exec airflow airflow dags unpause olist_full_refresh
docker compose -f airflow/docker-compose.yaml exec airflow airflow dags trigger olist_full_refresh
```

## Déposer un CSV de commandes

Remplace le chemin source et choisis un nom de batch encore inutilisé.
Le fichier doit reprendre les colonnes du CSV original des commandes.

```bash
mkdir -p data/incoming
head -n 1 data/raw/olist_orders_dataset.csv
head -n 1 /chemin/vers/mes_commandes.csv
cp /chemin/vers/mes_commandes.csv data/incoming/.orders_batch_001.tmp &&
  mv -n data/incoming/.orders_batch_001.tmp data/incoming/orders_batch_001.csv
```

Le fichier devient visible pour Airflow une fois la copie terminée et le fichier
renommé. Laisse le DAG planifié le récupérer ou déclenche-le manuellement.

## Dashboard Streamlit

Installe les dépendances à la première utilisation. Si le dashboard tourne déjà,
ouvre directement [localhost:8501](http://localhost:8501).

```bash
.venv/bin/python -m pip install -r dashboard/requirements.txt
.venv/bin/python -m streamlit run dashboard/app.py --server.address 127.0.0.1
```

Fais **Ctrl+C** dans le terminal où Streamlit a été lancé pour l’arrêter.
Si tu n’as plus ce terminal, retrouve le PID de cette commande, puis remplace
`12345` par le PID affiché.

```bash
pgrep -af '[s]treamlit run dashboard/app.py'
kill 12345
```

Le bouton **Actualiser** relit les tables Gold après un traitement réussi.

## Pipeline Spark en terminal

Lancer les tests unitaires avec les dépendances du pipeline.

```bash
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -B -m unittest discover -s tests
```

Faire une reconstruction manuelle dans un dossier de sortie séparé. Assure-toi
que Java est installé et choisis un dossier de sortie neuf.

```bash
.venv/bin/python main.py --input-dir data/raw --output-dir /tmp/olist-full-refresh
```

Pour arrêter ce lancement manuel, fais **Ctrl+C** dans son terminal.
