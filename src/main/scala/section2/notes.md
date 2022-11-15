# Section 2 - Notes

## Run Spark Shell in the cluster's master node

Start Docker Cluster and run bash in the master container:

```bash
$ docker compose up
# Copy data to the cluster's master node
$ docker cp src/main/resources/data spark-cluster-spark-master-1:/tmp
$ docker exec -it spark-cluster-spark-master-1 bash
```

Then run the spark shell:

```bash
$ cd spark/bin
$ ./spark-shell
```

Then open [localhost:4040](http://localhost:4040) on your browser for the Web View.

## Run `TestDeploy` in Docker Cluster

Configure and build the spark-optimization artifact with IntelliJ.
Then copy `spark-optimization.jar` to `spark-cluster/apps` and the movies JSON to `spark-cluster/data`.

Start the Docker cluster, run bash in the master container and start the job:

```bash
# Get the container's hostname
$ cat /etc/hostname
# 53b3e4a8905e

# Submit the job
$ /spark/bin/spark-submit \
  --class section2.TestDeploy \
  --master spark://53b3e4a8905e:7077 \
  --deploy-mode client \
  --verbose \
  --supervise \
  /opt/spark-apps/spark-optimization.jar \
  /opt/spark-data/movies/movies.json \
  /opt/spark-data/movies/good_comedies.json

# We can also configure the Spark Session in the command line with:
  --conf spark.executor.memory 1g \
# Or:
  --executor-memory 1g \
```
