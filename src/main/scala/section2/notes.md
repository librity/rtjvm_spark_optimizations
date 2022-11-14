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
