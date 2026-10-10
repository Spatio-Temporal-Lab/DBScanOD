## Project Architecture and How to Run the Code
The codebase is organized into modular packages separating distributed orchestration, local partition clustering, topological graph merging, and spatio-temporal partitioning utilities.

```text
DBScanOD/
├── pom.xml                                          # Maven project dependencies and build configuration
└── src/
    └── main/
        └── scala/
            ├── org/
            │   └── DBScanOD/
            │       └── spark/
            │           └── DBScanODTest_Spark.scala # Main driver application for Spark cluster submission
            └── org/
                └── apache/
                    └── spark/
                        └── Scala/
                            ├── DBScan3DNaive/       # Core SODA engine & distributed clustering logic
                            │   ├── DBScanPoint_3D.scala        # Base 3D spatio-temporal point structure & metrics
                            │   ├── DBScanLabeledPoint_3D.scala # Point state model (Core, Border, Noise flags)
                            │   ├── DBScanCube.scala            # 3D bounding cubes, inner/outer cube calculations
                            │   ├── DBScanGraph_3D.scala        # Undirected graph for cross-partition connectivity
                            │   ├── LocalDBScan_3D.scala        # Local density-based clustering engine per executor
                            │   └── DBScan3D_CubeSplit.scala    # Distributed pipeline coordinator (Partition, Local, Merge)
                            └── utils/               # Supporting algorithms and utilities
                                ├── file/
                                │   └── FileProcess.scala       # Dataset I/O, parsing, and preprocessing
                                ├── partition/
                                │   ├── CubeSplitPartition_3D.scala # Anchor-based graph partitioning algorithm
                                │   └── EvenSplitPartition_3D.scala # Baseline uniform spatial grid partitioning
                                └── sample/
                                    └── Sample.scala            # Uniform sampling for entropy divergence estimation

## Getting Started: Compilation and Execution

This section provides a step-by-step guide to building the project, initializing the cluster environment, staging data on HDFS, and submitting the distributed clustering job using `spark-submit`.

---

### 1. Build the Package

Compile the source code and assemble the executable JAR file using Maven:

```bash
# Clean previous builds and package the JAR (skipping test suites)
mvn clean package -DskipTests

Upon successful compilation, locate the generated artifact dbscan.jar under the target/ directory and copy it to your driver/master node:

```bash
cp target/dbscan.jar ~/dbscan.jar

### 2. Start Cluster Services

Ensure that both HDFS and Spark daemons are running across all cluster nodes.

```bash
# 1. Start HDFS daemons (run on NameNode/Master node)
# (Format NameNode once on initial deployment only: hdfs namenode -format)
start-dfs.sh

# 2. Start Spark Standalone cluster (run on Master node)
start-master.sh
start-slaves.sh

# 3. Verify active processes on the master node
jps
# Expected output on master: NameNode, SecondaryNameNode, Master

# 4. Verify active processes on worker nodes
ssh server02 "jps"
# Expected output on worker: DataNode, Worker

### 3. Stage Input Data in HDFS

The input file should be a CSV containing trajectory OD records with spatial coordinates (longitude, latitude) and timestamps for both origins and destinations.

```bash
# 1. Create the input directory in HDFS
hdfs dfs -mkdir -p /dbscan/input

# 2. Upload the local dataset to HDFS
hdfs dfs -put ~/taxi_od_pairs_1b.csv /dbscan/input/

# 3. Verify file presence
hdfs dfs -ls /dbscan/input

### 4. Submit the Distributed Spark Job

Launch the clustering job on the Spark Standalone cluster via spark-submit:

```bash
spark-submit \
  --class org.DBScanOD.spark.DBScanODTest_Spark \
  --master spark://10.242.4.64:7577 \
  --deploy-mode client \
  --driver-memory 30g \
  --driver-cores 4 \
  --executor-memory 30g \
  --executor-cores 4 \
  ~/dbscan.jar \
  hdfs://10.242.4.64:9500/dbscan/input/taxi_od_pairs_1b.csv \
  hdfs://10.242.4.64:9500/dbscan/output \
  0.03 \
  1800 \
  100 \
  2.5 \
  2.5 \
  100 \
  0.3
