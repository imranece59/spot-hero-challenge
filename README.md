# docker-spark-submit

## Docker image to run Spark applications

Performs the following tasks:
1. Gets the source code from the SCM repository
1. Builds the application
1. Runs the application by submitting it to the Spark cluster

## Building the image

Build the image:
```
docker build --no-cache -f Dockerfile.spark-2.3.0 -t docker-spark-submit .
  .
```
Make sure to run the above command inside the project folder

## Run

Run example:
```
docker run \
  -e SCM_URL="https://github.com/imranece59/spot-hero-challenge.git" \
  -e SPARK_MASTER="local[*]" \
  -e MAIN_CLASS="com.spothero.etl.ETL" \
  -e APP_ARGS="--input-file-path=/data/ --output-save-mode=false" \
  docker-spark-submit
```

## Run locally without Docker

Run the below command under project folder 

```
sbt eclipse
```

Import the project into scala eclipse and run it with master as Local[*]

```
 val sparkSession = SparkSession.builder
      .appName("ETL")
      .master("local[*]")
      .getOrCreate
```
## Command line arguments

Command line arguments are passed via environment variables for `docker` command: `-e VAR="value"`. Here is the full list:

| Name | Mandatory? | Meaning | Default value |
| ---- |:----------:| ------- | ------------- |
| SCM_URL | Yes | URL to get source code from git. | N/A |
| SCM_BRANCH | No | SCM branch to checkout. | master |
| PROJECT_SUBDIR | No | A relative directory to the root of the SCM working copy.<br>If specified, then the build will be executed in this directory, rather than in the root directory. | N/A |
| BUILD_COMMAND | No | Command to build the application. | `sbt clean assembly [creates FAT jar along with dependencies ]/sbt clean pacakage [creates program jar with compiled application classes]`<br>Means: build fat-jar using sbt-assembly plugin skipping the tests. |
| SPARK_MASTER | No | Spark master URL. | `local[*]` |
| SPARK_CONF | No | Arbitrary Spark configuration settings, like:<br>`--conf spark.executor.memory=2g --conf spark.driver.cores=2` | Empty |
| JAR_FILE | No | Path to the application jar file. Relative to the build directory.<br> If not specified, the jar file will be automatically found under `target/` subdirectory of the build directory. | N/A |
| MAIN_CLASS | Yes | Main class of the application to run. | N/A |
| APP_ARGS | No | Application arguments. <br> `--input-file-path=s3://test-bucket/object/` ["s3" --> s3://test-bucket/object/, "hdfs" --> hdfs://test/folder/, "local" --> file:///test/folder/] <br> `--output-save-mode=false` [display on the console or `true` write to file] <br> `--output-file-path=s3://test-bucket/out/` ["s3" --> s3://test-bucket/object/, "hdfs" --> hdfs://test/folder/, "local" --> file:///test/folder/ must to give if --output-save-mode is enabled] | Empty |

## Note

1. Make sure to have IAM role configured with the proper s3 bucket read/write permissions if running under EC2's for the file read/write
2. You can launch the multiple spark nodes at scale using this image and can run it as standlone or cluster-mode (yarn or mesos)
 
