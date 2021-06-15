#!/bin/sh

set -o errexit

test -z "$SCM_URL" && ( echo "SCM_URL is not set; exiting" ; exit 1 )
test -z "$SPARK_MASTER" && ( echo "SPARK_MASTER is not set; exiting" ; exit 1 )
test -z "$MAIN_CLASS" && ( echo "MAIN_CLASS is not set; exiting" ; exit 1 )


echo "Cloning the repository: $SCM_URL"
git clone "$SCM_URL"
ls -ltr
project_git="${SCM_URL##*/}"
project_dir="${project_git%.git}"
if test -n "$SCM_BRANCH"
then
	  cd "$project_dir"
	  git checkout "$SCM_BRANCH"
	  cd -
fi

echo "Building the jar"
if test -z "$PROJECT_SUBDIR"
then
	  cd "$project_dir"
else
	  cd "$project_dir/$PROJECT_SUBDIR"
fi
echo "Building at: $(pwd)"

if test -z "$BUILD_COMMAND"
then
	  build_command="sbt clean package"
else
	  build_command="$BUILD_COMMAND"
fi
echo "Building with command: $build_command"
eval $build_command


if test -z "$JAR_FILE"
then
	  jarfile="$(ls target/scala-*/*.jar)"
else
	  jarfile="$JAR_FILE"
fi

if ! test -f "$jarfile"
then
	  echo "Jar file not found or not a single file: $jarfile"
	  echo "You can specify jar file explicitly: -e JAR_FILE=/path/to/file"
	  exit 1
fi

echo "Submitting jar: $jarfile"
echo "Application main class: $MAIN_CLASS"
echo "Application arguments: $APP_ARGS"
echo "Spark master: $SPARK_MASTER"

spark-submit \
	--master "$SPARK_MASTER" \
       	$SPARK_CONF \
	--class "$MAIN_CLASS" \
	"$jarfile" \
	$APP_ARGS
