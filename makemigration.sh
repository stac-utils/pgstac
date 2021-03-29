#!/bin/bash
function usage(){
      echo "Usage:"
      echo "    $0 -h                           Display this help message."
      echo "    $0 [ postgresql://<dburl> ]     Create migration from existing database"
      echo "    $0 [ gitbranch ]                Create migration from a git branch"
      exit 0
}
while getopts ":h" opt; do
  case ${opt} in
    h )
      usage
      ;;
    \? )
      echo "Invalid Option: -$OPTARG" 1>&2
      exit 1
      ;;
    : )
      echo "Invalid option: -$OPTARG requires an argument" 1>&2
      ;;
  esac
done
shift $((OPTIND -1))
FROMDB=$1
{
  [ -z $FROMDB ] && usage
  DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
  MIGRA_IMAGE=$(docker build -q docker/)
  echo $MIGRA_IMAGE
  MIGRA_CONTAINER=$(docker run -d -p 5432 --rm -v $DIR:/workspaces/ -e PGUSER=postgres -e POSTGRES_HOST_AUTH_METHOD=trust $MIGRA_IMAGE)
  echo $MIGRA_CONTAINER
  docker exec $MIGRA_CONTAINER /workspaces/docker/dockermigra.sh $FROMDB
} >&2
docker exec  $MIGRA_CONTAINER cat /tmp/migration.sql
{
  docker kill $MIGRA_CONTAINER
} >&2