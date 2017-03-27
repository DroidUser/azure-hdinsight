_download_file()
{
    srcurl=$1;
    destfile=$2;
    overwrite=$3;

    if [ "$overwrite" = false ] && [ -e $destfile ]; then
        return;
    fi

    wget -O $destfile -q $srcurl;
}

_untar_file()
{
    zippedfile=$1;
    unzipdir=$2;

    if [ -e $zippedfile ]; then
        tar -xf $zippedfile -C $unzipdir;
    fi
}

_test_is_headnode()
{
    short_hostname=`hostname -s`
    if [[  $short_hostname == headnode* || $short_hostname == hn* ]]; then
        echo 1;
    else
        echo 0;
    fi
}

_test_is_datanode()
{
    short_hostname=`hostname -s`
    if [[ $short_hostname == workernode* || $short_hostname == wn* ]]; then
        echo 1;
    else
        echo 0;
    fi
}

_test_is_zookeepernode()
{
    short_hostname=`hostname -s`
    if [[ $short_hostname == zookeepernode* || $short_hostname == zk* ]]; then
        echo 1;
    else
        echo 0;
    fi
}

#find active namenode of the cluster
_get_namenode_hostname(){

    return_var=$1
    default=$2

    hadoop_cluster_name=`hdfs getconf -confKey dfs.nameservices`

    if [ $? -ne 0 -o -z "$hadoop_cluster_name" ]; then
        echo "Unable to fetch Hadoop Cluster Name"
        exit 1
    fi

    namenode_id_string=`hdfs getconf -confKey dfs.ha.namenodes.$hadoop_cluster_name`

    for namenode_id in `echo $namenode_id_string | tr "," " "`
    do
        status=`hdfs haadmin -getServiceState $namenode_id`
        if [ $status = "active" ]; then
            active_namenode=`hdfs getconf -confKey dfs.namenode.https-address.$hadoop_cluster_name.$namenode_id`
            IFS=':' read -ra $return_var<<< "$active_namenode"
            if [ "${!return_var}" == "" ]; then
                    eval $return_var="'$default'"
            fi

        fi
    done
}

_init(){
	
	_get_namenode_hostname namenode_hostname `hostname -f`
	hadoop_version={ eval hadoop version | head -1 } || "not found"
	hive_version={ eval hive --version | head -1 } || "not found"

	#Create file with hostnames
	host_metadata="metadata.txt"
	echo $namenode_hostname >> $host_metadata
	echo $hadoop_version >> $host_metadata
	echo $hive_version >> $host_metadata

	touch $host_metadata
}

_init
