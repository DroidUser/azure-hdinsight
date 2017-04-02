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
        tar -xzf $zippedfile -C $unzipdir;
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
    desired_status=$3

    hadoop_cluster_name=`hdfs getconf -confKey dfs.nameservices`

    if [ $? -ne 0 -o -z "$hadoop_cluster_name" ]; then
        echo "Unable to fetch Hadoop Cluster Name"
        exit 1
    fi

    namenode_id_string=`hdfs getconf -confKey dfs.ha.namenodes.$hadoop_cluster_name`

    for namenode_id in `echo $namenode_id_string | tr "," " "`
    do
        status=`hdfs haadmin -getServiceState $namenode_id`
        if [ $status = $desired_status ]; then
            active_namenode=`hdfs getconf -confKey dfs.namenode.https-address.$hadoop_cluster_name.$namenode_id`
            IFS=':' read -ra $return_var<<< "$active_namenode"
            if [ "${!return_var}" == "" ]; then
                    eval $return_var="'$default'"
            fi

        fi
    done
}
export -f _get_namenode_hostname

_init(){

	#Determine Hortonworks Data Platform version
	HDP_VERSION=`ls /usr/hdp/ -I current`

	#get active namenode of cluster
	_get_namenode_hostname active_namenode_hostname `hostname -f` "active"
	_get_namenode_hostname secondary_namenode_hostname `hostname -f` "standby"

	#download the spark config tar file
	_download_file https://raw.githubusercontent.com/DroidUser/azure-hdinsight/master/sparkconf.tar.gz /sparkconf.tar.gz

	# Untar the Spark config tar.
	mkdir /spark-config
	_untar_file /sparkconf.tar.gz /spark-config/

	#replace default config of spark in cluster
	cp -r /spark-config/0 /etc/spark2/$HDP_VERSION/
	cp -r /spark-config/LIVY /var/lib/ambari-server/resources/stacks/HDP/2.5/services/
	cp -r /spark-config/JUPYTER /var/lib/ambari-server/resources/stacks/HDP/2.5/services/
	
	
	#replace environment file
	cp /spark-config/environment /etc/
	source /etc/environment
	
	#create config directories
	mkdir /var/log/spark2
	mkdir -p /var/run/spark2

	#change permission
	chmod -R 775 /var/log/spark2
	chown -R spark: /var/log/spark2
	chmod -R 775 /var/run/spark2
	chown -R spark: /var/run/spark2

	#update the master hostname in configuration files
	sed -i 's|{{namenode-hostnames}}|thrift:\/\/'"${active_namenode_hostname}"':9083,thrift:\/\/'"${secondary_namenode_hostname}"':9083|g' /etc/spark2/$HDP_VERSION/0/hive-site.xml
	sed -i 's|{{history-server-hostname}}|'"${active_namenode_hostname}"':18080|g' /etc/spark2/$HDP_VERSION/0/spark-defaults.conf
	
	long_hostname=`hostname -f`
	
	#start the demons based on host
	if [ $long_hostname == $active_namenode_hostname ]; then
	 	cd /usr/hdp/current/spark2-client
		eval sudo -u spark ./sbin/start-history-server.sh
		eval sudo -u hive ./sbin/start-thriftserver.sh
	elif [ $long_hostname == $secondary_namenode_hostname ]; then
		cd /usr/hdp/current/spark2-client
		eval sudo -u hive ./sbin/start-thriftserver.sh
	else
		cd /usr/hdp/current/spark2-client
		eval sudo -u spark ./sbin/start-slaves.sh
	fi	 

	#Create file with hostnames
	host_metadata="metadata.txt"
	echo "HDI version : "$HDP_VERSION >> $host_metadata
	echo "active namenode : "$active_namenode_hostname >> $host_metadata
	echo "standby namenode : "$secondary_namenode_hostname >> $host_metadata

	touch $host_metadata
}

_init
