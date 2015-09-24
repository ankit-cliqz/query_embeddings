sh ../../cliqz_university/local/bin/load_cluster_env.sh test

. ../../cliqz_university/local/bin/load_cluster_env.sh test


unset CLIQZ_DMZ_GATEWAY

fab cliqz_tasks.ec2.launch_cluster