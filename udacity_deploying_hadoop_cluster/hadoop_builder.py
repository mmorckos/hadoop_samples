#!/usr/bin/env python 

'''
Simple script to manually build a hadoop cluster of 2 or more nodes on Amazon EC2 instances. The cluser has a single namenode that also doubles as a datanode.

Based on instructions from Udacity course: Deploying a Hadoop Cluster
'''

import os
import subprocess
import time

user = r'ubuntu'
keypair = r'KEY_PAIR_FILE_PATH'
local_ssh_config = r'SSH_CONFIG_FILEPATH'

namenode_aws_dns = r'NAME_NODE_AWS_PUBLIC_DNS'
namenode_aws_hostname = r'NAME_NODE_AWS_PRIVATE_DNS'
pseudo_name_node_host = r'namenode'

datanodes_aws_dns_list = [r'DATA_NODE_AWS_PUBLIC_DNS_0', r'DATA_NODE_AWS_PUBLIC_DNS_1', '...']
datanodes_aws_hostname_list = [r'DATA_NODE_AWS_PRIVATE_DNS_0', r'DATA_NODE_AWS_PRIVATE_DNS_1', '...']
pseudo_datanode_host_prefix = r'datanode'

hadoop_repo = 'http://apache.mirror.iweb.ca/hadoop/common/hadoop-2.7.2/hadoop-2.7.2.tar.gz'
hadoop_package = 'hadoop-2.7.2.tar.gz'
hadoop_replication_factor = '1'

instance_env_variables = [r'\#\# Hadoop Environment Variables', 
r'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64', 
r'export PATH=\$PATH:\$JAVA_HOME/bin', 
r'export HADOOP_HOME=/opt/hadoop/hadoop-2.7.2', 
r'export PATH=\$PATH:\$HADOOP_HOME/bin', 
r'export HADOOP_CONF_DIR=/opt/hadoop/hadoop-2.7.2/etc/hadoop']

## All instances

common_xml_config = ['\'<?xml version="1.0" encoding="UTF-8"?>\'',
'\'<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>\'']

core_site_xml_config = ['\'<configuration>\'',
'\'<property>\'',
'\'<name>fs.defaultFS</name>\'',
'\'<value>hdfs://' + namenode_aws_dns + ':9000</value>\'',
'\'</property>\'',
'\'</configuration>\'']

yarn_site_xml_config = ['\'<configuration>\'',
'\'<property>\'',
'\'<name>yarn.nodemanager.aux-services</name>\'',
'\'<value>mapreduce_shuffle</value>\'',
'\'</property>\'',
'\'<property>\'',
'\'<name>yarn.resourcemanager.hostname</name>\'',
'\'<value>' + namenode_aws_dns + '</value>\'',
'\'</property>\'',
'\'</configuration>\'']

mapred_site_xml_config = ['\'<configuration>\'',
'\'<property>\'',
'\'<name>mapreduce.jobtracker.address</name>\'',
'\'<value>' + namenode_aws_dns + ':54311</value>\'',
'\'</property>\'',
'\'<property>\'',
'\'<name>mapreduce.framework.name</name>\'',
'\'<value>yarn</value>\'',
'\'</property>\'',
'\'</configuration>\'']

## Namenode specific

hdfs_site_xml_namenode = ['\'<configuration>\'',
'\'<property>\'',
'\'<name>dfs.replication</name>\'',
'\'<value>' + hadoop_replication_factor + '</value>\'',
'\'</property>\'',
'\'<property>\'',
'\'<name>dfs.namenode.name.dir</name>\'',
'\'<value>file:///opt/hadoop/hadoop-2.7.2/data/hdfs/namenode</value>\'',
'\'</property>\'',
'\'<property>\'',
'\'<name>dfs.datanode.data.dir</name>\'',
'\'<value>file:///opt/hadoop/hadoop-2.7.2/data/hdfs/datanode</value>\'',
'\'</property>\'',
'\'</configuration>\'']

## Datanode specific

hdfs_site_xml_datanode = ['\'<configuration>\'',
'\'<property>\'',
'\'<name>dfs.replication</name>\'',
'\'<value>' + hadoop_replication_factor + '</value>\'',
'\'</property>\'',
'\'<property>\'',
'\'<name>dfs.datanode.data.dir</name>\'',
'\'<value>file:///opt/hadoop/hadoop-2.7.2/data/hdfs/datanode</value>\'',
'\'</property>\'',
'\'</configuration>\'']

def setup_ssh_config():
	#Name node
	fields = []
	fields.append('\'' + r'\nHost ' + pseudo_name_node_host + '\'')
	fields.append('\'' + r'\tHostname ' + namenode_aws_dns + '\'')
	fields.append('\'' + r'\tUser ' + user + '\'')
	fields.append('\'' + r'\tIdentityFile ' + keypair + '\'')
	# Data nodes
	for i in range(0, len(datanodes_aws_dns_list)):
		fields.append('\'' + r'\nHost ' + pseudo_datanode_host_prefix + str(i) + '\'')
		fields.append('\'' + r'\tHostname ' + datanodes_aws_dns_list[i] + '\'')
		fields.append('\'' + r'\tUser ' + user + '\'')
		fields.append('\'' + r'\tIdentityFile ' + keypair + '\'')
	# Exceute
	for field in fields:
		os.system('echo ' + field + ' >> ' + local_ssh_config);

def copy_keypair(hostname):
	os.system('scp ' + keypair + ' ' + user + '@' + hostname + ":~/.ssh");

##########################################
##########################################

def link_instances():
	print "*** Linking Hadoop node(s)..."
	# Copy ssh config file to namenode
	os.system('scp ' + local_ssh_config + ' ' + pseudo_name_node_host + ':~/.ssh')
	prog = subprocess.call(['ssh', user + '@' + pseudo_name_node_host, 'ssh-keygen -f ~/.ssh/id_rsa -t rsa -P ""'], stderr=subprocess.PIPE)
	prog = subprocess.call(['ssh', user + '@' + pseudo_name_node_host, 'cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys'], stderr=subprocess.PIPE)
	# Copy authroized keys to each data node
	for i in range(0, len(datanodes_aws_dns_list)):
		prog = subprocess.call(['ssh', user + '@' + pseudo_name_node_host, 'ssh ' + pseudo_datanode_host_prefix + str(i) + ' \'cat >> ~/.ssh/authorized_keys\' < ~/.ssh/id_rsa.pub'], stderr=subprocess.PIPE)
	print "*** Done linking Hadoop node(s)."

##########################################
##########################################

def exec_init_cmds(hostname):
	subprocess.call(['ssh', user + '@' + hostname, 'sudo apt-get update'], stderr=subprocess.PIPE)
	prog = subprocess.call(['ssh', user + '@' + hostname, 'yes | sudo apt-get dist-upgrade'], stderr=subprocess.PIPE)

def install_java(hostname):
	print "*** Installing Java..."
	prog = subprocess.call(['ssh', user + '@' + hostname, 'sudo add-apt-repository ppa:openjdk-r/ppa && sudo apt-get update && yes | sudo apt-get install openjdk-8-jdk'], stderr=subprocess.PIPE)
	print "*** Done!"

def install_hadoop(hostname):
	print "*** Downloading Hadoop..."
	prog = subprocess.Popen(['ssh', user + '@' + hostname, 'wget ' + hadoop_repo + ' -P ~ && logout'], stderr=subprocess.PIPE)
	time.sleep(30)
	print "*** Installing Hadoop..."
	prog = subprocess.call(['ssh', user + '@' + hostname, 'sudo tar -zxvf ' + hadoop_package], stderr=subprocess.PIPE)
	print "*** Done!"

def set_env_variables(hostname):
	for field in instance_env_variables:
		prog = subprocess.call(['ssh', user + '@' + hostname, 'echo ' + field + ' >>  ~/.bashrc'], stderr=subprocess.PIPE)
	prog = subprocess.call(['ssh', user + '@' + hostname, 'source ~/.bashrc'], stderr=subprocess.PIPE)

##########################################
##########################################

def core_site_config(hostname):
	prog = subprocess.call(['ssh', user + '@' + hostname, 'sudo echo ' + common_xml_config[0] + ' | sudo tee ~/hadoop-2.7.2/etc/hadoop/core-site.xml'], stderr=subprocess.PIPE)
	prog = subprocess.call(['ssh', user + '@' + hostname, 'sudo echo ' + common_xml_config[1] + ' | sudo tee --append ~/hadoop-2.7.2/etc/hadoop/core-site.xml'], stderr=subprocess.PIPE)
	for field in core_site_xml_config:
		prog = subprocess.call(['ssh', user + '@' + hostname, 'sudo echo ' + field + ' | sudo tee --append ~/hadoop-2.7.2/etc/hadoop/core-site.xml'], stderr=subprocess.PIPE)

def yarn_site_config(hostname):
	prog = subprocess.call(['ssh', user + '@' + hostname, 'sudo echo ' + common_xml_config[0] + ' | sudo tee ~/hadoop-2.7.2/etc/hadoop/yarn-site.xml'], stderr=subprocess.PIPE)
	prog = subprocess.call(['ssh', user + '@' + hostname, 'sudo echo ' + common_xml_config[1] + ' | sudo tee --append ~/hadoop-2.7.2/etc/hadoop/yarn-site.xml'], stderr=subprocess.PIPE)
	for field in yarn_site_xml_config:
		prog = subprocess.call(['ssh', user + '@' + hostname, 'sudo echo ' + field + ' | sudo tee --append ~/hadoop-2.7.2/etc/hadoop/yarn-site.xml'], stderr=subprocess.PIPE)

def mapred_site_config(hostname):
	prog = subprocess.call(['ssh', user + '@' + hostname, 'sudo cp ~/hadoop-2.7.2/etc/hadoop/mapred-site.xml.template ~/hadoop-2.7.2/etc/hadoop/mapred-site.xml'], stderr=subprocess.PIPE)
	prog = subprocess.call(['ssh', user + '@' + hostname, 'sudo echo ' + common_xml_config[0] + ' | sudo tee ~/hadoop-2.7.2/etc/hadoop/mapred-site.xml'], stderr=subprocess.PIPE)
	prog = subprocess.call(['ssh', user + '@' + hostname, 'sudo echo ' + common_xml_config[1] + ' | sudo tee --append ~/hadoop-2.7.2/etc/hadoop/mapred-site.xml'], stderr=subprocess.PIPE)
	for field in mapred_site_xml_config:
		prog = subprocess.call(['ssh', user + '@' + hostname, 'sudo echo ' + field + ' | sudo tee --append ~/hadoop-2.7.2/etc/hadoop/mapred-site.xml'], stderr=subprocess.PIPE)

def hdfs_site_config_namenode(hostname):
	prog = subprocess.call(['ssh', user + '@' + hostname, 'sudo echo ' + common_xml_config[0] + ' | sudo tee ~/hadoop-2.7.2/etc/hadoop/hdfs-site.xml'], stderr=subprocess.PIPE)
	prog = subprocess.call(['ssh', user + '@' + hostname, 'sudo echo ' + common_xml_config[1] + ' | sudo tee --append ~/hadoop-2.7.2/etc/hadoop/hdfs-site.xml'], stderr=subprocess.PIPE)
	prog = subprocess.call(['ssh', user + '@' + hostname, 'sudo mkdir -p ~/hadoop-2.7.2/data/hdfs/namenode'], stderr=subprocess.PIPE)
	prog = subprocess.call(['ssh', user + '@' + hostname, 'sudo mkdir -p ~/hadoop-2.7.2/data/hdfs/datanode'], stderr=subprocess.PIPE)
	for field in hdfs_site_xml_namenode:
		prog = subprocess.call(['ssh', user + '@' + hostname, 'sudo echo ' + field + ' | sudo tee --append ~/hadoop-2.7.2/etc/hadoop/hdfs-site.xml'], stderr=subprocess.PIPE)

def hdfs_site_config_datanode(hostname):
	prog = subprocess.call(['ssh', user + '@' + hostname, 'sudo echo ' + common_xml_config[0] + ' | sudo tee ~/hadoop-2.7.2/etc/hadoop/hdfs-site.xml'], stderr=subprocess.PIPE)
	prog = subprocess.call(['ssh', user + '@' + hostname, 'sudo echo ' + common_xml_config[1] + ' | sudo tee --append ~/hadoop-2.7.2/etc/hadoop/core-site.xml'], stderr=subprocess.PIPE)
	prog = subprocess.call(['ssh', user + '@' + hostname, 'sudo mkdir -p ~/hadoop-2.7.2/data/hdfs/datanode'], stderr=subprocess.PIPE)
	for field in hdfs_site_xml_datanode:
		prog = subprocess.call(['ssh', user + '@' + hostname, 'sudo echo ' + field + ' | sudo tee --append ~/hadoop-2.7.2/etc/hadoop/hdfs-site.xml'], stderr=subprocess.PIPE)

def namenode_master(hostname):
	prog = subprocess.call(['ssh', user + '@' + hostname, 'sudo touch ~/hadoop-2.7.2/etc/hadoop/masters'], stderr=subprocess.PIPE)
	prog = subprocess.call(['ssh', user + '@' + hostname, 'sudo echo ' + namenode_aws_hostname + ' | sudo tee --append ~/hadoop-2.7.2/etc/hadoop/masters'], stderr=subprocess.PIPE)

def namenode_slaves(hostname):
	for host in datanodes_aws_hostname_list:
		prog = subprocess.call(['ssh', user + '@' + hostname, 'sudo echo ' + host + ' | sudo tee --append ~/hadoop-2.7.2/etc/hadoop/slaves'], stderr=subprocess.PIPE)

def hosts_config_namenode():
	prog = subprocess.call(['ssh', user + '@' + pseudo_name_node_host, 'sudo echo \'' + namenode_aws_dns + ' ' + namenode_aws_hostname + '\' | sudo tee --append /etc/hosts'], stderr=subprocess.PIPE)
	for i in range(0, len(datanodes_aws_dns_list)):
		prog = subprocess.call(['ssh', user + '@' + pseudo_name_node_host, 'sudo echo \'' + datanodes_aws_dns_list[i] + ' ' + datanodes_aws_hostname_list[i] + '\' | sudo tee --append  /etc/hosts'], stderr=subprocess.PIPE)

##########################################
##########################################

def copy_hadoop(hostname):
	print "*** Downloading Hadoop..."
	prog = subprocess.call(['ssh', user + '@' + hostname, 'sudo mkdir -p /opt/hadoop && sudo mv ~/hadoop-2.7.2/ /opt/hadoop/'], stderr=subprocess.PIPE)
	print "*** Done!"

def chown(hostname):
	prog = subprocess.call(['ssh', user + '@' + hostname, 'sudo source ~/.bashrc'], stderr=subprocess.PIPE)
	prog = subprocess.call(['ssh', user + '@' + hostname, 'sudo chown -R ubuntu $HADOOP_HOME'], stderr=subprocess.PIPE)


##########################################
##########################################

def setup_name_node():
	print "*** Setting up Hadoop name node(s)..."
	copy_keypair(pseudo_name_node_host)
	exec_init_cmds(pseudo_name_node_host)
	install_java(pseudo_name_node_host)
	install_hadoop(pseudo_name_node_host)
	core_site_config(pseudo_name_node_host)
	yarn_site_config(pseudo_name_node_host)
	mapred_site_config(pseudo_name_node_host)
	hdfs_site_config_namenode(pseudo_name_node_host)
	namenode_master(pseudo_name_node_host)
	namenode_slaves(pseudo_name_node_host)
	hosts_config_namenode()
	copy_hadoop(pseudo_name_node_host)
	set_env_variables(pseudo_name_node_host)
	chown(pseudo_name_node_host)
	print "*** Done up Hadoop name node(s)."

def setup_datanode():
	print "*** Setting up Hadoop data node(s)..."
	for i in range(0, len(datanodes_aws_hostname_list)):
		hostname = pseudo_datanode_host_prefix + str(i)
		copy_keypair(hostname)
		exec_init_cmds(hostname)
		install_java(hostname)
		install_hadoop(hostname)
		core_site_config(hostname)
		yarn_site_config(hostname)
		mapred_site_config(hostname)
		hdfs_site_config_datanode(hostname)
		copy_hadoop(hostname)
		set_env_variables(hostname)
		chown(hostname)
	print "*** Done up Hadoop data node(s)."


setup_ssh_config()
setup_name_node()
setup_datanode()
link_instances()
