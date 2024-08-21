# curl -s "https://raw.githubusercontent.com/icanbwell/SparkPipelineFramework/master/install/linux/install.sh" | bash
sudo yum update -y

sudo yum -y install zlib-devel bzip2 bzip2-devel readline-devel sqlite \
sqlite-devel openssl-devel xz xz-devel libffi-devel findutils

/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)"
echo 'eval $(/home/linuxbrew/.linuxbrew/bin/brew shellenv)' >> ~/.bash_profile
eval $(/home/linuxbrew/.linuxbrew/bin/brew shellenv)

export TMPDIR=/tmp
export SPARK_VER=3.0.1
export HADOOP_VER=3.2

sudo mkdir -p /usr/local/opt
sudo chmod 777 /usr/local/opt
