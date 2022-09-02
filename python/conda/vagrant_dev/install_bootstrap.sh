#! /bin/bash

wget https://repo.anaconda.com/miniconda/Miniconda3-py38_4.12.0-Linux-x86_64.sh -q -O ~/miniconda.sh
chmod +x ~/miniconda.sh
~/miniconda.sh -b -p $HOME/miniconda
rm ~/miniconda.sh
echo "PATH=$PATH:$HOME/miniconda/bin" >> /home/vagrant/.bashrc
sudo /home/vagrant/miniconda/bin/conda update conda -y -q
sudo /home/vagrant/miniconda/bin/conda install conda-build -y -q

