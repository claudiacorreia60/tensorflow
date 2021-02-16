#!/bin/bash

echo "python3 -m venv tf-env"
python3 -m venv tf-env

echo "source tf-env/bin/activate"
source tf-env/bin/activate

echo "pip3 install six numpy wheel setuptools moc"
pip3 install six numpy wheel setuptools moc
pip3 install six wheel setuptools moc
pip install numpy==1.18.5

echo "pip3 install  future>=0.17.1"
pip3 install 'future>=0.17.1'

echo "pip3 install keras_applications --no-deps"
pip3 install keras_applications --no-deps

echo "pip3 install keras_preprocessing --no-deps"
pip3 install keras_preprocessing --no-deps

echo "./configure"
./configure

echo "export LD_LIBRARY_PATH=$(pwd)/third_party/tbb/lib:$LD_LIBRARY_PATH"
export LD_LIBRARY_PATH=$(pwd)/third_party/tbb/lib:$LD_LIBRARY_PATH

echo 'bazel build --verbose_failures --config=opt --config=cuda //tensorflow/tools/pip_package:build_pip_package --action_env="LD_LIBRARY_PATH=${LD_LIBRARY_PATH}"'
bazel build --verbose_failures --config=opt --config=cuda //tensorflow/tools/pip_package:build_pip_package --action_env="LD_LIBRARY_PATH=${LD_LIBRARY_PATH}"

echo "./bazel-bin/tensorflow/tools/pip_package/build_pip_package $(pwd)/../tensorflow_pkg"
./bazel-bin/tensorflow/tools/pip_package/build_pip_package $(pwd)/../tensorflow_pkg
