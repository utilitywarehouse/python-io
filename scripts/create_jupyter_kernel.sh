#!/bin/bash

kernel_dir=$(jupyter --data-dir)/kernels/iolib

if [[ ! -d $kernel_dir ]]; then
    echo "Creating kernel at $kernel_dir/"
    mkdir -p $kernel_dir
fi

root=$(pwd | sed 's/\/scripts.*$//')
cat > ${kernel_dir}/kernel.json <<EOF
{
  "display_name": "iolib",
  "language": "python",
  "argv": [
    "$(which python)",
    "-m",
    "ipykernel_launcher",
    "-f",
    "{connection_file}"],
  "env": {
    "PYTHONPATH": "$root"
  }
}
EOF
echo "Jupyter kernel 'iolib' created"

if [[ $(pip freeze | grep -e ^ipykernel==) ]]; then
    echo "ipykernel already installed"
else
    echo "Installing ipykernel already installed"
    pip install ipykernel
fi

