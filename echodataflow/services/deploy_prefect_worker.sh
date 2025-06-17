#!/bin/bash
source /media/volume/shimada_202506_volume/miniforge3/etc/profile.d/mamba.sh
mamba activate echodataflow_20250609
prefect worker start --pool 'local'