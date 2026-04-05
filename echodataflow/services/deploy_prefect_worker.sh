#!/bin/bash
# Use mamba run for non-interactive shells
mamba run -n echodataflow_20250609 prefect worker start --pool 'local'