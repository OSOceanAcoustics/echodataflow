#!/bin/bash

# Step 1: Create a Python Virtual Environment
python3 -m venv $HOME/env/echoflow-prod
source $HOME/env/echoflow-prod/bin/activate

# Step 2: Clone the Echoflow Repository
cd $HOME/
git clone https://github.com/OSOceanAcoustics/echoflow.git
cd $HOME/echoflow

# Step 3: Checkout the Dev Branch and Update (Optional) - Skip if using Prod/main branch
git checkout dev
git pull origin dev

# Step 4: Install the Echoflow Project in Editable Mode
pip install -e .

# Step 5: Log in to Prefect Cloud and Set Your API Key - Change to step 5b if using prefect locally
echo "Enter Prefect API key: "
read prefectKey
prefect cloud login -k $prefectKey

# Step 5b: Setup prefect locally
# prefect profile create echoflow-local

# Step 6: Set Up the Prefect Worker as a Systemd Service
echo "Enter Work Pool Name: "
read workPool
cd /etc/systemd/system

# Create and edit the prefect-worker.service file
sudo cat <<EOL > prefect-worker.service
[Unit]
Description=Prefect-Worker

[Service]
User=$(whoami)
WorkingDirectory=$HOME/echoflow
ExecStart=$(which prefect) agent start --pool $workPool
Restart=always

[Install]
WantedBy=multi-user.target
EOL

# Step 7: Restart to to make systemd aware of the new service
sudo systemctl daemon-reload

# Optionally, enable the service to start at boot
sudo systemctl enable prefect-worker.service

# Step 8: Start the Prefect Worker Service
sudo systemctl start prefect-worker.service

echo "Setup completed. The Echoflow worker is now running. Send tasks to $workPool using Prefect UI or CLI."