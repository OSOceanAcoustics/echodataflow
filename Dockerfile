# We're using the latest version of Prefect with Python 3.10
FROM prefecthq/prefect:2-python3.10

# Add our requirements.txt file to the image and install dependencies
COPY requirements.txt .

# Update setuptools and pip before installing echoflow
RUN pip install --upgrade pip setuptools

RUN pip install --no-cache-dir --trusted-host pypi.python.org echoflow
# RUN pip install -r requirements.txt --trusted-host pypi.python.org --no-cache-dir
RUN echoflow init


# Run our flow script when the container starts
CMD ["python", "-m", "echoflow.docker_trigger.py"]

EXPOSE 4200