# We're using the latest version of Prefect with Python 3.10
FROM prefecthq/prefect:2-python3.10

# Add our requirements.txt file to the image and install dependencies
COPY ./echodataflow/docker_trigger.py /opt/prefect/flows/

# Update setuptools and pip before installing echodataflow
RUN pip install --upgrade pip setuptools

RUN pip install --no-cache-dir --trusted-host pypi.python.org echodataflow
RUN echodataflow init
RUN prefect profiles create echodataflow-local

# Run our flow script when the container starts
CMD ["python", "flows/docker_trigger.py"]

EXPOSE 4200