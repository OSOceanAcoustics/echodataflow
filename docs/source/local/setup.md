# Local Installation and Setup

## Step 0: Setup and Install Echoflow

Before we proceed with the instructions on using `echoflow`, let's ensure that we have properly set up and prepared our system with echoflow for usage. Feel free to skip this step if you have echoflow already set up.

### Step 0.1: Make a virtual environment

To keep your Echoflow environment isolated, it's recommended to create a virtual environment using Conda or Python's built-in venv module. Here's an example using Conda:

```bash
conda create --name echoflow-env
conda activate echoflow-env
```

Or, using Python's venv:

```bash
python -m venv echoflow-env
source echoflow-env/bin/activate  # On Windows, use `echoflow-env\Scripts\activate`
```
### Step 0.2: Clone the Repository
Now that you have a virtual environment set up, you can clone the Echoflow project repository to your local machine using the following command:

```bash
git clone <echoflow_repo>
```

### Step 0.3: Install the Package
Navigate to the project directory you've just cloned and install the Echoflow package. The -e flag is crucial as it enables editable mode, which is especially helpful during development and testing. Now, take a moment and let the echoflow do its thing while you enjoy your coffee.

```bash
cd <project_directory>
pip install -e .
```

---

### Step 0.4: Initialize Echoflow and Prefect

To kickstart your journey with Echoflow and Prefect, follow these simple initialization steps:

#### 0.4.1 Initializing Echoflow
Begin by initializing Echoflow with the following command:

```bash
echoflow init
```

This command sets up the groundwork for your Echoflow environment, preparing it for seamless usage.

#### 0.4.2 Initializing Prefect
For Prefect, initialization involves a few extra steps, including secure authentication. Enter the following command to initiate the Prefect authentication process:

- If you have a Prefect Cloud account, provide your Prefect API key to securely link your account. Type your API key when prompted and press Enter.

```bash
prefect cloud login
```

- If you don't have a Prefect Cloud account yet, you can use local prefect account. This is especially useful for those who are just starting out and want to explore Prefect without an account.

```bash
prefect profiles create echoflow-local
```


The initialization process will ensure that both Echoflow and Prefect are properly set up and ready for you to dive into your cloud-based workflows.
