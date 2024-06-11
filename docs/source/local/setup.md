# Local Installation and Setup

## Step 0: Setup and Install Echodataflow

Before we proceed with the instructions on using `echodataflow`, let's ensure that we have properly set up and prepared our system with echodataflow for usage. Feel free to skip this step if you have echodataflow already set up.

### Step 0.1: Make a virtual environment

To keep your Echodataflow environment isolated, it's recommended to create a virtual environment using Conda or Python's built-in venv module. Here's an example using Conda:

```bash
conda create --name echodataflow-env
conda activate echodataflow-env
```

Or, using Python's venv:

```bash
python -m venv echodataflow-env
source echodataflow-env/bin/activate  # On Windows, use `echodataflow-env\Scripts\activate`
```

### Step 0.2: Install the Package
Once you have a virtual environment set up, just like any other python package you need to install using pip. Now, take a moment and let the echodataflow do its thing while you enjoy your coffee.

```bash
pip install echodataflow
```

---

### Step 0.3: Initialize Echodataflow and Prefect

To kickstart your journey with Echodataflow and Prefect, follow these simple initialization steps:

#### 0.3.1 Initializing Echodataflow
Begin by initializing Echodataflow with the following command:

```bash
echodataflow init
```

This command sets up the groundwork for your Echodataflow environment, preparing it for seamless usage.

#### 0.3.2 Initializing Prefect
For Prefect, initialization involves a few extra steps, including secure authentication. Enter the following command to initiate the Prefect authentication process:

- If you have a Prefect Cloud account, provide your Prefect API key to securely link your account. Type your API key when prompted and press Enter.

```bash
prefect cloud login
```

- If you don't have a Prefect Cloud account yet, you can use local prefect account. This is especially useful for those who are just starting out and want to explore Prefect without an account.

```bash
prefect profiles create echodataflow-local
```


The initialization process will ensure that both Echodataflow and Prefect are properly set up and ready for you to dive into your cloud-based workflows.
