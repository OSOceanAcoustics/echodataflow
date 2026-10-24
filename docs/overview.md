# How Echodataflow works

Echodataflow executes echosounder data processing pipelines by following the "recipes," or text-based specification of the workflow steps and parameters. It is developed to streamline reusing and experimenting data processing steps and parameter combinations following the same general blueprints. 




## Recipes

Each data processing pipeline is defined by a pair of recipes: the "deployment" and "parameters" recipes. The recipes are organized around the concept of "flow," whcih is the central container of workflow logic in Prefect. The flows are regular Python functions decorated by `@flow` that makes it possible for Prefect to execute and monitor the operations.

In Echodataflow:
- The **deployment recipe** contains flow deployment specifications, such as timing and computing resources.
- The **parameter recipe** contains input parameters for the specific flow functions.

Under the hood, the Echodataflow deployment engine parses the recipes to assemble and execute the workflow using mechanisms available from the the open-source [Prefect](https://www.prefect.io/) workflow orchestration package. 

The flows currently available in the Echodataflow package were developed to support the development team's research work to execute near-real-time echosounder data processing on ocean platforms and post-processing archived data. See the Echodataflow-recipes repository for reproducible examples of the near-real-time and post-processing worflows, and [](recipe-spec) for details on preparing the deployment parameter recipes.




## Deployment setup
To deploy a workflow, we need a server and the work pools:
- The [server](https://docs.prefect.io/v3/concepts/server) orchestrates the execution of flows and provides visibility into the status of each flow run
- The [work pools](https://docs.prefect.io/v3/concepts/work-pools) connect the orchestration layer to the computing infrastructure where flows run and define how that infrastructure is configured.


### Quick local testin
For quick local testing, start a Prefect server and a worker in separate terminals:

1. In the first terminal, start the server:

   ```shell
   prefect server start
   ```

2. In the second terminal, configure Prefect to connect to the local server, then start a worker for the `local` work pool:

   ```shell
   prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
   prefect worker start --pool local
   ```

Open the Prefect dashboard at <http://127.0.0.1:4200> to monitor flow runs. Keep the server and worker running while testing deployments.


### Operational deployments
For operational deployments, these can be run via system services so that they start automatically on boot and restart if they fail. See [](deployment-system-services) for details on running them on different operating systems.

