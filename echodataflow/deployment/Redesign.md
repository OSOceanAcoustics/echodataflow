1. Deployment Engine
Iterate through deployment.yaml to create services in Prefect.
Leverage the Echodataflow Master Service to deploy services, renaming them as specified in deployment.yaml, and deploy the master service to Prefect.
Manage schedules, tags, and descriptions associated with each service.
Handle pipeline parameters, such as adding base paths and storage options when specified globally.
Automatically create work pools and work queues if not already present, using a default pool for all services and individual queues for each service.

2. Infrastructure Engine
Manage worker deployment using systemd, Docker, or Kubernetes.
Utilize Terraform to provision infrastructure.
Support adding workers as systemd processes.
Enable flow deployments from GitHub or local files via Docker. When a flow is triggered, it pulls the necessary Docker image and runs the flow within a container.

3. Echodataflow Master Service
Each service manages a set of pipelines or flows.
Parse the source and destination of each pipeline and convert them into an intermediate, JSON-serializable format.
Extract Prefect flow configuration and execute flows sequentially.
After each flow completes, pass data quality (DQ) checks to the AOP (After Operation Processor) for execution.
Run DQ checks on the flow's output after its execution.
Parse and configure logging.
Use a Singleton pattern to handle missing logging configurations, with new functions such as echodataflow.get_logger() and EDFLogger.log().
Store cleanup information received from the AOP and execute cleanup operations after all flows finish.

4. Echodataflow Master Flow
Handle a sequence of tasks to be executed.
Perform parameter sanitization by validating each task’s signature.
Use options to configure flow behavior.
Retrieve inputs from the intermediate representation.
Group files when grouping is specified.
Execute tasks for each file or group, either concurrently or in parallel.
Return the output directly without converting it back to an intermediate representation but with cleanup information attached.
AOP runs DQ checks on the output and returns the final result, including cleanup details, to the intermediate representation.