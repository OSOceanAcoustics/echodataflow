# Echoflow Configuration and Credential Blocks

Echoflow leverages the concept of "blocks" from Prefect, which serve as containers for storing various types of data, including credentials and sensitive information. Currently, Echoflow supports two types of blocks: Azure Cosmos DB Credentials Block and AWS Credentials Block. These blocks allow you to securely store sensitive data while benefiting from Prefect's robust integration capabilities.

For a deeper understanding of blocks, you can refer to the [Prefect documentation](https://docs.prefect.io/2.11.5/concepts/blocks/).

## Types of Blocks in Echoflow

In the context of Echoflow, there are two main categories of blocks:

### 1. Echoflow Configuration Blocks

These blocks serve as repositories for references to credential blocks, as well as repositories for the various Prefect profiles that have been established using Echoflow's functions.

### 2. Credential Blocks

Credential blocks store sensitive information, such as authentication keys and tokens, securely. Echoflow integrates with Prefect's capabilities to ensure that sensitive data is protected.

## Creating Credential Blocks

Credential blocks can be conveniently created using an `.ini` file. By leveraging Prefect's integration, Echoflow ensures that the credentials stored in these blocks are handled securely. To create a credential block, you can follow these steps:

1. Open the `credentials.ini` file, which is located under the `.echoflow` directory in your home directory.
```bash
# Terminal command
cd ~/.echoflow
```
2. Place the necessary credential information within the `credentials.ini` file.
```bash
# Terminal command
nano credentials.ini # Or use any of your favourite editors
```
3. Store the updated `.ini` file in the `.echoflow` directory, which resides in your home directory.
4. Utilize [echoflow load-credentials](../../echoflow/stages/subflows/echoflow.py#load_credential_configuration) command to generate a new credential block, leveraging the content from the `.ini` file. 
```bash 
echoflow load-credentials
```
5. Add the name of the block in pipeline or datastore yaml configuration files under `storage_options` section with the appropriate storage type (refer [StorageType](../../echoflow/config/models/datastore.py#StorageType)).

```yaml
# Example
storage_options:
  block_name: echoflow-aws-credentials  # Name of the block containing credentials
  type: AWS  # Specify the storage type using StorageType enum
```

By providing the block name and storage type, ensure that the correct block is used for storage operations, and maintain clarity regarding the chosen storage type.

Once a credential block is created, it can be managed through the Prefect Dashboard. Additionally, if needed, you can use the `echoflow load-credentials` command with the `--sync` argument to ensure your blocks stay up-to-date with any changes made in the Prefect UI. This ensures that your configurations remain accurate and aligned across the application. **It is highly recommended to create new blocks whenever possible, as modifying existing blocks can lead to data loss or conflicts.** 

## Considerations When Using `echoflow load-credentials`

When utilizing the `echoflow load-credentials` command, be aware of the following considerations:

- **Overwriting Values**: When using `echoflow load-credentials`, all the values from the `.ini` file will be written to the credential block, potentially overwriting existing values. Exercise caution when using this command to prevent unintentional data loss.
- **Creating New Blocks**: To maintain data integrity and security, it's advised to create new blocks rather than modifying existing ones. If editing an existing block becomes necessary, it should be done through the Prefect Dashboard.
- **Sync Argument**: The `--sync` argument is available in the `echoflow load-credentials` command. When set, this option syncs the credential block updates with the Prefect UI. This feature facilitates the seamless management of blocks through the dashboard, enhancing collaboration and control over credentials.

By adhering to these guidelines, you can ensure the secure management of sensitive information while effectively configuring and utilizing Echoflow within your projects.


# Configuration File Explanation: credentials.ini

This Markdown file explains the structure and contents of the `credentials.ini` configuration file.

## AWS Section

The `[AWS]` section contains configuration settings related to AWS credentials.

- `aws_access_key_id`: Your AWS access key.
- `aws_secret_access_key`: Your AWS secret access key.
- `aws_session_token`: AWS session token (optional).
- `region_name`: AWS region name.
- `name`: Name of the AWS credentials configuration.
- `active`: Indicates if the AWS credentials are active (True/False).
- `options`: Additional options for AWS configuration.

## AzureCosmos Section

The `[AZCosmos]` section contains configuration settings related to Azure Cosmos DB credentials.

- `name`: Name of the Azure Cosmos DB credentials configuration.
- `connection_string`: Azure Cosmos DB connection string.
- `active`: Indicates if the Azure Cosmos DB credentials are active (True/False).
- `options`: Additional options for Azure Cosmos DB configuration.

Example:

```ini
[AWS]
aws_key = my-access-key
aws_secret = my-secret-key
token = my-session-token
region = us-west-1
name = my-aws-credentials
active = True
option_key = option_value

[AZCosmos]
name = my-az-cosmos-credentials
connection_string = my-connection-string
active = True
option_key = option_value
```

