"""
Echodataflow CLI

This module provides a command-line interface (CLI) for managing Echodataflow credentials and configurations.
You can use this CLI to generate, load, and manage credential blocks stored in the `credentials.ini` file.

Usage:
- To load credentials and create credential blocks:
  echodataflow load-credentials [--sync]
- To initialize an empty `credentials.ini` file:
  echodataflow init

Subcommands:
- `load-credentials`: Load credentials from the `.ini` file and create corresponding credential blocks.
  Options:
  --sync: If provided, syncs blocks updated using the Prefect UI.

- `init`: Initializes an empty `.ini` file for Echodataflow credentials configuration.

Example:
- Load credentials and sync blocks:
  echodataflow load-credentials --sync

- Initialize an empty `.ini` file:
  echodataflow init

Note:
- Use the appropriate subcommand to perform desired actions related to Echodataflow credentials.
- When using `load-credentials`, ensure that the `credentials.ini` file is correctly formatted with appropriate sections and keys.
- Supported section names are "AWS" and "AZCosmos".
"""


import argparse
import os

from echodataflow.stages.echodataflow import load_credential_configuration


def generate_ini_file():
    """
    Generate the credentials.ini file and configuration files for Echodataflow.

    This function creates a directory named `.echodataflow` in the user's home directory
    if it doesn't exist already. It then generates the `credentials.ini` file within
    this directory, which serves as the configuration file for Echodataflow credentials.

    Additionally, the function configures pre-defined rules by generating an `echodataflow_rules.txt`
    file containing rule definitions that dictate the workflow execution sequence in Echodataflow.

    Example:
        Calling this function will create a `.echodataflow` directory in the user's home
        directory and generate the `credentials.ini` and `echodataflow_rules.txt` files within it.
        The credentials.ini file holds Echodataflow configuration details, while the echodataflow_rules.txt
        file defines the workflow execution sequence.

    Rules Format:
        The `echodataflow_rules.txt` file contains lines with rule definitions. Each rule specifies
        the execution sequence of two workflow components, separated by a colon. For example:
        "echodataflow_open_raw:echodataflow_compute_Sv" means `echodataflow_open_raw` should be executed
        before `echodataflow_compute_Sv`.

    Note:
        If the `.echodataflow` directory or `credentials.ini` file already exists, this function will
        skip their creation.

    """

    # Get the user's home directory and expand ~
    home_directory = os.path.expanduser("~")

    # Create the directory if it doesn't exist
    config_directory = os.path.join(home_directory, ".echodataflow")
    os.makedirs(config_directory, exist_ok=True)

    # Write the .ini file
    ini_file_path = os.path.join(config_directory, "credentials.ini")
    
    if not os.path.exists(ini_file_path):
        with open(ini_file_path, "w") as config_file:
            config_file.write("# Credential configuration file for Echodataflow\n")
        print("Successfully created credentials.ini file under .echodataflow directory.")
    else:
        print("credentials.ini file already exists. Skipping creation.")

    print("Configuring pre-defined rules...")
    rules_path = os.path.join(config_directory, "echodataflow_rules.txt")
    rules_file = open(rules_path, "w")
    rules = [
    "echodataflow_open_raw:echodataflow_compute_Sv",
    "echodataflow_open_raw:echodataflow_combine_echodata",
    "echodataflow_open_raw:echodataflow_compute_TS",
    "echodataflow_combine_echodata:echodataflow_compute_Sv",
    "echodataflow_compute_Sv:echodataflow_compute_MVBS"
    ]
    for r in rules:
        rules_file.write(r+"\n")
    rules_file.close()

    print("Initilization complete")

def main():
    """
    Main entry point of the Echodataflow CLI script.

    This function provides subcommands for generating and managing Echodataflow credentials configuration:

    Subcommands:
    - `load-credentials`: Load credentials from a configuration file and create corresponding credential blocks.
      Options:
      --sync: If provided, syncs blocks updated using the Prefect UI.

    - `init`: Initializes an empty `.ini` file for Echodataflow credentials configuration.

    Example:
    To load credentials and sync:
    ```
    echodataflow load-credentials --sync
    ```

    To initialize an empty `.ini` file:
    ```
    echodataflow init
    ```

    Note:
    - Use the appropriate subcommand to perform desired actions related to Echodataflow credentials.
    - When using `load-credentials`, ensure that the `credentials.ini` file is correctly formatted with appropriate sections and keys.
    - Supported section names are "AWS" and "AZCosmos".
    - Any unrecognized sections will be reported.
    """
    parser = argparse.ArgumentParser(description="Echodataflow")

    subparsers = parser.add_subparsers(title="Echodataflow Commands", dest="command")

    load_parser = subparsers.add_parser("load-credentials", help="Load credentials from .ini file")
    load_parser.add_argument('--sync', action='store_true', help="If provided, syncs blocks updated using Prefect Dashoard.")

    run_parser = subparsers.add_parser("init", help="Initializes an empty .ini file")

    args = parser.parse_args()
    if args.command is None:
        print("No command provided. Use 'load-credentials' or 'init'.")
    else:
        if args.command == "load-credentials":
            if args.sync:
                print("Syncing with Prefect Dashboard")
                print("Creating credential blocks from ~/.echodataflow/credentials.ini")
                load_credential_configuration(sync=True)
            else:
                print("To sync with Prefect Dashboard use `--sync` option")
                print("Creating credential blocks from ~/.echodataflow/credentials.ini")
                load_credential_configuration(sync=False)
            print()
        elif args.command == "init":
            print("Initializing Echodataflow")
            generate_ini_file()
        else:
            print("Unknown Command")

if __name__ == "__main__":
    main()