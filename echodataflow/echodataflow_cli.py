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
import textwrap

from echodataflow.stages.echodataflow import load_credential_configuration

def fetch_ruleset():
    """
    Retrieves the path to the Echodataflow rules text file.

    This function constructs the path to the 'echodataflow_rules.txt' file located within
    a '.echodataflow' directory in the user's home directory. The function assumes that
    the '.echodataflow' directory is already present in the user's home directory. If the
    directory does not exist, this function will still return the constructed path,
    but accessing the file may result in an error if further action is not taken
    to either create the directory or place the 'echodataflow_rules.txt' file in the expected
    location.

    Returns:
        str: The absolute path to the 'echodataflow_rules.txt' file within the '.echodataflow'
             directory in the user's home directory.
    """
    # Get the user's home directory and expand ~
    home_directory = os.path.expanduser("~")

    # Create the directory if it doesn't exist
    config_directory = os.path.join(home_directory, ".echodataflow")
    rules_path = os.path.join(config_directory, "echodataflow_rules.txt")
    
    return rules_path

def clean_ruleset(rules: set):
    """
    Cleans the existing ruleset file and repopulates it with a given set of rules.

    This function first deletes the current 'echodataflow_rules.txt' file located within
    the '.echodataflow' directory in the user's home directory. It then creates a new
    'echodataflow_rules.txt' file and populates it with the rules provided in the input set.
    The function performs a sanity check on each rule to ensure it follows the expected 
    format "parent_flow:child_flow". If any rule does not comply, a ValueError is raised,
    and the operation is aborted. 

    Parameters:
        rules (set): A set of strings, each representing a rule in the format
                     "parent_flow:child_flow". This set of rules will replace any
                     existing rules in the 'echodataflow_rules.txt' file.

    Returns:
        set: The same set of rules provided as input, indicating the rules now
             present in the 'echodataflow_rules.txt' file.

    Raises:
        ValueError: If any rule in the input set does not contain a colon (':'),
                    indicating it does not follow the "parent_flow:child_flow" format.

    Side effects:
        - The existing 'echodataflow_rules.txt' file is deleted, and its contents are lost.
        - A new 'echodataflow_rules.txt' file is created with the contents set to the input rules.
        - If the input rules contain any format errors, the entire operation is aborted,
          and the 'echodataflow_rules.txt' file remains deleted.

    Notes:
        - This function relies on `fetch_ruleset` to determine the path to the rules file and
          `add_rules_from_set` to repopulate the new rules file. It is assumed that both
          functions handle any required validations and exceptions beyond format checking.
    """
    rules_path = fetch_ruleset()
    
    os.remove(rules_path)
    
    add_rules_from_set(rule_set=rules)
    return rules

def fetch_all_rules():
    """
    Reads and returns all the existing rules from the 'echodataflow_rules.txt' file.
    
    This function locates the '.echodataflow' directory within the user's home directory,
    reads the 'echodataflow_rules.txt' file, and returns a list of all rules defined within
    that file. Each rule is returned as a string in the list, including any newline characters.
    
    Returns:
        list of str: A list containing all the rules defined in 'echodataflow_rules.txt'.
    """
    rules_path = fetch_ruleset()
    with open(rules_path, 'r') as file:
        rules = file.readlines()
    return clean_ruleset(set(rules))
        
def add_new_rule(new_rule) -> None:
    """
    Appends a new rule to the 'echodataflow_rules.txt' file within the '.echodataflow' directory.
    
    This function takes a single rule as input and appends it to the end of the
    'echodataflow_rules.txt' file, located in the '.echodataflow' directory within the user's home directory.
    If the directory or the file doesn't exist, they are created. The new rule is added as a new line
    in the file.
    
    Parameters:
        new_rule (str): The rule to be added to the file. Should be in the format 'parent_flow:child_flow'.
        
    Returns:
        None
    """
    if len(new_rule.split(':')) != 2:
        print("Sanity check failed. Please make sure all rules follow the convention : One rule per line. Format -> parent_flow:child_flow")
        raise ValueError("Error adding rules. Sanity Check failed.")
    rules_path = fetch_ruleset()
    """Append a new rule to the existing rules file."""
    with open(rules_path, 'a') as file:
        file.write(new_rule+"\n")
    print("New rule added successfully.")
    
def add_rules_from_set(rule_set: set):
    """
    Writes a set of rules to the Echodataflow rules file, replacing any existing content.

    This function takes a set of rules and writes them to the 'echodataflow_rules.txt' file,
    located within the '.echodataflow' directory in the user's home directory. Each rule is 
    written on a new line. Before writing, the function performs a sanity check on each rule
    to ensure it follows the expected format ("parent_flow:child_flow"). If any rule does not
    comply with this format, the function raises a ValueError and aborts the operation.

    Parameters:
        rule_set (set): A set of strings, each representing a rule in the format
                        "parent_flow:child_flow".

    Raises:
        ValueError: If any rule in the `rule_set` does not contain a colon (':'),
                    indicating it does not follow the "parent_flow:child_flow" format.

    Returns:
        None
    
    """
    rules_path = fetch_ruleset()
    
    for rule in rule_set:
        if ':' not in rule:
            print("Sanity check failed. Please make sure all rules follow the convention : One rule per line. Format -> parent_flow:child_flow")
            raise ValueError("Error adding rules. Sanity Check failed.") 
        if len(rule.split(':')) != 2:
            print("Sanity check failed. Please make sure all rules follow the convention : One rule per line. Format -> parent_flow:child_flow")
            raise ValueError("Error adding rules. Sanity Check failed.")
    with open(rules_path, 'w') as ruleset:        
        for rule in rule_set:
            ruleset.write(rule)    

def add_rules_from_file(file_path) -> None:
    """
    Reads rules from a specified file and adds them to the Echodataflow configuration.

    This function opens a file from the given file path, reads each line as a rule,
    and adds it using the add_new_rule function. It performs a basic sanity check on each rule to ensure
    it follows the expected format "parent_flow:child_flow". If any rule does not comply with this format,
    the function raises a ValueError and stops adding further rules.

    Parameters:
        file_path (str): The path to the file containing rules to be added. Each rule must be on a new line.

    Raises:
        ValueError: If any rule in the file does not follow the "parent_flow:child_flow" format.
        Exception: If there is an error reading from the file.

    Returns:
        None
    """
    rules_path = fetch_ruleset()
    
    try:
        with open(rules_path, 'r') as file:
            new_rules = [line.strip() for line in file if line.strip()]
        
        add_rules_from_set(set(new_rules))
        
        print(f"Added {len(new_rules)} new rule(s) from {rules_path}.")
    except Exception as e:
        print(f"Error reading from file {rules_path}: {e}")

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
        "echodataflow_open_raw:echodataflow_compute_SV" means `echodataflow_open_raw` should be executed
        before `echodataflow_compute_SV`.

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
    "echodataflow_open_raw:echodataflow_compute_SV",
    "echodataflow_open_raw:echodataflow_combine_echodata",
    "echodataflow_open_raw:echodataflow_compute_TS",
    "echodataflow_combine_echodata:echodataflow_compute_SV",
    "echodataflow_compute_SV:echodataflow_compute_MVBS"
    ]
    for r in rules:
        rules_file.write(r+"\n")
    rules_file.close()

    print("Initilization complete")

def generate_stage_file(stage_name: str):
        file_content = f"""
    \"\"\"
    Echodataflow {stage_name.capitalize()} Task

    This module defines a Prefect Flow and associated tasks for the Echodataflow {stage_name.capitalize()} stage.

    Classes:
        None

    Functions:
        echodataflow_{stage_name}(config: Dataset, stage: Stage, data: Union[str, List[Output]])
        process_{stage_name}(config: Dataset, stage: Stage, out_data: Union[List[Dict], List[Output]], working_dir: str)

    Author: Soham Butala
    Email: sbutala@uw.edu
    Date: August 22, 2023
    \"\"\"
    import os
    from typing import Dict, List, Optional, Union

    import echopype as ep
    from prefect import flow, task

    from echodataflow.aspects.echodataflow_aspect import echodataflow
    from echodataflow.models.datastore import Dataset
    from echodataflow.models.output_model import Output
    from echodataflow.models.pipeline import Stage
    from echodataflow.utils import log_util
    from echodataflow.utils.file_utils import (get_output, get_working_dir,
                                        get_zarr_list, isFile,
                                        process_output_groups, get_out_zarr)


    @flow
    @echodataflow(processing_stage="{stage_name.replace("_", "-")}", type="FLOW")
    def echodataflow_{stage_name}(
            config: Dataset, stage: Stage, prev_stage: Optional[Stage]
    ):
        \"\"\"
        {stage_name.replace("_", " ")} from echodata.

        Args:
            config (Dataset): Configuration for the dataset being processed.
            stage (Stage): Configuration for the current processing stage.
            prev_stage (Stage): Configuration for the previous processing stage.

        Returns:
            List[Output]: List of The input dataset with the {stage_name.replace("_", " ")} data added.

        Example:
            # Define configuration and data
            dataset_config = ...
            pipeline_stage = ...
            echodata_outputs = ...

            # Execute the Echodataflow {stage_name} stage
            {stage_name}_output = echodataflow_{stage_name}(
                config=dataset_config,
                stage=pipeline_stage,
                data=echodata_outputs
            )
            print("Output :", {stage_name}_output)
        \"\"\"
        data: Union[str, List[Output]] = get_output()
        outputs: List[Output] = []
        futures = []
        working_dir = get_working_dir(config=config, stage=stage)

        if type(data) == list:
            if type(data[0].data) == list:
                for output_data in data:
                    transect_list = output_data.data
                    for ed in transect_list:
                        transect = str(ed.get("out_path")).split(".")[0] + ".{stage_name.replace("_", "").capitalize()}"
                        process_{stage_name}_wo = process_{stage_name}.with_options(
                            name=transect, task_run_name=transect, retries=3
                        )
                        future = process_{stage_name}_wo.submit(
                            config=config, stage=stage, out_data=ed, working_dir=working_dir
                        )
                        futures.append(future)
            else:
                for output_data in data:
                    future = process_{stage_name}.submit(
                        config=config, stage=stage, out_data=output_data, working_dir=working_dir
                    )
                    futures.append(future)

            ed_list = [f.result() for f in futures]

            outputs = process_output_groups(name=stage.name, config=config, ed_list=ed_list)

        return outputs


    @task
    @echodataflow()
    def process_{stage_name}(
        config: Dataset, stage: Stage, out_data: Union[Dict, Output], working_dir: str
    ):
        \"\"\"
        Process and {stage_name.replace("_", " ")} from Echodata object into the dataset.

        Args:
            config (Dataset): Configuration for the dataset being processed.
            stage (Stage): Configuration for the current processing stage.
            out_data (Union[Dict, Output]): Processed outputs (xr.Dataset) to {stage_name.replace("_", " ")}.
            working_dir (str): Working directory for processing.

        Returns:
            The input dataset with the {stage_name.replace("_", " ")} data added

        Example:
            # Define configuration, processed data, and working directory
            dataset_config = ...
            pipeline_stage = ...
            processed_outputs = ...
            working_directory = ...

            # Process and {stage_name.replace("_", " ")}
            {stage_name}_output = process_{stage_name}(
                config=dataset_config,
                stage=pipeline_stage,
                out_data=processed_outputs,
                working_dir=working_directory
            )
            print(" Output :", {stage_name}_output)
        \"\"\"

        if type(out_data) == dict:
            file_name = str(out_data.get("file_name"))
            transect = str(out_data.get("transect"))        
        else:
            file_name = str(out_data.data.get("file_name"))
            transect = str(out_data.data.get("transect"))
        """ + """
        try:
            log_util.log(msg={'msg':f' ---- Entering ----', 'mod_name':__file__, 'func_name':file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)
                
            out_zarr = get_out_zarr(group = stage.options.get('group', True), working_dir=working_dir, transect=transect, file_name=file_name, storage_options=config.output.storage_options_dict)
            
            
            log_util.log(msg={'msg':f'Processing file, output will be at {out_zarr}', 'mod_name':__file__, 'func_name':file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)
            
            if stage.options.get("use_offline") == False or isFile(out_zarr, config.output.storage_options_dict) == False:
                log_util.log(msg={'msg':f'File not found in the destination folder / use_offline flag is False', 'mod_name':__file__, 'func_name':file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)
            
                ed_list = get_zarr_list.fn(transect_data=out_data, storage_options=config.output.storage_options_dict)
                
                log_util.log(msg={'msg':f'Computing """ + f"""{stage_name}""" + """', 'mod_name':__file__, 'func_name':file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)
                
                # xr_d = ..call your function here...
                
                log_util.log(msg={'msg':f'Converting to Zarr', 'mod_name':__file__, 'func_name':file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)
            
                xr_d.to_zarr(store=out_zarr, mode="w", consolidated=True,
                                storage_options=config.output.storage_options_dict)
                                        
            else:
                log_util.log(msg={'msg':f'Skipped processing {file_name}. File found in the destination folder. To replace or reprocess set `use_offline` flag to False', 'mod_name':__file__, 'func_name':file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)
            
            log_util.log(msg={'msg':f' ---- Exiting ----', 'mod_name':__file__, 'func_name':file_name}, use_dask=stage.options['use_dask'], eflogging=config.logging)
            
            return {'out_path': out_zarr, 'transect': transect, 'file_name': file_name, 'error': False}
        except Exception as e:
            return {'transect': transect, 'file_name': file_name, 'error': True, 'error_desc': e}   
        """

        with open(f'./{stage_name}.py', 'w') as file:
            file.write(textwrap.dedent(file_content))


def main():
    """
    Main entry point of the Echodataflow CLI script.
    
    This function provides subcommands for generating and managing Echodataflow configurations:
    
    Subcommands:
    - `load-credentials`: Load credentials from a configuration file and create corresponding credential blocks.
      Options:
      --sync: If provided, syncs blocks updated using the Prefect UI.
    
    - `init`: Initializes an empty `.ini` file for Echodataflow configurations.
    
    - `gs`: Helps create boilerplate code for a specific stage.
      Arguments:
        - `stage_name`: Name of the stage for which to generate boilerplate code.
    
    - `rules`: View, add, or import flow rules from a file.
      Options:
        --add: Add a new rule interactively. Requires input in `parent_flow:child_flow` format.
        --add-from-file: Path to a file containing rules to be added. Each rule should be on a new line in `parent_flow:child_flow` format.
    
    Example usage:
    - To load credentials and sync:
      ```
      echodataflow load-credentials --sync
      ```
    
    - To initialize an empty `.ini` file:
      ```
      echodataflow init
      ```
    
    - To create boilerplate code for a specific stage:
      ```
      echodataflow gs <stage_name>
      ```
    
    - To add a new rule interactively:
      ```
      echodataflow rules --add
      ```
    
    - To import rules from a file:
      ```
      echodataflow rules --add-from-file path/to/rules.txt
      ```

    Note:
    - Use the appropriate subcommand to perform desired actions related to Echodataflow configurations.
    - When using `load-credentials`, ensure that the `credentials.ini` file is correctly formatted with appropriate sections and keys.
    - Supported section names are "AWS" and "AZCosmos".
    - Any unrecognized sections will be reported.
    """
    parser = argparse.ArgumentParser(description="Echodataflow")

    subparsers = parser.add_subparsers(title="Echodataflow Commands", dest="command")

    load_parser = subparsers.add_parser("load-credentials", help="Load credentials from .ini file")
    load_parser.add_argument('--sync', action='store_true', help="If provided, syncs blocks updated using Prefect Dashoard.")

    run_parser = subparsers.add_parser("init", help="Initializes an empty .ini file")
    
    gs_parser = subparsers.add_parser("gs", help="Helps create boilerplate code for any stage")
    gs_parser.add_argument('stage_name', help='Name of the stage for which to generate boilerplate code')
    
    rule_parser = subparsers.add_parser("rules", help="View or add flow rules.")
    rule_parser.add_argument('--add', action='store_true', help="Add a new rule. Format -> parent_flow:child_flow")
    rule_parser.add_argument('--add-from-file', action='store_true', help="Path to a file containing new rules to add. One rule per line. Format -> parent_flow:child_flow")
    
    
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
        elif args.command == "gs":
            stage_name = args.stage_name
            if stage_name:        
                print("Creating Stage : ", stage_name)
                
                generate_stage_file(str(stage_name))
                
                print(f"Boilerplate code for {stage_name} stage created successfully.")
                print(f"Modify the generated code to match your specific requirements and add the new stage in the configured rules.")
            else:
                print("Please pass a stage name. Example `echodataflow gs <stage_name>`")
        elif args.command == "rules":
            if args.add:
                print("Adding new rule to the ruleset.")
                new_rule = input("Please enter the parent and child allowed flow in `parent_flow:child_flow` format: ")
                add_new_rule(new_rule)
            elif args.add_from_file:
                print("Adding new rules to the ruleset.")
                file_path = input("Please enter the path to the file containing new rules. Please make sure all rules follow the convention : One rule per line. Format -> parent_flow:child_flow: ")                
                if not file_path:
                    print("No File Path provided")
                else:
                    add_rules_from_file(file_path)
            else:
                rules = fetch_all_rules()
                print("These are the current rules configured:")
                
                [print(r, end='') for r in rules]
        else:
            print("Unknown Command")

if __name__ == "__main__":
    main()