#!/bin/bash

# Define the nodes to run the command on
nodes=(dali001 dali003 dali005 dali006 dali007 dali008 dali009 dali010 dali011 dali012 dali013 dali014 dali015 dali016 dali017 dali018 dali020 dali021 dali022 dali023 dali024 dali026 dali027)

# Define the function to run the command on each node
function run_command_on_node() {
    node=$1
    output=$(djup --node $node --force_new --ram 2000)  
    if [[ $output == *"Happy strax analysis,"* ]]; then
        echo "$node succeeded"
    else
        echo "$node failed"
    fi
}

# Run the command on each node in parallel
for node in "${nodes[@]}"; do
    run_command_on_node "$node" &
    sleep 5
done

# Wait for all commands to finish
wait

# Report the status of each node
for node in "${nodes[@]}"; do
    echo "$(run_command_on_node "$node")"
done

# Clean up all the jobs we just submitted
scancel --name=straxlab

exit 0
