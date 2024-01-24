# SNOMED-Cluster-Creator

This script creates clusters of SNOMED codes based on the transitive closure table in the SNOMED database.

The clusters are created by specifying a list of parent codes and then finding all the child codes of those parents.

The child codes are then converted to their current code if they have been retired.

The clusters are output to a csv file and an xlsx file.

The csv file is then used to create a txt file for each cluster.

Instructions:

Obtain the SNOMED databases from the NHS Digital TRUD website.

Configure the paths to the databases and the output files.

Configure the clusters dictionary with the clusters to create.
