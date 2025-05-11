# pyiceberg-practice

This script demonstrates how to: 

1. Create a partitioned table on iceberg
2. Ceate a branch off that table called audit_branch
3. Pull data from the polygon api and quickly write data to that branch if request is successful
4. Write the data to the audit_branch

This example is for the use case of WAP (write, audit, publish). This way we can test the loaded data directly from the table but a different branch (kinda like git). If the data in the branch passes all data quality and unit checks we can further fast forward the main branch to match the state of the audit branch. 

