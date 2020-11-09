# criteria-based-selection-fhir

This repository includes a fhir preprocessor, which allows one to define inclusion and exclusion criteria
as well as a main resource to be searched for and 
extract data for this base resource and basic patient data of patients referenced by the base resource.


## Getting Started

To get started we have provided a simple start script, which starts the fhir-preprocessor in development mode as well as a PSQL database filled with example fhir resources for testing purposes.

To start this example execute the `start.sh` in this repository.
Once started up you can exexute the exampleQuery.py (`python3 exampleQuery.py`) to run a query against the test data initialised under `./deploy/test-db/init-scripts`.

This will filter the dataset of the test database and return a csv output containing the the selected dataset based on the filtered data.
