import requests

query_spec = {
    "loinc": ["718-7"],
    "include": [
          {
                "resource": "Observation",
                "system": "http://loinc.org",
                "search_type": "exact",
                "dateField": "effectiveDateTime",
                "codes": ["718-7"],
                "value_restriction":{
                    "operator": ">",
                    "compare_to": 8
                },
                "time_interval": 0
            },
            {
                "resource": "Observation",
                "system": "http://loinc.org",
                "search_type": "exact",
                "dateField": "effectiveDateTime",
                "codes": ["718-7"],
                "value_restriction":{
                    "operator": ">",
                    "compare_to": 10
                },
                "time_interval": 0
            }

    ],
    "exclude": [
            {
                "resource": "Condition",
                "system": "http://fhir.de/CodeSystem/dimdi/icd-10-gm",
                "search_type": "begins",
                "codes": ["C"],
                "dateField": "recordedDate",
                "time_interval": 0
            },
            {
                "resource": "Procedure",
                "system": "http://fhir.de/CodeSystem/dimdi/ops",
                "search_type": "begins",
                "codes": ["8-80"],
                "dateField": "recordedDate",
                "time_interval": 1
            },
            {
                "resource": "Observation",
                "system": "http://loinc.org",
                "search_type": "exact",
                "dateField": "effectiveDateTime",
                "codes": ["718-7"],
                "value_restriction":{
                    "operator": "<",
                    "compare_to": 8
                },
                "time_interval": 0
            }
        ]
    ,
    "feature_set": [
        {
            "resource": "Patient",
            "name": "pid",
            "resource_val_path": "identifier.0.value"
        },
        {
            "resource": "Patient",
            "name": "sex",
            "resource_val_path": "gender"
        },
        {
            "resource": "Observation",
            "name": "age",
            "computed":{
                "field_1":{
                    "resource": "Observation",
                    "resource_val_path": "effectiveDateTime"
                },
                "field_2":{
                    "resource": "Patient",
                    "resource_val_path": "birthDate"
                },
                "operation": "diffYearsDate"
            }
        },
        {
            "resource": "Observation",
            "name": "val",
            "resource_val_path": "valueQuantity.value"
        },
        {
            "resource": "Observation",
            "name": "lrl",
            "resource_val_path": "referenceRange.0.low.value"
        },
        {
            "resource": "Observation",
            "name": "url",
            "resource_val_path": "referenceRange.0.high.value"
        },
        {
            "resource": "Observation",
            "name": "date",
            "resource_val_path": "effectiveDateTime"
        },
        {
            "resource": "Observation",
            "name": "method",
            "resource_val_path": "method.text"
        }

    ]
}
response = requests.post("http://localhost:5000/filterTasks?execution_type=sync", json=query_spec)
task_id = response.json()["task_id"]
respTable = requests.get("http://localhost:5000/aggregationTasks/" + task_id).text

print(respTable)
