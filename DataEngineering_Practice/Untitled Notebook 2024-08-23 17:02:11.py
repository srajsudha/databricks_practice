# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# JSON string
json_str = '''{
    "consistsOfLocationInput": {
        "floatingBI": null,
        "composedOfLocation": [{
            "identifiedByUniqueData": null,
            "locatedAtStandardAddress": {
                "definedAtAddress": {
                    "number": "2",
                    "zipCode": "30826",
                    "country": "DEU",
                    "city": "Garbsen",
                    "street": "Heinrich-Nordhoff-Ring",
                    "municipality": null,
                    "referenceType": "StructuredAddress",
                    "addressLine2": null,
                    "source": "user",
                    "state": null,
                    "addressStatus": "NOT_VALIDATED"
                },
                "definedAtGeoCodeInfo": null
            },
            "assessedByAssessment": null,
            "composedOfComplex": [{
                "identifiedByUniqueData": null,
                "assessedByAssessment": null,
                "label": null,
                "composedOfBuilding": [{
                    "identifiedByUniqueData": {
                        "uniquePropertyReferenceNumber": null,
                        "intakeId": "9fffe958-aa00-42ce-95c6-a1b2c18e3089",
                        "companyRegisterNumber": null,
                        "ucvId": null
                    },
                    "assessedByAssessment": [{
                        "doneViaEvaluation": [{
                            "consistsOfValuation": [{
                                "basedOnValue": [{
                                    "amount": {
                                        "currency": "EUR",
                                        "value": 8292.0498046875
                                    },
                                    "type": "MarketValue",
                                    "measurement": null
                                }]
                            }],
                            "consistsOfOccupancyAssessment": null
                        }]
                    }],
                    "label": "Heinrich-Nordhoff-Ring, 2, 30826, Garbsen, DEU",
                    "includesItem": [{
                        "identifiedByUniqueData": null,
                        "itemType": "EQUIPMENT",
                        "assessedByAssessment": [{
                            "doneViaEvaluation": [{
                                "consistsOfValuation": [{
                                    "basedOnValue": [{
                                        "amount": {
                                            "currency": "EUR",
                                            "value": 9895.580078125
                                        },
                                        "type": "TSI",
                                        "measurement": null
                                    }]
                                }],
                                "consistsOfOccupancyAssessment": null
                            }]
                        }],
                        "label": null
                    }, {
                        "identifiedByUniqueData": null,
                        "itemType": "STOCK_ON_DECLARATION",
                        "assessedByAssessment": [{
                            "doneViaEvaluation": [{
                                "consistsOfValuation": [{
                                    "basedOnValue": [{
                                        "amount": {
                                            "currency": "EUR",
                                            "value": 0.0
                                        },
                                        "type": "TSI",
                                        "measurement": null
                                    }]
                                }],
                                "consistsOfOccupancyAssessment": null
                            }]
                        }],
                        "label": null
                    }, {
                        "identifiedByUniqueData": null,
                        "itemType": "STOCK",
                        "assessedByAssessment": [{
                            "doneViaEvaluation": [{
                                "consistsOfValuation": [{
                                    "basedOnValue": [{
                                        "amount": {
                                            "currency": "EUR",
                                            "value": 0.0
                                        },
                                        "type": "TSI",
                                        "measurement": null
                                    }]
                                }],
                                "consistsOfOccupancyAssessment": null
                            }]
                        }],
                        "label": null
                    }]
                }]
            }],
            "additionalInfo": null,
            "label": null
        }],
        "id": "17588e8c-09d6-4b9f-a46d-c40a9f633d8d"
    },
    "cytoraId": "baebea62-b44f-4a71-8cca-d11a6c1cff44",
    "submissionId": "NCX_GO02000190.1.0",
    "receivedOn": [2024, 7, 30, 12, 15, 17],
    "aimsId": "66a8cb413144ea508ad81ffa",
    "cytoraUrl": "https://uwp.cytora.com/riskstream/search?search=baebea62-b44f-4a71-8cca-d11a6c1cff44&viewId=None&inboxId=None&recordId=None&workspaceId=allianz_de&renderSchema=None",
    "id": "10000122",
    "consistsOfSubmissionGeneralInfo": {
        "expiryDate": null,
        "lineOfBusiness": "Property",
        "belongsToOrganisation": "DE",
        "inceptionDate": [2026, 1, 1],
        "quoteRequiredByDate": null,
        "handeledByUnderwriter": {
            "name": "Tim Olfenius",
            "userId": null,
            "email": "tim.olfenius@allianz.de"
        },
        "mediatedByIntermediary": {
            "name": "Allianz Beratungs- und Vertriebs-AG",
            "id": "0/000/000"
        },
        "id": "fa60f0c4-3eb2-46ab-92ce-86243d1fb5a5",
        "salesOffering": null,
        "forCustomer": {
            "identifiedByUniqueData": null,
            "consistsOfOccupancyAssessment": [{
                "percentage": {
                    "value": 1.0
                },
                "hasOccupancy": [{
                    "code": "13140",
                    "notation": "LOCAL_CODE",
                    "notationCountry": "DE",
                    "label": "Grobe Metallbe- und -verarbeitung (soweit nicht separat aufgeführt)"
                }],
                "source": "manual",
                "primary": true
            }],
            "locatedAtAddress": {
                "number": "2",
                "zipCode": "30826",
                "country": "Deutschland",
                "city": "Garbsen",
                "street": "Heinrich-Nordhoff-Ring",
                "municipality": null,
                "referenceType": "StructuredAddress",
                "addressLine2": "",
                "source": "user",
                "state": null,
                "addressStatus": "NOT_VALIDATED"
            },
            "name": "Skodock Metallwarenfabrik GmbH",
            "financialRisk": "Unknown",
            "ucvId": "AZ0000000052216204",
            "turnover": null,
            "numberOfEmployees": null
        },
        "priority": "HIGH",
        "coverages": ["PD", "BI"]
    }
}'''

# Create a Spark DataFrame from the JSON string
df = spark.read.json(sc.parallelize([json_str]))

# Display the DataFrame
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import explode, col

# Flatten the DataFrame
df_flattened = df.select(
    col("aimsId"),
    col("cytoraId"),
    col("submissionId"),
    col("receivedOn"),
    col("cytoraUrl"),
    col("id").alias('first_id'),
    col("consistsOfSubmissionGeneralInfo.*"),
    col("consistsOfLocationInput.id").alias("locationInputId"),
    explode(col("consistsOfLocationInput.composedOfLocation")).alias("location")
).select(
    col("aimsId"),
    col("cytoraId"),
    col("submissionId"),
    col("receivedOn"),
    col("cytoraUrl"),
    col("first_id"),
    col("expiryDate"),
    col("lineOfBusiness"),
    col("belongsToOrganisation"),
    col("inceptionDate"),
    col("quoteRequiredByDate"),
    col("handledByUnderwriter.*"),
    col("mediatedByIntermediary.*"),
    col("salesOffering"),
    col("forCustomer.*"),
    col("priority"),
    col("coverages"),
    col("locationInputId"),
    col("location.*")
).select(
    col("aimsId"),
    col("cytoraId"),
    col("submissionId"),
    col("receivedOn"),
    col("cytoraUrl"),
    col("first_id"),
    col("expiryDate"),
    col("lineOfBusiness"),
    col("belongsToOrganisation"),
    col("inceptionDate"),
    col("quoteRequiredByDate"),
    col("handledByUnderwriter.name").alias("underwriterName"),
    col("handledByUnderwriter.userId").alias("underwriterUserId"),
    col("handledByUnderwriter.email").alias("underwriterEmail"),
    col("mediatedByIntermediary.name").alias("intermediaryName"),
    col("mediatedByIntermediary.id").alias("intermediaryId"),
    col("salesOffering"),
    col("consistsOfSubmissionGeneralInfo.identifiedByUniqueData"),
    col("consistsOfOccupancyAssessment"),
    col("locatedAtAddress.*"),
    col("forCustomer.name").alias("customerName"),
    col("financialRisk"),
    col("ucvId"),
    col("turnover"),
    col("numberOfEmployees"),
    col("priority"),
    col("coverages"),
    col("locationInputId"),
    col("additionalInfo"),
    col("assessedByAssessment"),
    col("composedOfComplex"),
    col("location.label").alias("locationLabel")
)

# Display the flattened DataFrame
display(df_flattened)
