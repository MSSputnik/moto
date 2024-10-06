import boto3
import pytest
from uuid import uuid4
from moto import mock_aws
from moto.core import DEFAULT_ACCOUNT_ID as ACCOUNT_ID

# See our Development Tips on writing tests for hints on how to write good tests:
# http://docs.getmoto.org/en/latest/docs/contributing/development_tips/tests.html


def idfn(val):
    return val["Type"]


data_sources = [
    {
        "DataSourceId": "3ed26f04-d2a7-4d8d-8bd5-b3e81aa34d7f",
        "Name": "adobe analytics data source",
        "Type": "ADOBE_ANALYTICS",
        "DataSourceParameters": {},
        "Credentials": {},
    },
    {
        "DataSourceId": "8d92c8f7-3b31-421b-be40-71967029084a",
        "Name": "amazon elasticsearch data source",
        "Type": "AMAZON_ELASTICSEARCH",
        "DataSourceParameters": {"AmazonElasticsearchParameters": {"Domain": "string"}},
        "Credentials": {
            "CredentialPair": {
                "Username": "string",
                "Password": "string",
                "AlternateDataSourceParameters": [
                    {
                        "AmazonElasticsearchParameters": {"Domain": "string"},
                    }
                ],
            },
            "CopySourceArn": "string",
            "SecretArn": "string",
        },
    },
    {
        "DataSourceId": "088312d7-1c48-4dc8-bda9-7355ee7df82d",
        "Name": "athena data source",
        "Type": "ATHENA",
        "DataSourceParameters": {
            "AthenaParameters": {"WorkGroup": "primary"},
        },
        "Credentials": {
            "CredentialPair": {
                "Username": "username",
                "Password": "password",
                "AlternateDataSourceParameters": [
                    {
                        "AthenaParameters": {
                            "WorkGroup": "primary",
                        },
                    }
                ],
            },
            "CopySourceArn": "string",
            "SecretArn": "string",
        },
    },
    {
        "DataSourceId": "27686bde-c12e-4c5a-8e9a-10cd91082836",
        "Name": "aurora data source",
        "Type": "AURORA",
        "DataSourceParameters": {
            "AuroraParameters": {
                "Host": "string",
                "Port": 123,
                "Database": "string",
            },
        },
        "Credentials": {
            "CredentialPair": {
                "Username": "string",
                "Password": "string",
                "AlternateDataSourceParameters": [
                    {
                        "AuroraParameters": {
                            "Host": "string",
                            "Port": 123,
                            "Database": "string",
                        },
                    }
                ],
            },
            "CopySourceArn": "string",
            "SecretArn": "string",
        },
    },
    {
        "DataSourceId": "70df5243-1ff6-450c-85d0-c6d698380202",
        "Name": "aurora postgresql data source",
        "Type": "AURORA_POSTGRESQL",
        "DataSourceParameters": {
            "AuroraPostgreSqlParameters": {
                "Host": "string",
                "Port": 123,
                "Database": "string",
            },
        },
        "Credentials": {
            "CredentialPair": {
                "Username": "string",
                "Password": "string",
            },
            "CopySourceArn": "string",
            "SecretArn": "string",
        },
    },
    {
        "DataSourceId": "488da5f8-9e72-4c68-9985-ead2b44aba3e",
        "Name": "aws iot analytics data source",
        "Type": "AWS_IOT_ANALYTICS",
        "DataSourceParameters": {
            "AwsIotAnalyticsParameters": {"DataSetName": "string"},
        },
        "Credentials": {
            "CredentialPair": {
                "Username": "string",
                "Password": "string",
            },
            "CopySourceArn": "string",
            "SecretArn": "string",
        },
    },
    {
        "DataSourceId": "6bf76908-7edc-488c-be71-0f65f1f2d623",
        "Name": "github data source",
        "Type": "GITHUB",
        "DataSourceParameters": {},
        "Credentials": {
            "CredentialPair": {
                "Username": "string",
                "Password": "string",
            },
            "CopySourceArn": "string",
            "SecretArn": "string",
        },
    },
    {
        "DataSourceId": "ae18cb67-4017-4802-a511-f29439a4d6c5",
        "Name": "jira data source",
        "Type": "JIRA",
        "DataSourceParameters": {
            "JiraParameters": {"SiteBaseUrl": "string"},
        },
        "Credentials": {
            "CredentialPair": {
                "Username": "string",
                "Password": "string",
            },
            "CopySourceArn": "string",
            "SecretArn": "string",
        },
    },
    {
        "DataSourceId": "eeb8898e-d4f1-435e-a8e8-9b5796b810fb",
        "Name": "mariadb data source",
        "Type": "MARIADB",
        "DataSourceParameters": {
            "MariaDbParameters": {
                "Host": "string",
                "Port": 123,
                "Database": "string",
            },
        },
        "Credentials": {
            "CredentialPair": {
                "Username": "string",
                "Password": "string",
            },
            "CopySourceArn": "string",
            "SecretArn": "string",
        },
    },
    {
        "DataSourceId": "99273bd6-6211-42e8-959c-d2b84482ff80",
        "Name": "mysql data source",
        "Type": "MYSQL",
        "DataSourceParameters": {
            "MySqlParameters": {
                "Host": "string",
                "Port": 123,
                "Database": "string",
            },
        },
        "Credentials": {
            "CredentialPair": {
                "Username": "string",
                "Password": "string",
            },
            "CopySourceArn": "string",
            "SecretArn": "string",
        },
    },
    {
        "DataSourceId": "aa380ce1-22cd-4200-8d78-af866e5f51e8",
        "Name": "oracle data source",
        "Type": "ORACLE",
        "DataSourceParameters": {
            "OracleParameters": {
                "Host": "string",
                "Port": 123,
                "Database": "string",
            },
        },
        "Credentials": {
            "CredentialPair": {
                "Username": "string",
                "Password": "string",
            },
            "CopySourceArn": "string",
            "SecretArn": "string",
        },
    },
    {
        "DataSourceId": "e8934376-0628-4520-a0e6-4de93329e14e",
        "Name": "postgresql data source",
        "Type": "POSTGRESQL",
        "DataSourceParameters": {
            "PostgreSqlParameters": {
                "Host": "string",
                "Port": 123,
                "Database": "string",
            },
        },
        "Credentials": {
            "CredentialPair": {
                "Username": "string",
                "Password": "string",
            },
            "CopySourceArn": "string",
            "SecretArn": "string",
        },
    },
    {
        "DataSourceId": "43b56e00-5e63-4c80-9a08-883437c768de",
        "Name": "presto data source",
        "Type": "PRESTO",
        "DataSourceParameters": {
            "PrestoParameters": {
                "Host": "string",
                "Port": 123,
                "Catalog": "string",
            },
        },
        "Credentials": {
            "CredentialPair": {
                "Username": "string",
                "Password": "string",
            },
            "CopySourceArn": "string",
            "SecretArn": "string",
        },
    },
    {
        "DataSourceId": "6bde75af-ce07-4bed-ac96-78a1310e4fdf",
        "Name": "redshift data source",
        "Type": "REDSHIFT",
        "DataSourceParameters": {
            "RedshiftParameters": {
                "Host": "string",
                "Port": 123,
                "Database": "string",
                "ClusterId": "string",
                "IAMParameters": {
                    "RoleArn": "arn:aws:iam::123456789012:role/role-name-with-path",
                    "DatabaseUser": "string",
                    "DatabaseGroups": [
                        "string",
                    ],
                    "AutoCreateDatabaseUser": True | False,
                },
                "IdentityCenterConfiguration": {
                    "EnableIdentityPropagation": True | False
                },
            },
        },
        "Credentials": {
            "CredentialPair": {
                "Username": "string",
                "Password": "string",
            },
            "CopySourceArn": "string",
            "SecretArn": "string",
        },
    },
    {
        "DataSourceId": "597e56f9-c0c4-4aa0-a09c-1589e0ba8b8a",
        "Name": "s3 data source",
        "Type": "S3",
        "DataSourceParameters": {
            "S3Parameters": {
                "ManifestFileLocation": {"Bucket": "string", "Key": "string"},
                "RoleArn": "arn:aws:iam::123456789012:role/role-name-with-path",
            },
        },
        "Credentials": {
            "CredentialPair": {
                "Username": "string",
                "Password": "string",
            },
            "CopySourceArn": "string",
            "SecretArn": "string",
        },
    },
    {
        "DataSourceId": "983336cc-6cfc-4389-b831-12152a9394c1",
        "Name": "salesforce data source",
        "Type": "SALESFORCE",
        "DataSourceParameters": {},
        "Credentials": {
            "CredentialPair": {
                "Username": "string",
                "Password": "string",
            },
            "CopySourceArn": "string",
            "SecretArn": "string",
        },
    },
    {
        "DataSourceId": "30cd8c4d-e81a-4a3c-9a70-c9103f4f272c",
        "Name": "servicenow data source",
        "Type": "SERVICENOW",
        "DataSourceParameters": {
            "ServiceNowParameters": {"SiteBaseUrl": "string"},
        },
        "Credentials": {
            "CredentialPair": {
                "Username": "string",
                "Password": "string",
            },
            "CopySourceArn": "string",
            "SecretArn": "string",
        },
    },
    {
        "DataSourceId": "55a38c9b-a7ef-46de-9019-2c35a25cc695",
        "Name": "snowflake data source",
        "Type": "SNOWFLAKE",
        "DataSourceParameters": {
            "SnowflakeParameters": {
                "Host": "string",
                "Database": "string",
                "Warehouse": "string",
            },
        },
        "Credentials": {
            "CredentialPair": {
                "Username": "string",
                "Password": "string",
            },
            "CopySourceArn": "string",
            "SecretArn": "string",
        },
    },
    {
        "DataSourceId": "cf01cc68-c5fa-4c60-adff-25189fa50930",
        "Name": "spark data source",
        "Type": "SPARK",
        "DataSourceParameters": {
            "SparkParameters": {"Host": "string", "Port": 123},
        },
        "Credentials": {
            "CredentialPair": {
                "Username": "string",
                "Password": "string",
            },
            "CopySourceArn": "string",
            "SecretArn": "string",
        },
    },
    {
        "DataSourceId": "c4a2c0a2-5bd2-43a2-8775-e493e4bb31bc",
        "Name": "sqlserver data source",
        "Type": "SQLSERVER",
        "DataSourceParameters": {
            "SqlServerParameters": {
                "Host": "string",
                "Port": 123,
                "Database": "string",
            },
        },
        "Credentials": {
            "CredentialPair": {
                "Username": "string",
                "Password": "string",
            },
            "CopySourceArn": "string",
            "SecretArn": "string",
        },
    },
    {
        "DataSourceId": "6f8955f8-122c-4a7a-b73c-885dfc1c1cf7",
        "Name": "teradata data source",
        "Type": "TERADATA",
        "DataSourceParameters": {
            "TeradataParameters": {
                "Host": "string",
                "Port": 123,
                "Database": "string",
            },
        },
        "Credentials": {
            "CredentialPair": {
                "Username": "string",
                "Password": "string",
            },
            "CopySourceArn": "string",
            "SecretArn": "string",
        },
    },
    {
        "DataSourceId": "ff799369-7c90-4f6d-80c6-410c16bb8799",
        "Name": "twitter data source",
        "Type": "TWITTER",
        "DataSourceParameters": {
            "TwitterParameters": {"Query": "string", "MaxRows": 123},
        },
        "Credentials": {
            "CredentialPair": {
                "Username": "string",
                "Password": "string",
            },
            "CopySourceArn": "string",
            "SecretArn": "string",
        },
    },
    {
        "DataSourceId": "d7605366-6928-4682-9f52-d15a990150dd",
        "Name": "timestream data source",
        "Type": "TIMESTREAM",
        "DataSourceParameters": {},
        "Credentials": {
            "CredentialPair": {
                "Username": "string",
                "Password": "string",
            },
            "CopySourceArn": "string",
            "SecretArn": "string",
        },
    },
    {
        "DataSourceId": "58104ec2-5f9f-4007-aff9-0c5ae585bb56",
        "Name": "amazon opensearch data source",
        "Type": "AMAZON_OPENSEARCH",
        "DataSourceParameters": {
            "AmazonOpenSearchParameters": {"Domain": "string"},
        },
        "Credentials": {
            "CredentialPair": {
                "Username": "string",
                "Password": "string",
            },
            "CopySourceArn": "string",
            "SecretArn": "string",
        },
    },
    {
        "DataSourceId": "957f0378-047f-4d20-a9cb-d6ead450c8d9",
        "Name": "exasol data source",
        "Type": "EXASOL",
        "DataSourceParameters": {
            "ExasolParameters": {"Host": "string", "Port": 123},
        },
        "Credentials": {
            "CredentialPair": {
                "Username": "string",
                "Password": "string",
            },
            "CopySourceArn": "string",
            "SecretArn": "string",
        },
    },
    {
        "DataSourceId": "fec6489d-eaf7-487f-9c9b-f9abef3e49fa",
        "Name": "databricks data source",
        "Type": "DATABRICKS",
        "DataSourceParameters": {
            "DatabricksParameters": {
                "Host": "string",
                "Port": 123,
                "SqlEndpointPath": "string",
            },
        },
        "Credentials": {
            "CredentialPair": {
                "Username": "string",
                "Password": "string",
            },
            "CopySourceArn": "string",
            "SecretArn": "string",
        },
    },
    {
        "DataSourceId": "2be013bc-6bd6-4811-9f9a-ea436f4bf3c9",
        "Name": "starburst data source",
        "Type": "STARBURST",
        "DataSourceParameters": {
            "StarburstParameters": {
                "Host": "string",
                "Port": 123,
                "Catalog": "string",
                "ProductType": "ENTERPRISE",
            },
        },
        "Credentials": {
            "CredentialPair": {
                "Username": "string",
                "Password": "string",
            },
            "CopySourceArn": "string",
            "SecretArn": "string",
        },
    },
    {
        "DataSourceId": "d1cfe933-cebd-4aa9-abcf-1657a24984d1",
        "Name": "trino data source",
        "Type": "TRINO",
        "DataSourceParameters": {
            "TrinoParameters": {"Host": "string", "Port": 123, "Catalog": "string"},
        },
        "Credentials": {
            "CredentialPair": {
                "Username": "string",
                "Password": "string",
            },
            "CopySourceArn": "string",
            "SecretArn": "string",
        },
    },
    {
        "DataSourceId": "7e5dbc06-5132-4f00-8260-65c43ab49652",
        "Name": "bigquery data source",
        "Type": "BIGQUERY",
        "DataSourceParameters": {
            "BigQueryParameters": {"ProjectId": "string", "DataSetRegion": "string"}
        },
        "Credentials": {
            "CredentialPair": {
                "Username": "string",
                "Password": "string",
            },
            "CopySourceArn": "string",
            "SecretArn": "string",
        },
    },
]


@pytest.mark.parametrize("request_params", data_sources, ids=idfn)
@mock_aws
def test_create_data_source(request_params):
    client = boto3.client("quicksight", region_name="eu-west-1")

    resp = client.create_data_source(
        AwsAccountId=ACCOUNT_ID,
        DataSourceId=request_params["DataSourceId"],
        Name=request_params["Name"],
        Type=request_params["Type"],
        DataSourceParameters=request_params["DataSourceParameters"],
        Credentials=request_params["Credentials"],
        Permissions=[
            {
                "Principal": "Some Principal ARN",
                "Actions": ["Permission 1", "Permission 2"],
            },
        ],
        VpcConnectionProperties={"VpcConnectionArn": "Some text without meaning"},
        SslProperties={"DisableSsl": False},
        Tags=[
            {"Key": "Name", "Value": request_params["Name"]},
        ],
        FolderArns=[
            f"arn:aws:quicksight:eu-west-1:{ACCOUNT_ID}:folder/myfolder",
        ],
    )

    assert resp["Arn"] == (
        f"arn:aws:quicksight:eu-west-1:{ACCOUNT_ID}:data-source/{request_params['DataSourceId']}"
    )
    assert resp["DataSourceId"] == request_params["DataSourceId"]


@mock_aws
def test_describe_data_source():
    client = boto3.client("quicksight", region_name="eu-west-1")

    # I don't use parametrize here to make shure multiple data sources exist on the server when describing each of them.
    for data_source in data_sources:
        client.create_data_source(
            AwsAccountId=ACCOUNT_ID,
            DataSourceId=data_source["DataSourceId"],
            Name=data_source["Name"],
            Type=data_source["Type"],
            DataSourceParameters=data_source["DataSourceParameters"],
            Credentials=data_source["Credentials"],
            Permissions=[
                {
                    "Principal": "Some Principal ARN",
                    "Actions": ["Permission 1", "Permission 2"],
                },
            ],
            VpcConnectionProperties={"VpcConnectionArn": "Some text without meaning"},
            SslProperties={"DisableSsl": False},
            Tags=[
                {"Key": "Name", "Value": data_source["Name"]},
            ],
            FolderArns=[
                f"arn:aws:quicksight:eu-west-1:{ACCOUNT_ID}:folder/myfolder",
            ],
        )

    for data_source in data_sources:
        resp = client.describe_data_source(
            AwsAccountId=ACCOUNT_ID,
            DataSourceId=data_source["DataSourceId"],
        )

        assert "DataSource" in resp

        assert resp["DataSource"]["Arn"] == (
            f"arn:aws:quicksight:eu-west-1:{ACCOUNT_ID}:data-source/{data_source['DataSourceId']}"
        )
        assert resp["DataSource"]["DataSourceId"] == data_source["DataSourceId"]
        assert resp["DataSource"]["Name"] == data_source["Name"]
        assert resp["DataSource"]["Type"] == data_source["Type"]
        assert "CreatedTime" in resp["DataSource"]
        assert "LastUpdatedTime" in resp["DataSource"]
        assert (
            resp["DataSource"]["DataSourceParameters"]
            == data_source["DataSourceParameters"]
        )
    assert resp["DataSource"]["VpcConnectionProperties"] == {
        "VpcConnectionArn": "Some text without meaning"
    }
