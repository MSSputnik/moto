"""Unit tests for quicksight-supported APIs."""

import boto3
import pytest
from botocore.exceptions import ClientError, ParamValidationError

from moto import mock_aws
from moto.core import DEFAULT_ACCOUNT_ID as ACCOUNT_ID

# See our Development Tips on writing tests for hints on how to write good tests:
# http://docs.getmoto.org/en/latest/docs/contributing/development_tips/tests.html


@pytest.mark.parametrize(
    "input, output, exception",
    [
        (
            {"Test": "X"},
            None,
            {
                "Error": "Parameter validation failed:\nInvalid length for parameter FolderId, value: 0, valid min length: 1"
            },
        ),
        (
            {
                "FolderId": "hast@Test",
            },
            None,
            {
                "Code": "ValidationException",
                "Message": "1 validation error detected: Value 'hast@Test' at 'folderId' failed to satisfy constraint: Member must satisfy regular expression pattern: [\\w\\-]+",
            },
        ),
        (
            {
                "FolderId": "3367dfe7-f60d-4229-90c6-ab9953d670ff",
            },
            None,
            {
                "Code": "InvalidParameterValueException",
                "Message": "No folder name found",
            },
        ),
        (
            {
                "FolderId": "3367dfe7-f60d-4229-90c6-ab9953d670ff",
                "Name": "MyFirstFolder",
            },
            {
                "Arn": f"arn:aws:quicksight:us-west-2:{ACCOUNT_ID}:folder/3367dfe7-f60d-4229-90c6-ab9953d670ff",
                "FolderId": "3367dfe7-f60d-4229-90c6-ab9953d670ff",
                "Name": "MyFirstFolder",
                "FolderType": "SHARED",
                "FolderPath": [],
                "SharingModel": "ACCOUNT",
            },
            None,
        ),
        (
            {
                "FolderId": "3367dfe7-f60d-4229-90c6-ab9953d670ff",
                "Name": "MyFirstFolder",
                "FolderType": "RESTRICTED",
                "SharingModel": "NAMESPACE",
            },
            {
                "Arn": f"arn:aws:quicksight:us-west-2:{ACCOUNT_ID}:folder/3367dfe7-f60d-4229-90c6-ab9953d670ff",
                "FolderId": "3367dfe7-f60d-4229-90c6-ab9953d670ff",
                "Name": "MyFirstFolder",
                "FolderType": "RESTRICTED",
                "FolderPath": [],
                "SharingModel": "NAMESPACE",
            },
            None,
        ),
    ],
)
@mock_aws
def test_create_folder__describe(input, output, exception):
    client = boto3.client("quicksight", region_name="us-west-2")
    param_dict = {
        "AwsAccountId": ACCOUNT_ID,
        "FolderId": input.get("FolderId", ""),
    }
    if input.get("Name", None):
        param_dict["Name"] = input["Name"]
    if input.get("FolderType", None):
        param_dict["FolderType"] = input["FolderType"]
    if input.get("ParentFolderArn", None):
        param_dict["ParentFolderArn"] = input["ParentFolderArn"]
    if input.get("Tags", None):
        param_dict["Tags"] = input["Tags"]
    if input.get("Permissions", None):
        param_dict["Permissions"] = input["Permissions"]
    if input.get("SharingModel", None):
        param_dict["SharingModel"] = input["SharingModel"]

    if exception:  # Response expects exception
        with pytest.raises((ClientError, ParamValidationError, Exception)) as exc:
            client.create_folder(**param_dict)

        if isinstance(exc.value, ParamValidationError):  # ParamValidationError
            assert str(exc.value) == exception.get("Error", "")

        else:  # ClientError
            err = exc.value.response["Error"]
            assert err["Code"] == exception.get("Code", None)
            if exception.get("Message", None) is not None:
                assert err["Message"] == exception.get("Message")

    else:
        resp = client.create_folder(**param_dict)

        assert "FolderId" in resp
        assert resp["FolderId"] == input.get("FolderId", "")

        assert "Arn" in resp
        assert (
            resp["Arn"]
            == f"arn:aws:quicksight:us-west-2:{ACCOUNT_ID}:folder/{input['FolderId']}"
        )

        resp = client.describe_folder(
            AwsAccountId=ACCOUNT_ID, FolderId=input["FolderId"]
        )

        assert resp["Status"] == 200
        assert "Folder" in resp

        folder_data = resp["Folder"]

        for key, value in output.items():
            assert key in folder_data
            assert folder_data[key] == value


@pytest.mark.parametrize(
    "input, permissions, resolved_permissions, exception",
    [
        (
            {
                "FolderId": "3367dfe7-f60d-4229-90c6-ab9953d670ff",
                "Name": "MyFirstFolder",
            },
            [],
            None,
            None,
        ),
        (
            {
                "FolderId": "3367dfe7-f60d-4229-90c6-ab9953d670ff",
                "Name": "MyFirstFolder",
                "Permissions": [
                    {
                        "Principal": f"arn:aws:quicksight:us-west-2:{ACCOUNT_ID}:user/default/does-not-exist",
                        "Actions": ["X"],
                    }
                ],
            },
            None,
            None,
            {
                "Code": "InvalidParameterValueException",
                "Message": f"Principal ARN arn:aws:quicksight:us-west-2:{ACCOUNT_ID}:user/default/does-not-exist is not part of the same account {ACCOUNT_ID}",
            },
        ),
        (
            {
                "FolderId": "3367dfe7-f60d-4229-90c6-ab9953d670ff",
                "Name": "MyFirstFolder",
                "Permissions": [
                    {
                        "Principal": f"arn:aws:quicksight:us-west-2:{ACCOUNT_ID}:user/default/user1",
                        "Actions": ["X"],
                    },
                    {
                        "Principal": f"arn:aws:quicksight:us-west-2:{ACCOUNT_ID}:group/default/group1",
                        "Actions": ["X"],
                    },
                ],
            },
            None,
            None,
            {
                "Code": "InvalidParameterValueException",
                "Message": "ResourcePermission list contains unsupported permission sets ['X'] for this resource. Valid sets : ['quicksight:DescribeFolder'] or ['quicksight:CreateFolder', 'quicksight:DescribeFolder', 'quicksight:CreateFolderMembership', 'quicksight:DeleteFolderMembership', 'quicksight:DescribeFolderPermissions'] or ['quicksight:CreateFolder', 'quicksight:DescribeFolder', 'quicksight:UpdateFolder', 'quicksight:DeleteFolder', 'quicksight:CreateFolderMembership', 'quicksight:DeleteFolderMembership', 'quicksight:DescribeFolderPermissions', 'quicksight:UpdateFolderPermissions']",
            },
        ),
        (
            {
                "FolderId": "3367dfe7-f60d-4229-90c6-ab9953d670ff",
                "Name": "MyFirstFolder",
                "Permissions": [
                    {
                        "Principal": f"arn:aws:quicksight:us-west-2:{ACCOUNT_ID}:user/default/user1",
                        "Actions": [
                            "quicksight:CreateFolder",
                            "quicksight:CreateFolderMembership",
                            "quicksight:DeleteFolder",
                            "quicksight:DeleteFolderMembership",
                            "quicksight:DescribeFolder",
                            "quicksight:DescribeFolderPermissions",
                            "quicksight:UpdateFolder",
                            "quicksight:UpdateFolderPermissions",
                        ],
                    },
                    {
                        "Principal": f"arn:aws:quicksight:us-west-2:{ACCOUNT_ID}:group/default/group1",
                        "Actions": ["quicksight:DescribeFolder"],
                    },
                ],
            },
            [
                {
                    "Principal": f"arn:aws:quicksight:us-west-2:{ACCOUNT_ID}:user/default/user1",
                    "Actions": [
                        "quicksight:CreateFolder",
                        "quicksight:CreateFolderMembership",
                        "quicksight:DeleteFolder",
                        "quicksight:DeleteFolderMembership",
                        "quicksight:DescribeFolder",
                        "quicksight:DescribeFolderPermissions",
                        "quicksight:UpdateFolder",
                        "quicksight:UpdateFolderPermissions",
                    ],
                },
                {
                    "Principal": f"arn:aws:quicksight:us-west-2:{ACCOUNT_ID}:group/default/group1",
                    "Actions": ["quicksight:DescribeFolder"],
                },
            ],
            None,
            None,
        ),
        (
            {
                "FolderId": "3367dfe7-f60d-4229-90c6-ab9953d670ff",
                "Name": "MyFirstFolder",
                "Permissions": [
                    {
                        "Principal": f"arn:aws:quicksight:us-west-2:{ACCOUNT_ID}:user/default/user1",
                        "Actions": [
                            "quicksight:CreateFolder",
                            "quicksight:CreateFolderMembership",
                            "quicksight:DeleteFolder",
                            "quicksight:DeleteFolderMembership",
                            "quicksight:DescribeFolder",
                            "quicksight:DescribeFolderPermissions",
                            "quicksight:UpdateFolder",
                            "quicksight:UpdateFolderPermissions",
                        ],
                    },
                    {
                        "Principal": f"arn:aws:quicksight:us-west-2:{ACCOUNT_ID}:group/default/group1",
                        "Actions": ["quicksight:DescribeFolder"],
                    },
                ],
            },
            None,
            [
                {
                    "Principal": f"arn:aws:quicksight:us-west-2:{ACCOUNT_ID}:user/default/user1",
                    "Actions": [
                        "quicksight:CreateFolder",
                        "quicksight:CreateFolderMembership",
                        "quicksight:DeleteFolder",
                        "quicksight:DeleteFolderMembership",
                        "quicksight:DescribeFolder",
                        "quicksight:DescribeFolderPermissions",
                        "quicksight:UpdateFolder",
                        "quicksight:UpdateFolderPermissions",
                    ],
                },
                {
                    "Principal": f"arn:aws:quicksight:us-west-2:{ACCOUNT_ID}:group/default/group1",
                    "Actions": ["quicksight:DescribeFolder"],
                },
            ],
            None,
        ),
    ],
)
@mock_aws
def test_create_folder__describe_permissions(
    input, permissions, resolved_permissions, exception
):
    client = boto3.client("quicksight", region_name="us-west-2")
    # create a user and a group for testing
    client.register_user(  #
        AwsAccountId=ACCOUNT_ID,  #
        Namespace="default",
        IdentityType="QUICKSIGHT",
        UserName="user1",
        Email="user1@test.com",
        UserRole="AUTHOR",
    )
    client.create_group(  #
        AwsAccountId=ACCOUNT_ID,  #
        Namespace="default",
        GroupName="group1",
    )

    param_dict = {
        "AwsAccountId": ACCOUNT_ID,
        "FolderId": input.get("FolderId", ""),
    }
    if input.get("Name", None):
        param_dict["Name"] = input["Name"]
    if input.get("FolderType", None):
        param_dict["FolderType"] = input["FolderType"]
    if input.get("ParentFolderArn", None):
        param_dict["ParentFolderArn"] = input["ParentFolderArn"]
    if input.get("Tags", None):
        param_dict["Tags"] = input["Tags"]
    if input.get("Permissions", None):
        param_dict["Permissions"] = input["Permissions"]
    if input.get("SharingModel", None):
        param_dict["SharingModel"] = input["SharingModel"]

    if exception:  # Response expects exception
        with pytest.raises((ClientError, ParamValidationError, Exception)) as exc:
            client.create_folder(**param_dict)

        if isinstance(exc.value, ParamValidationError):  # ParamValidationError
            assert str(exc.value) == exception.get("Error", "")

        else:  # ClientError
            err = exc.value.response["Error"]
            assert err["Code"] == exception.get("Code", None)
            if exception.get("Message", None) is not None:
                assert err["Message"] == exception.get("Message")

    else:
        resp = client.create_folder(**param_dict)

        assert "FolderId" in resp
        assert resp["FolderId"] == input.get("FolderId", "")

        assert "Arn" in resp
        assert (
            resp["Arn"]
            == f"arn:aws:quicksight:us-west-2:{ACCOUNT_ID}:folder/{input['FolderId']}"
        )

        if permissions:
            resp = client.describe_folder_permissions(
                AwsAccountId=ACCOUNT_ID, FolderId=input["FolderId"]
            )

            assert resp["Status"] == 200
            assert "Permissions" in resp

            permission_data = resp["Permissions"]

            assert permission_data == permissions

        if resolved_permissions:
            resp = client.describe_folder_resolved_permissions(
                AwsAccountId=ACCOUNT_ID, FolderId=input["FolderId"]
            )

            assert resp["Status"] == 200
            assert "Permissions" in resp

            permission_data = resp["Permissions"]

            assert permission_data == resolved_permissions


# @pytest.mark.parametrize(
#     "request_params",
#     [
#         {
#             "GroupName": "mygroup",
#             "Description": "my new fancy group",
#         },
#         {
#             "GroupName": "users@munich",
#             "Description": "all munich users",
#         },
#     ],
# )
# @mock_aws
# def test_describe_group(request_params):
#     client = boto3.client("quicksight", region_name="us-west-2")
#     client.create_group(
#         AwsAccountId=ACCOUNT_ID,
#         Namespace="default",
#         GroupName=request_params["GroupName"],
#         Description=request_params["Description"],
#     )

#     resp = client.describe_group(
#         GroupName=request_params["GroupName"],
#         AwsAccountId=ACCOUNT_ID,
#         Namespace="default",
#     )

#     assert "Group" in resp

#     assert resp["Group"]["Arn"] == (
#         f"arn:aws:quicksight:us-west-2:{ACCOUNT_ID}:group/default/{request_params['GroupName']}"
#     )
#     assert resp["Group"]["GroupName"] == request_params["GroupName"]
#     assert resp["Group"]["Description"] == request_params["Description"]
#     assert resp["Group"]["PrincipalId"] == f"{ACCOUNT_ID}"


# @pytest.mark.parametrize(
#     "request_params",
#     [
#         {
#             "GroupName": "mygroup",
#             "Description": "my new fancy group",
#         },
#         {
#             "GroupName": "users@munich",
#             "Description": "all munich users",
#         },
#     ],
# )
# @mock_aws
# def test_update_group(request_params):
#     client = boto3.client("quicksight", region_name="us-west-2")
#     client.create_group(
#         AwsAccountId=ACCOUNT_ID,
#         Namespace="default",
#         GroupName=request_params["GroupName"],
#         Description="desc1",
#     )

#     resp = client.update_group(
#         GroupName=request_params["GroupName"],
#         AwsAccountId=ACCOUNT_ID,
#         Namespace="default",
#         Description="desc2",
#     )
#     assert resp["Group"]["Description"] == "desc2"

#     resp = client.describe_group(
#         GroupName=request_params["GroupName"],
#         AwsAccountId=ACCOUNT_ID,
#         Namespace="default",
#     )

#     assert "Group" in resp
#     assert resp["Group"]["Arn"] == (
#         f"arn:aws:quicksight:us-west-2:{ACCOUNT_ID}:group/default/{request_params['GroupName']}"
#     )
#     assert resp["Group"]["GroupName"] == request_params["GroupName"]
#     assert resp["Group"]["Description"] == "desc2"
#     assert resp["Group"]["PrincipalId"] == f"{ACCOUNT_ID}"


# @pytest.mark.parametrize(
#     "request_params",
#     [
#         {
#             "GroupName": "mygroup",
#             "Description": "my new fancy group",
#         },
#         {
#             "GroupName": "users@munich",
#             "Description": "all munich users",
#         },
#     ],
# )
# @mock_aws
# def test_delete_group(request_params):
#     client = boto3.client("quicksight", region_name="us-east-2")
#     client.create_group(
#         AwsAccountId=ACCOUNT_ID,
#         Namespace="default",
#         GroupName=request_params["GroupName"],
#         Description=request_params["Description"],
#     )

#     client.delete_group(
#         GroupName=request_params["GroupName"],
#         AwsAccountId=ACCOUNT_ID,
#         Namespace="default",
#     )

#     with pytest.raises(ClientError) as exc:
#         client.describe_group(
#             GroupName=request_params["GroupName"],
#             AwsAccountId=ACCOUNT_ID,
#             Namespace="default",
#         )
#     err = exc.value.response["Error"]
#     assert err["Code"] == "ResourceNotFoundException"


# @mock_aws
# def test_list_groups__initial():
#     client = boto3.client("quicksight", region_name="us-east-2")
#     resp = client.list_groups(AwsAccountId=ACCOUNT_ID, Namespace="default")

#     assert resp["GroupList"] == []
#     assert resp["Status"] == 200


# @mock_aws
# def test_list_groups():
#     client = boto3.client("quicksight", region_name="us-east-1")
#     for i in range(4):
#         group_name = f"group{i}" if i < 2 else f"group{i}@test"
#         client.create_group(
#             AwsAccountId=ACCOUNT_ID, Namespace="default", GroupName=group_name
#         )

#     resp = client.list_groups(AwsAccountId=ACCOUNT_ID, Namespace="default")

#     assert len(resp["GroupList"]) == 4
#     assert resp["Status"] == 200

#     assert {
#         "Arn": f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:group/default/group0",
#         "GroupName": "group0",
#         "PrincipalId": ACCOUNT_ID,
#     } in resp["GroupList"]

#     assert {
#         "Arn": f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:group/default/group3@test",
#         "GroupName": "group3@test",
#         "PrincipalId": ACCOUNT_ID,
#     } in resp["GroupList"]


# @mock_aws
# def test_list_groups__paginated():
#     client = boto3.client("quicksight", region_name="us-east-1")
#     for i in range(125):
#         client.create_group(
#             AwsAccountId=ACCOUNT_ID, Namespace="default", GroupName=f"group{i}"
#         )
#     # default pagesize is 100
#     page1 = client.list_groups(AwsAccountId=ACCOUNT_ID, Namespace="default")
#     assert len(page1["GroupList"]) == 100
#     assert "NextToken" in page1

#     # We can ask for a smaller pagesize
#     page2 = client.list_groups(
#         AwsAccountId=ACCOUNT_ID,
#         Namespace="default",
#         MaxResults=15,
#         NextToken=page1["NextToken"],
#     )
#     assert len(page2["GroupList"]) == 15
#     assert "NextToken" in page2

#     # We could request all of them in one go
#     all_users = client.list_groups(
#         AwsAccountId=ACCOUNT_ID,
#         Namespace="default",
#         MaxResults=1000,
#     )
#     length = len(all_users["GroupList"])
#     # We don't know exactly how much workspaces there are, because we are running multiple tests at the same time
#     assert length >= 125


# @mock_aws
# def test_search_groups():
#     client = boto3.client("quicksight", region_name="us-east-1")
#     for i in range(4):
#         group_name = f"group{i}" if i < 2 else f"test{i}@test"
#         client.create_group(
#             AwsAccountId=ACCOUNT_ID, Namespace="default", GroupName=group_name
#         )

#     resp = client.search_groups(
#         AwsAccountId=ACCOUNT_ID,
#         Namespace="default",
#         Filters=[
#             {"Operator": "StringEquals", "Name": "GROUP_NAME", "Value": "group1"},
#         ],
#     )

#     assert len(resp["GroupList"]) == 1
#     assert resp["Status"] == 200

#     assert {
#         "Arn": f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:group/default/group1",
#         "GroupName": "group1",
#         "PrincipalId": ACCOUNT_ID,
#     } in resp["GroupList"]


# @mock_aws
# def test_search_groups__paginated():
#     client = boto3.client("quicksight", region_name="us-east-1")
#     for i in range(250):
#         group_name = f"group{i}" if i % 2 else f"test{i}@test"
#         client.create_group(
#             AwsAccountId=ACCOUNT_ID, Namespace="default", GroupName=group_name
#         )

#     # default pagesize is 100
#     page1 = client.search_groups(
#         AwsAccountId=ACCOUNT_ID,
#         Namespace="default",
#         Filters=[
#             {"Operator": "StartsWith", "Name": "GROUP_NAME", "Value": "group"},
#         ],
#     )
#     assert len(page1["GroupList"]) == 100
#     assert "NextToken" in page1

#     # We can ask for a smaller pagesize
#     page2 = client.search_groups(
#         AwsAccountId=ACCOUNT_ID,
#         Namespace="default",
#         Filters=[
#             {"Operator": "StartsWith", "Name": "GROUP_NAME", "Value": "group"},
#         ],
#         MaxResults=15,
#         NextToken=page1["NextToken"],
#     )
#     assert len(page2["GroupList"]) == 15
#     assert "NextToken" in page2

#     # We could request all of them in one go
#     all_users = client.search_groups(
#         AwsAccountId=ACCOUNT_ID,
#         Namespace="default",
#         Filters=[
#             {"Operator": "StartsWith", "Name": "GROUP_NAME", "Value": "group"},
#         ],
#         MaxResults=1000,
#     )
#     length = len(all_users["GroupList"])
#     # We don't know exactly how much workspaces there are, because we are running multiple tests at the same time
#     assert length >= 125


# @mock_aws
# def test_list_groups__diff_account_region():
#     ACCOUNT_ID_2 = "998877665544"
#     client_us = boto3.client("quicksight", region_name="us-east-2")
#     client_eu = boto3.client("quicksight", region_name="eu-west-1")
#     resp = client_us.create_group(
#         AwsAccountId=ACCOUNT_ID,
#         Namespace="default",
#         GroupName="group_us_1",
#         Description="Group in US Account 1",
#     )
#     resp = client_us.create_group(
#         AwsAccountId=ACCOUNT_ID_2,
#         Namespace="default",
#         GroupName="group_us_2",
#         Description="Group in US Account 2",
#     )
#     resp = client_eu.create_group(
#         AwsAccountId=ACCOUNT_ID,
#         Namespace="default",
#         GroupName="group_eu_1",
#         Description="Group in EU Account 1",
#     )

#     # Return Account 1, Region US
#     resp = client_us.list_groups(AwsAccountId=ACCOUNT_ID, Namespace="default")
#     assert len(resp["GroupList"]) == 1
#     assert resp["Status"] == 200

#     resp["GroupList"][0].pop("PrincipalId")

#     assert resp["GroupList"][0] == {
#         "Arn": f"arn:aws:quicksight:us-east-2:{ACCOUNT_ID}:group/default/group_us_1",
#         "GroupName": "group_us_1",
#         "Description": "Group in US Account 1",
#     }

#     # Return Account 2, Region US
#     resp = client_us.list_groups(AwsAccountId=ACCOUNT_ID_2, Namespace="default")

#     assert len(resp["GroupList"]) == 1
#     assert resp["Status"] == 200

#     resp["GroupList"][0].pop("PrincipalId")

#     assert resp["GroupList"][0] == {
#         "Arn": f"arn:aws:quicksight:us-east-2:{ACCOUNT_ID_2}:group/default/group_us_2",
#         "GroupName": "group_us_2",
#         "Description": "Group in US Account 2",
#     }

#     # Return Account 1, Region EU
#     resp = client_eu.list_groups(AwsAccountId=ACCOUNT_ID, Namespace="default")

#     assert len(resp["GroupList"]) == 1
#     assert resp["Status"] == 200

#     resp["GroupList"][0].pop("PrincipalId")

#     assert resp["GroupList"][0] == {
#         "Arn": f"arn:aws:quicksight:eu-west-1:{ACCOUNT_ID}:group/default/group_eu_1",
#         "GroupName": "group_eu_1",
#         "Description": "Group in EU Account 1",
#     }


# @mock_aws
# def test_search_groups__check_exceptions():
#     client = boto3.client("quicksight", region_name="us-east-1")
#     # Just do an exception test. No need to create a group first.

#     with pytest.raises(ClientError) as exc:
#         client.search_groups(
#             AwsAccountId=ACCOUNT_ID,
#             Namespace="default",
#             Filters=[
#                 {
#                     "Operator": "StringEquals",
#                     "Name": "GROUP_DESCRIPTION",
#                     "Value": "My Group 1",
#                 },
#             ],
#         )
#     err = exc.value.response["Error"]
#     assert err["Code"] == "ValidationException"
