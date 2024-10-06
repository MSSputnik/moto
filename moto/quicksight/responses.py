"""Handles incoming quicksight requests, invokes methods, returns responses."""

import json
from urllib.parse import unquote

from moto.core.common_types import TYPE_RESPONSE
from moto.core.responses import BaseResponse

from .models import QuickSightBackend, quicksight_backends


class QuickSightResponse(BaseResponse):
    def __init__(self) -> None:
        super().__init__(service_name="quicksight")

    @property
    def quicksight_backend(self) -> QuickSightBackend:
        """Return backend instance specific for this region."""
        return quicksight_backends[self.current_account][self.region]

    def create_data_set(self) -> str:
        params = json.loads(self.body)
        data_set_id = params.get("DataSetId")
        name = params.get("Name")
        data_set = self.quicksight_backend.create_data_set(data_set_id, name)
        return json.dumps(data_set.to_json())

    def create_group(self) -> str:
        params = json.loads(self.body)
        group_name = params.get("GroupName")
        description = params.get("Description")
        aws_account_id = self.path.split("/")[-4]
        namespace = self.path.split("/")[-2]
        group = self.quicksight_backend.create_group(
            group_name=group_name,
            description=description,
            aws_account_id=aws_account_id,
            namespace=namespace,
        )
        return json.dumps(dict(Group=group.to_json()))

    def create_group_membership(self) -> str:
        aws_account_id = self.path.split("/")[-7]
        namespace = self.path.split("/")[-5]
        group_name = unquote(self.path.split("/")[-3])
        user_name = unquote(self.path.split("/")[-1])
        member = self.quicksight_backend.create_group_membership(
            aws_account_id, namespace, group_name, user_name
        )
        return json.dumps({"GroupMember": member.to_json()})

    def create_ingestion(self) -> str:
        data_set_id = self.path.split("/")[-3]
        ingestion_id = self.path.split("/")[-1]
        ingestion = self.quicksight_backend.create_ingestion(data_set_id, ingestion_id)
        return json.dumps(ingestion.to_json())

    def describe_group_membership(self) -> str:
        aws_account_id = self.path.split("/")[-7]
        namespace = self.path.split("/")[-5]
        group_name = unquote(self.path.split("/")[-3])
        user_name = unquote(self.path.split("/")[-1])
        member = self.quicksight_backend.describe_group_membership(
            aws_account_id, namespace, group_name, user_name
        )
        return json.dumps({"GroupMember": member.to_json()})

    def list_groups(self) -> str:
        max_results = self._get_int_param("max-results")
        next_token = self._get_param("next-token")
        aws_account_id = self.path.split("/")[-4]
        namespace = self.path.split("/")[-2]
        groups, next_token = self.quicksight_backend.list_groups(
            aws_account_id, namespace, max_results=max_results, next_token=next_token
        )
        return json.dumps(
            {"NextToken": next_token, "GroupList": [g.to_json() for g in groups]}
        )

    def list_group_memberships(self) -> str:
        max_results = self._get_int_param("max-results")
        next_token = self._get_param("next-token")
        aws_account_id = self.path.split("/")[-6]
        namespace = self.path.split("/")[-4]
        group_name = unquote(self.path.split("/")[-2])
        members, next_token = self.quicksight_backend.list_group_memberships(
            aws_account_id,
            namespace,
            group_name,
            max_results=max_results,
            next_token=next_token,
        )
        return json.dumps(
            {"NextToken": next_token, "GroupMemberList": [m.to_json() for m in members]}
        )

    def list_users(self) -> str:
        max_results = self._get_int_param("max-results")
        next_token = self._get_param("next-token")
        aws_account_id = self.path.split("/")[-4]
        namespace = self.path.split("/")[-2]
        users, next_token = self.quicksight_backend.list_users(
            aws_account_id, namespace, max_results=max_results, next_token=next_token
        )
        return json.dumps(
            {"NextToken": next_token, "UserList": [u.to_json() for u in users]}
        )

    def list_user_groups(self) -> str:
        max_results = self._get_int_param("max-results")
        next_token = self._get_param("next-token")
        aws_account_id = self.path.split("/")[-6]
        namespace = self.path.split("/")[-4]
        user_name = unquote(self.path.split("/")[-2])
        groups, next_token = self.quicksight_backend.list_user_groups(
            aws_account_id,
            namespace,
            user_name,
            max_results=max_results,
            next_token=next_token,
        )
        return json.dumps(
            {"NextToken": next_token, "GroupList": [g.to_json() for g in groups]}
        )

    def register_user(self) -> str:
        params = json.loads(self.body)
        identity_type = params.get("IdentityType")
        email = params.get("Email")
        user_role = params.get("UserRole")
        aws_account_id = self.path.split("/")[-4]
        namespace = self.path.split("/")[-2]
        user_name = params.get("UserName")
        user = self.quicksight_backend.register_user(
            identity_type=identity_type,
            email=email,
            user_role=user_role,
            aws_account_id=aws_account_id,
            namespace=namespace,
            user_name=user_name,
        )
        return json.dumps(dict(User=user.to_json(), UserInvitationUrl="TBD"))

    def update_user(self) -> str:
        aws_account_id = self.path.split("/")[-5]
        namespace = self.path.split("/")[-3]
        user_name = unquote(self.path.split("/")[-1])
        body = json.loads(self.body)
        email = body.get("Email", None)
        user_role = body.get("Role", None)

        user = self.quicksight_backend.update_user(
            aws_account_id, namespace, user_name, email, user_role
        )
        return json.dumps(dict(User=user.to_json()))

    def describe_group(self) -> str:
        aws_account_id = self.path.split("/")[-5]
        namespace = self.path.split("/")[-3]
        group_name = unquote(self.path.split("/")[-1])

        group = self.quicksight_backend.describe_group(
            aws_account_id, namespace, group_name
        )
        return json.dumps(dict(Group=group.to_json()))

    def describe_user(self) -> str:
        aws_account_id = self.path.split("/")[-5]
        namespace = self.path.split("/")[-3]
        user_name = unquote(self.path.split("/")[-1])

        user = self.quicksight_backend.describe_user(
            aws_account_id, namespace, user_name
        )
        return json.dumps(dict(User=user.to_json()))

    def delete_group(self) -> TYPE_RESPONSE:
        aws_account_id = self.path.split("/")[-5]
        namespace = self.path.split("/")[-3]
        group_name = unquote(self.path.split("/")[-1])

        self.quicksight_backend.delete_group(aws_account_id, namespace, group_name)
        return 204, {"status": 204}, json.dumps({"Status": 204})

    def delete_user(self) -> TYPE_RESPONSE:
        aws_account_id = self.path.split("/")[-5]
        namespace = self.path.split("/")[-3]
        user_name = unquote(self.path.split("/")[-1])

        self.quicksight_backend.delete_user(aws_account_id, namespace, user_name)
        return 204, {"status": 204}, json.dumps({"Status": 204})

    def update_group(self) -> str:
        aws_account_id = self.path.split("/")[-5]
        namespace = self.path.split("/")[-3]
        group_name = unquote(self.path.split("/")[-1])
        description = json.loads(self.body).get("Description")

        group = self.quicksight_backend.update_group(
            aws_account_id, namespace, group_name, description
        )
        return json.dumps(dict(Group=group.to_json()))

    def search_groups(self) -> str:
        max_results = self._get_int_param("max-results")
        next_token = self._get_param("next-token")
        aws_account_id = self.path.split("/")[-4]
        namespace = self.path.split("/")[-2]
        body = json.loads(self.body)

        groups, next_token = self.quicksight_backend.search_groups(
            aws_account_id,
            namespace,
            body.get("Filters", None),
            max_results=max_results,
            next_token=next_token,
        )
        return json.dumps(
            {"NextToken": next_token, "GroupList": [g.to_json() for g in groups]}
        )

    def create_data_source(self) -> str:
        body_params = json.loads(self.body)
        account_id = self._get_param("AwsAccountId")
        data_source_id = body_params.get("DataSourceId", None)
        name = body_params.get("Name", None)
        ds_type = body_params.get("Type", None)
        data_source_parameters = body_params.get("DataSourceParameters", None)
        credentials = body_params.get("Credentials", None)
        permissions = body_params.get("Permissions", None)
        vpc_connection_properties = body_params.get("VpcConnectionProperties", None)
        ssl_properties = body_params.get("SslProperties", None)
        tags = body_params.get("Tags", None)
        folder_arns = body_params.get("FolderArns", None)
        data_source = self.quicksight_backend.create_data_source(
            data_source_id=data_source_id,
            name=name,
            account_id=account_id,
            ds_type=ds_type,
            data_source_parameters=data_source_parameters,
            credentials=credentials,
            permissions=permissions,
            vpc_connection_properties=vpc_connection_properties,
            ssl_properties=ssl_properties,
            tags=tags,
            folder_arns=folder_arns,
        )
        return json.dumps(
            {
                "Arn": data_source.arn,
                "DataSourceId": data_source.data_source_id,
                "CreationStatus": data_source.status,
            }
        )

    def describe_data_source(self) -> str:
        account_id = self._get_param("AwsAccountId")
        data_source_id = self._get_param("DataSourceId")
        data_source = self.quicksight_backend.describe_data_source(
            account_id=account_id, data_source_id=data_source_id
        )
        return json.dumps(dict(DataSource=data_source.to_json()))
