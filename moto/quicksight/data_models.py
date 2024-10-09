from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Union

from moto.core.common_models import BaseModel
from moto.moto_api._internal import mock_random as random
from moto.utilities.tagging_service import TaggingService
from moto.utilities.utils import get_partition

from .exceptions import InvalidParameterValueException


class QuicksightDataSet(BaseModel):
    def __init__(self, account_id: str, region: str, _id: str, name: str):
        self.arn = f"arn:{get_partition(region)}:quicksight:{region}:{account_id}:data-set/{_id}"
        self._id = _id
        self.name = name
        self.region = region
        self.account_id = account_id

    def to_json(self) -> Dict[str, Any]:
        return {
            "Arn": self.arn,
            "DataSetId": self._id,
            "IngestionArn": f"arn:{get_partition(self.region)}:quicksight:{self.region}:{self.account_id}:ingestion/tbd",
        }


class QuicksightIngestion(BaseModel):
    def __init__(
        self, account_id: str, region: str, data_set_id: str, ingestion_id: str
    ):
        self.arn = f"arn:{get_partition(region)}:quicksight:{region}:{account_id}:data-set/{data_set_id}/ingestions/{ingestion_id}"
        self.ingestion_id = ingestion_id

    def to_json(self) -> Dict[str, Any]:
        return {
            "Arn": self.arn,
            "IngestionId": self.ingestion_id,
            "IngestionStatus": "INITIALIZED",
        }


class QuicksightMembership(BaseModel):
    def __init__(self, account_id: str, region: str, group: str, user: str):
        self.group = group
        self.user = user
        self.arn = f"arn:{get_partition(region)}:quicksight:{region}:{account_id}:group/default/{group}/{user}"

    def to_json(self) -> Dict[str, str]:
        return {"Arn": self.arn, "MemberName": self.user}


class QuicksightGroup(BaseModel):
    def __init__(
        self,
        region: str,
        group_name: str,
        description: str,
        aws_account_id: str,
        namespace: str,
    ):
        self.arn = f"arn:{get_partition(region)}:quicksight:{region}:{aws_account_id}:group/default/{group_name}"
        self.group_name = group_name
        self.description = description
        self.aws_account_id = aws_account_id
        self.namespace = namespace
        self.region = region

        self.members: Dict[str, QuicksightMembership] = dict()

    def add_member(self, member_name: str) -> QuicksightMembership:
        membership = QuicksightMembership(
            self.aws_account_id, self.region, self.group_name, member_name
        )
        self.members[member_name] = membership
        return membership

    def delete_member(self, user_name: str) -> None:
        self.members.pop(user_name, None)

    def get_member(self, user_name: str) -> Union[QuicksightMembership, None]:
        return self.members.get(user_name, None)

    def list_members(self) -> List[QuicksightMembership]:
        return list(self.members.values())

    def to_json(self) -> Dict[str, Any]:
        return {
            "Arn": self.arn,
            "GroupName": self.group_name,
            "Description": self.description,
            "PrincipalId": self.aws_account_id,
            "Namespace": self.namespace,
        }


class QuicksightUser(BaseModel):
    def __init__(
        self,
        account_id: str,
        region: str,
        email: str,
        identity_type: str,
        username: str,
        user_role: str,
    ):
        self.arn = f"arn:{get_partition(region)}:quicksight:{region}:{account_id}:user/default/{username}"
        self.email = email
        self.identity_type = identity_type
        self.username = username
        self.user_role = user_role
        self.active = False
        self.principal_id = random.get_random_hex(10)

    def to_json(self) -> Dict[str, Any]:
        return {
            "Arn": self.arn,
            "Email": self.email,
            "IdentityType": self.identity_type,
            "Role": self.user_role,
            "UserName": self.username,
            "Active": self.active,
            "PrincipalId": self.principal_id,
        }


class QuicksightPermission(BaseModel):
    folder_viewer_actions = ["quicksight:DescribeFolder"]
    folder_author_actions = [
        "quicksight:CreateFolder",
        "quicksight:DescribeFolder",
        "quicksight:CreateFolderMembership",
        "quicksight:DeleteFolderMembership",
        "quicksight:DescribeFolderPermissions",
    ]
    folder_owner_actions = [
        "quicksight:CreateFolder",
        "quicksight:DescribeFolder",
        "quicksight:UpdateFolder",
        "quicksight:DeleteFolder",
        "quicksight:CreateFolderMembership",
        "quicksight:DeleteFolderMembership",
        "quicksight:DescribeFolderPermissions",
        "quicksight:UpdateFolderPermissions",
    ]

    def __init__(
        self,
        *,
        prinicpal: Union[str, None] = None,
        actions: Union[List[str], None] = None,
        permission: Union[Dict[str, Any], None] = None,
    ):
        if not permission:
            self.principal: str = prinicpal if prinicpal else ""
            self.actions: List[str] = actions if actions else list()
        else:
            self.principal = permission.get("Principal", "")
            self.actions = permission.get("Actions", list())

    def validate_permission_set(self, resource: Literal["FOLDER"]) -> bool:
        valid = False
        if resource == "FOLDER":
            for check_list in [
                self.folder_viewer_actions,
                self.folder_author_actions,
                self.folder_owner_actions,
            ]:
                sorted_set = sorted(check_list)
                sorted_actions = sorted(self.actions)
                if sorted_set == sorted_actions:
                    valid = True
                    break
            if not valid:
                raise InvalidParameterValueException(
                    f"ResourcePermission list contains unsupported permission sets {self.actions} for this resource. Valid sets : {self.folder_viewer_actions} or {self.folder_author_actions} or {self.folder_owner_actions}"
                )
            return True
        raise InvalidParameterValueException(
            f"MOTO: Unknown Permission Set for {resource}"
        )

    def to_json(self) -> Dict[str, Any]:
        return {"Principal": self.principal, "Actions": self.actions}


class QuicksightFolder(BaseModel):
    def __init__(
        self,
        account_id: str,
        region: str,
        folder_id: str,
        name: str,
        folder_type: str,
        parent_folder_arn: str,
        permissions: List[QuicksightPermission],
        tags: List[Dict[str, str]],
        sharing_model: str,
    ):
        self.tagger = TaggingService()
        self.arn = f"arn:{get_partition(region)}:quicksight:{region}:{account_id}:folder/{folder_id}"
        self.account_id = account_id
        self.region = region
        self.folder_id = folder_id
        self.name = name
        self.folder_type = folder_type
        self.parent_folder_arn = parent_folder_arn
        if tags:
            self.tagger.tag_resource(self.arn, tags)
        self.sharing_model = sharing_model
        self.created_time = datetime.now(timezone.utc)
        self.last_updated_time = self.created_time
        self.permissions = permissions

    def get_folder_path(self) -> List[str]:
        """Get Folder Path not yet implemented."""
        return list()

    def to_json(self) -> Dict[str, Any]:
        return {
            "FolderId": self.folder_id,
            "Arn": self.arn,
            "Name": self.name,
            "FolderType": self.folder_type,
            "FolderPath": self.get_folder_path(),
            "CreatedTime": self.created_time.isoformat(),
            "LastUpdatedTime": self.last_updated_time.isoformat(),
            "SharingModel": self.sharing_model,
        }

    def permissions_to_json(self) -> Dict[str, Any]:
        if self.permissions:
            return [permission.to_json() for permission in self.permissions]
        return []
