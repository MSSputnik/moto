import re
from typing import Any, Dict, List

from moto.core.base_backend import BackendDict, BaseBackend
from moto.utilities.paginator import paginate

from .data_models import (
    QuicksightDataSet,
    QuicksightFolder,
    QuicksightGroup,
    QuicksightIngestion,
    QuicksightMembership,
    QuicksightPermission,
    QuicksightUser,
)
from .exceptions import (
    InvalidParameterValueException,
    ResourceNotFoundException,
    ValidationException,
)
from .utils import PAGINATION_MODEL, QuicksightSearchFilterFactory


def _create_id(aws_account_id: str, namespace: str, _id: str) -> str:
    return f"{aws_account_id}:{namespace}:{_id}"


class QuickSightBackend(BaseBackend):
    """Implementation of QuickSight APIs."""

    def __init__(self, region_name: str, account_id: str):
        super().__init__(region_name, account_id)
        self.groups: Dict[str, QuicksightGroup] = dict()
        self.users: Dict[str, QuicksightUser] = dict()
        self.folder: Dict[str, QuicksightFolder] = dict()

    def is_valid_principal_arn(
        self, account_id: str, namespace: str = "", arn: str = ""
    ) -> bool:
        id_for_ns = f"{account_id}:"
        group_found = any(
            [
                group
                for _id, group in self.groups.items()
                if _id.startswith(id_for_ns) and group.arn == arn
            ]
        )
        if group_found:
            return True
        return any(
            [
                user
                for _id, user in self.users.items()
                if _id.startswith(id_for_ns) and user.arn == arn
            ]
        )

    def create_data_set(self, data_set_id: str, name: str) -> QuicksightDataSet:
        return QuicksightDataSet(
            self.account_id, self.region_name, data_set_id, name=name
        )

    def create_group(
        self, group_name: str, description: str, aws_account_id: str, namespace: str
    ) -> QuicksightGroup:
        group = QuicksightGroup(
            region=self.region_name,
            group_name=group_name,
            description=description,
            aws_account_id=aws_account_id,
            namespace=namespace,
        )
        _id = _create_id(aws_account_id, namespace, group_name)
        self.groups[_id] = group
        return group

    def create_group_membership(
        self, aws_account_id: str, namespace: str, group_name: str, member_name: str
    ) -> QuicksightMembership:
        group = self.describe_group(aws_account_id, namespace, group_name)
        return group.add_member(member_name)

    def create_ingestion(
        self, data_set_id: str, ingestion_id: str
    ) -> QuicksightIngestion:
        return QuicksightIngestion(
            self.account_id, self.region_name, data_set_id, ingestion_id
        )

    def delete_group(
        self, aws_account_id: str, namespace: str, group_name: str
    ) -> None:
        _id = _create_id(aws_account_id, namespace, group_name)
        self.groups.pop(_id, None)

    def delete_user(self, aws_account_id: str, namespace: str, user_name: str) -> None:
        # Delete users from all groups
        for group in self.groups.values():
            group.delete_member(user_name)
        # Delete user itself
        _id = _create_id(aws_account_id, namespace, user_name)
        self.users.pop(_id, None)

    def describe_group(
        self, aws_account_id: str, namespace: str, group_name: str
    ) -> QuicksightGroup:
        _id = _create_id(aws_account_id, namespace, group_name)
        if _id not in self.groups:
            raise ResourceNotFoundException(f"Group {group_name} not found")
        return self.groups[_id]

    def describe_group_membership(
        self, aws_account_id: str, namespace: str, group_name: str, member_name: str
    ) -> QuicksightMembership:
        group = self.describe_group(aws_account_id, namespace, group_name)
        member = group.get_member(member_name)
        if member is None:
            raise ResourceNotFoundException(f"Member {member_name} not found")
        return member

    def describe_user(
        self, aws_account_id: str, namespace: str, user_name: str
    ) -> QuicksightUser:
        _id = _create_id(aws_account_id, namespace, user_name)
        if _id not in self.users:
            raise ResourceNotFoundException(f"User {user_name} not found")
        return self.users[_id]

    @paginate(pagination_model=PAGINATION_MODEL)
    def list_groups(self, aws_account_id: str, namespace: str) -> List[QuicksightGroup]:
        id_for_ns = _create_id(aws_account_id, namespace, _id="")
        return [
            group for _id, group in self.groups.items() if _id.startswith(id_for_ns)
        ]

    @paginate(pagination_model=PAGINATION_MODEL)
    def search_groups(
        self, aws_account_id: str, namespace: str, filters: List[Dict[str, str]]
    ) -> List[QuicksightGroup]:
        id_for_ns = _create_id(aws_account_id, namespace, _id="")
        filter_list = QuicksightSearchFilterFactory.validate_and_create_filter(
            model_type=QuicksightGroup, input=filters
        )
        return [
            group
            for _id, group in self.groups.items()
            if _id.startswith(id_for_ns) and filter_list.match(group)
        ]

    @paginate(pagination_model=PAGINATION_MODEL)
    def list_group_memberships(
        self, aws_account_id: str, namespace: str, group_name: str
    ) -> List[QuicksightMembership]:
        group = self.describe_group(aws_account_id, namespace, group_name)
        return group.list_members()

    @paginate(pagination_model=PAGINATION_MODEL)
    def list_users(self, aws_account_id: str, namespace: str) -> List[QuicksightUser]:
        id_for_ns = _create_id(aws_account_id, namespace, _id="")
        return [user for _id, user in self.users.items() if _id.startswith(id_for_ns)]

    @paginate(pagination_model=PAGINATION_MODEL)
    def list_user_groups(
        self, aws_account_id: str, namespace: str, user_name: str
    ) -> List[QuicksightGroup]:
        id_for_ns = _create_id(aws_account_id, namespace, _id="")
        group_list: Dict[str, QuicksightGroup] = {}
        # Loop through all groups and check if the user is member.
        for id, group in self.groups.items():
            if group.get_member(user_name):
                group_list[id] = group
        return [group for _id, group in group_list.items() if _id.startswith(id_for_ns)]

    def register_user(
        self,
        identity_type: str,
        email: str,
        user_role: str,
        aws_account_id: str,
        namespace: str,
        user_name: str,
    ) -> QuicksightUser:
        """
        The following parameters are not yet implemented:
        IamArn, SessionName, CustomsPermissionsName, ExternalLoginFederationProviderType, CustomFederationProviderUrl, ExternalLoginId
        """
        user = QuicksightUser(
            account_id=aws_account_id,
            region=self.region_name,
            email=email,
            identity_type=identity_type,
            user_role=user_role,
            username=user_name,
        )
        _id = _create_id(aws_account_id, namespace, user_name)
        self.users[_id] = user
        return user

    def update_user(
        self,
        aws_account_id: str,
        namespace: str,
        user_name: str,
        email: str,
        user_role: str,
    ) -> QuicksightUser:
        user = self.describe_user(aws_account_id, namespace, user_name)
        user.email = email
        user.user_role = user_role
        return user

    def update_group(
        self, aws_account_id: str, namespace: str, group_name: str, description: str
    ) -> QuicksightGroup:
        group = self.describe_group(aws_account_id, namespace, group_name)
        group.description = description
        return group

    def create_folder(
        self,
        account_id: str,
        region: str,
        folder_id: str,
        name: str,
        folder_type: str,
        parent_folder_arn: str,
        permissions: List[Dict[str, Any]],
        tags: List[Dict[str, str]],
        sharing_model: str,
    ) -> QuicksightFolder:
        # Test valid folder_id
        validation_errors: List[str] = []
        if not re.match(r"^[\w\-]+$", folder_id):
            validation_errors.append(
                f"Value '{folder_id}' at 'folderId' failed to satisfy constraint: Member must satisfy regular expression pattern: [\\w\\-]+"
            )

        if len(validation_errors) > 0:
            raise ValidationException(
                f"{len(validation_errors)} validation error detected: {validation_errors[0]}"
            )
        if not name:
            raise InvalidParameterValueException("No folder name found")
        folder_permissions: List[QuicksightPermission] = list()
        if isinstance(permissions, list):
            for permission in permissions:
                if isinstance(permission, dict):
                    new_permission = QuicksightPermission(permission=permission)
                    if self.is_valid_principal_arn(
                        account_id=account_id, arn=new_permission.principal
                    ) and new_permission.validate_permission_set("FOLDER"):
                        folder_permissions.append(new_permission)
                    else:
                        raise InvalidParameterValueException(
                            f"Principal ARN {new_permission.principal} is not part of the same account {account_id}"
                        )

        folder = QuicksightFolder(
            account_id=account_id,
            region=region,
            folder_id=folder_id,
            name=name,
            folder_type=folder_type,
            parent_folder_arn=parent_folder_arn,
            permissions=folder_permissions,
            tags=tags,
            sharing_model=sharing_model,
        )
        _id = _create_id(account_id, "", folder_id)
        self.folder[_id] = folder
        return folder

    def describe_folder(self, account_id: str, folder_id: str) -> QuicksightFolder:
        validation_errors: List[str] = []
        if not re.match(r"^[\w\-]+$", folder_id):
            validation_errors.append(
                f"Value '{folder_id}' at 'folderId' failed to satisfy constraint: Member must satisfy regular expression pattern: [\\w\\-]+"
            )

        if len(validation_errors) > 0:
            raise ValidationException(
                f"{len(validation_errors)} validation error detected: {validation_errors[0]}"
            )
        _id = _create_id(account_id, "", folder_id)
        if _id not in self.folder:
            raise ResourceNotFoundException(f"Folder {folder_id} not found")
        return self.folder[_id]


quicksight_backends = BackendDict(QuickSightBackend, "quicksight")
