from enum import Enum
from typing import Optional, Any, List, TypeVar, Generic

from pydantic import BaseModel, Field

T = TypeVar('T')


class BaseEventModel(BaseModel):
    class Config:
        allow_population_by_field_name = True
        allow_mutation = False


class MapEventDto(BaseEventModel):
    id: str
    name: str


class TaggedNodeType(BaseEventModel):
    id: str
    name: str
    icon: Optional[str] = None


class TaggedNode(BaseEventModel):
    id: str
    title: str
    map: MapEventDto
    node_type: Optional[TaggedNodeType] = None
    parent_title: Optional[str] = None
    color: Optional[str] = None


class CmdBufferCommandType(str, Enum):
    copy = "copy"
    cut = "cut"


class CmdBufferCommandMetaDto(BaseEventModel):
    map: MapEventDto
    titles: List[str]


# todo move to rf_api_client
class CmdBufferCommandDto(BaseEventModel):
    id: str
    type: CmdBufferCommandType
    nodes: List[str]
    branch: bool
    oneshot: bool
    meta: CmdBufferCommandMetaDto


class EventData(BaseEventModel):
    pass


class NodeTaggedData(EventData):
    node: TaggedNode
    order: int
    tag_id: str


class SearchQuerySavedData(EventData):
    id: str
    title: str
    query: str
    timestamp: int
    user_id: Optional[str] = None


class CmdBufferPushedData(EventData):
    cmd: CmdBufferCommandDto
    position: int


class EventType(Enum):
    node_updated = "node_updated"
    node_type_updated = "node_type_updated"
    node_created = "node_created"
    node_deleted = "node_deleted"
    node_tagged = "node_tagged"
    node_untagged = "node_untagged"
    node_moved = "node_moved"
    branch_deleted = "branch_deleted"
    branch_moved = "branch_moved"
    comment_pushed = "comment_pushed"
    comment_updated = "comment_updated"
    comment_deleted = "comment_deleted"
    comment_all_read = "comment_all_read"
    dialog_show = "dialog_show"
    dialog_result = "dialog_result"
    notification_show = "notification_show"
    url_show = "url_show"
    search_query_saved = "search_query_saved"
    search_query_deleted = "search_query_deleted"
    command_pushed = "command_pushed"
    command_deleted = "command_deleted"
    node_copied = "node_copied"
    branch_copied = "branch_copied"
    branch_access_denied = "branch_access_denied"
    node_access_denied = "node_access_denied"
    branch_access_granted = "branch_access_granted"
    node_access_granted = "node_access_granted"
    branch_subscription_granted = "branch_subscription_granted"
    branch_subscription_denied = "branch_subscription_denied"
    node_subscription_granted = "node_subscription_granted"
    node_subscription_denied = "node_subscription_denied"


# noinspection PyUnusedLocal
class EventVisitor(Generic[T]):
    def __init__(self, default_result: T):
        self._default_result = default_result

    def node_updated(self, event: 'NodeUpdatedMapEvent'):
        return self._default_result

    def node_type_updated(self, event: 'NodeTypeUpdatedMapEvent'):
        return self._default_result

    def node_created(self, event: 'NodeCreatedMapEvent'):
        return self._default_result

    def node_deleted(self, event: 'NodeDeletedMapEvent'):
        return self._default_result

    def node_tagged(self, event: 'NodeTaggedMapEvent'):
        return self._default_result

    def node_untagged(self, event: 'NodeUntaggedMapEvent'):
        return self._default_result

    def node_moved(self, event: 'NodeMovedMapEvent'):
        return self._default_result

    def branch_deleted(self, event: 'BranchDeletedMapEvent'):
        return self._default_result

    def branch_moved(self, event: 'BranchMovedMapEvent'):
        return self._default_result

    def comment_pushed(self, event: 'CommentPushedMapEvent'):
        return self._default_result

    def comment_updated(self, event: 'CommentUpdatedMapEvent'):
        return self._default_result

    def comment_deleted(self, event: 'CommentDeletedMapEvent'):
        return self._default_result

    def comment_all_read(self, event: 'CommentAllReadMapEvent'):
        return self._default_result

    def dialog_show(self, event: 'DialogShowMapEvent'):
        return self._default_result

    def dialog_result(self, event: 'DialogResultMapEvent'):
        return self._default_result

    def notification_show(self, event: 'NotificationShowMapEvent'):
        return self._default_result

    def url_show(self, event: 'UrlShowMapEvent'):
        return self._default_result

    def search_query_saved(self, event: 'SearchQuerySavedMapEvent'):
        return self._default_result

    def search_query_deleted(self, event: 'SearchQueryDeletedMapEvent'):
        return self._default_result

    def command_pushed(self, event: 'CommandPushedMapEvent'):
        return self._default_result

    def command_deleted(self, event: 'CommandDeletedMapEvent'):
        return self._default_result

    def node_copied(self, event: 'NodeCopiedMapEvent'):
        return self._default_result

    def branch_copied(self, event: 'BranchCopiedMapEvent'):
        return self._default_result

    def branch_access_denied(self, event: 'BranchAccessDeniedMapEvent'):
        return self._default_result

    def node_access_denied(self, event: 'NodeAccessDeniedMapEvent'):
        return self._default_result

    def branch_access_granted(self, event: 'BranchAccessGrantedMapEvent'):
        return self._default_result

    def node_access_granted(self, event: 'NodeAccessGrantedMapEvent'):
        return self._default_result

    def branch_subscription_granted(self, event: 'BranchSubscriptionGrantedMapEvent'):
        return self._default_result

    def branch_subscription_denied(self, event: 'BranchSubscriptionDeniedMapEvent'):
        return self._default_result

    def node_subscription_granted(self, event: 'NodeSubscriptionGrantedMapEvent'):
        return self._default_result

    def node_subscription_denied(self, event: 'NodeSubscriptionDeniedMapEvent'):
        return self._default_result


class BaseMapEvent(BaseEventModel):
    type: EventType
    what: str
    session_id: Optional[str] = Field(alias='sessionId', default=None)


class AnyMapEvent(BaseMapEvent):
    data: Optional[Any] = None


class TypedMapEvent(BaseMapEvent):
    async def visit(self, visitor: EventVisitor[T]) -> T:
        raise NotImplementedError()


class NodeUpdatedMapEvent(TypedMapEvent):
    async def visit(self, visitor: EventVisitor[T]) -> T:
        return visitor.node_updated(self)


class NodeTypeUpdatedMapEvent(TypedMapEvent):
    async def visit(self, visitor: EventVisitor[T]) -> T:
        return visitor.node_type_updated(self)


class NodeCreatedMapEvent(TypedMapEvent):
    async def visit(self, visitor: EventVisitor[T]) -> T:
        return visitor.node_created(self)


class NodeDeletedMapEvent(TypedMapEvent):
    async def visit(self, visitor: EventVisitor[T]) -> T:
        return visitor.node_deleted(self)


class NodeTaggedMapEvent(TypedMapEvent):
    data: NodeTaggedData

    async def visit(self, visitor: EventVisitor[T]) -> T:
        return visitor.node_tagged(self)


class NodeUntaggedMapEvent(TypedMapEvent):
    async def visit(self, visitor: EventVisitor[T]) -> T:
        return visitor.node_untagged(self)


class NodeMovedMapEvent(TypedMapEvent):
    async def visit(self, visitor: EventVisitor[T]) -> T:
        return visitor.node_moved(self)


class BranchDeletedMapEvent(TypedMapEvent):
    async def visit(self, visitor: EventVisitor[T]) -> T:
        return visitor.branch_deleted(self)


class BranchMovedMapEvent(TypedMapEvent):
    async def visit(self, visitor: EventVisitor[T]) -> T:
        return visitor.branch_moved(self)


class CommentPushedMapEvent(TypedMapEvent):
    async def visit(self, visitor: EventVisitor[T]) -> T:
        return visitor.comment_pushed(self)


class CommentUpdatedMapEvent(TypedMapEvent):
    async def visit(self, visitor: EventVisitor[T]) -> T:
        return visitor.comment_updated(self)


class CommentDeletedMapEvent(TypedMapEvent):
    async def visit(self, visitor: EventVisitor[T]) -> T:
        return visitor.comment_deleted(self)


class CommentAllReadMapEvent(TypedMapEvent):
    async def visit(self, visitor: EventVisitor[T]) -> T:
        return visitor.comment_all_read(self)


class DialogShowMapEvent(TypedMapEvent):
    async def visit(self, visitor: EventVisitor[T]) -> T:
        return visitor.dialog_show(self)


class DialogResultMapEvent(TypedMapEvent):
    async def visit(self, visitor: EventVisitor[T]) -> T:
        return visitor.dialog_result(self)


class NotificationShowMapEvent(TypedMapEvent):
    async def visit(self, visitor: EventVisitor[T]) -> T:
        return visitor.notification_show(self)


class UrlShowMapEvent(TypedMapEvent):
    async def visit(self, visitor: EventVisitor[T]) -> T:
        return visitor.url_show(self)


class SearchQuerySavedMapEvent(TypedMapEvent):
    data: SearchQuerySavedData

    async def visit(self, visitor: EventVisitor[T]) -> T:
        return visitor.search_query_saved(self)


class SearchQueryDeletedMapEvent(TypedMapEvent):
    async def visit(self, visitor: EventVisitor[T]) -> T:
        return visitor.search_query_deleted(self)


class CommandPushedMapEvent(TypedMapEvent):
    data: CmdBufferPushedData

    async def visit(self, visitor: EventVisitor[T]) -> T:
        return visitor.command_pushed(self)


class CommandDeletedMapEvent(TypedMapEvent):
    async def visit(self, visitor: EventVisitor[T]) -> T:
        return visitor.command_deleted(self)


class NodeCopiedMapEvent(TypedMapEvent):
    async def visit(self, visitor: EventVisitor[T]) -> T:
        return visitor.node_copied(self)


class BranchCopiedMapEvent(TypedMapEvent):
    async def visit(self, visitor: EventVisitor[T]) -> T:
        return visitor.branch_copied(self)


class BranchAccessDeniedMapEvent(TypedMapEvent):
    async def visit(self, visitor: EventVisitor[T]) -> T:
        return visitor.branch_access_denied(self)


class NodeAccessDeniedMapEvent(TypedMapEvent):
    async def visit(self, visitor: EventVisitor[T]) -> T:
        return visitor.node_access_denied(self)


class BranchAccessGrantedMapEvent(TypedMapEvent):
    async def visit(self, visitor: EventVisitor[T]) -> T:
        return visitor.branch_access_granted(self)


class NodeAccessGrantedMapEvent(TypedMapEvent):
    async def visit(self, visitor: EventVisitor[T]) -> T:
        return visitor.node_access_granted(self)


class BranchSubscriptionGrantedMapEvent(TypedMapEvent):
    async def visit(self, visitor: EventVisitor[T]) -> T:
        return visitor.branch_subscription_granted(self)


class BranchSubscriptionDeniedMapEvent(TypedMapEvent):
    async def visit(self, visitor: EventVisitor[T]) -> T:
        return visitor.branch_subscription_denied(self)


class NodeSubscriptionGrantedMapEvent(TypedMapEvent):
    async def visit(self, visitor: EventVisitor[T]) -> T:
        return visitor.node_subscription_granted(self)


class NodeSubscriptionDeniedMapEvent(TypedMapEvent):
    async def visit(self, visitor: EventVisitor[T]) -> T:
        return visitor.node_subscription_denied(self)


class CompoundMapEvent(AnyMapEvent):
    additional: Optional[List[AnyMapEvent]] = None


event_type_to_typed_event = {
    EventType.node_updated: NodeUpdatedMapEvent,
    EventType.node_type_updated: NodeTypeUpdatedMapEvent,
    EventType.node_created: NodeCreatedMapEvent,
    EventType.node_deleted: NodeDeletedMapEvent,
    EventType.node_tagged: NodeTaggedMapEvent,
    EventType.node_untagged: NodeUntaggedMapEvent,
    EventType.node_moved: NodeMovedMapEvent,
    EventType.branch_deleted: BranchDeletedMapEvent,
    EventType.branch_moved: BranchMovedMapEvent,
    EventType.comment_pushed: CommentPushedMapEvent,
    EventType.comment_updated: CommentUpdatedMapEvent,
    EventType.comment_deleted: CommentDeletedMapEvent,
    EventType.comment_all_read: CommentAllReadMapEvent,
    EventType.dialog_show: DialogShowMapEvent,
    EventType.dialog_result: DialogResultMapEvent,
    EventType.notification_show: NotificationShowMapEvent,
    EventType.url_show: UrlShowMapEvent,
    EventType.search_query_saved: SearchQuerySavedMapEvent,
    EventType.search_query_deleted: SearchQueryDeletedMapEvent,
    EventType.command_pushed: CommandPushedMapEvent,
    EventType.command_deleted: CommandDeletedMapEvent,
    EventType.node_copied: NodeCopiedMapEvent,
    EventType.branch_copied: BranchCopiedMapEvent,
    EventType.branch_access_denied: BranchAccessDeniedMapEvent,
    EventType.node_access_denied: NodeAccessDeniedMapEvent,
    EventType.branch_access_granted: BranchAccessGrantedMapEvent,
    EventType.node_access_granted: NodeAccessGrantedMapEvent,
    EventType.branch_subscription_granted: BranchSubscriptionGrantedMapEvent,
    EventType.branch_subscription_denied: BranchSubscriptionDeniedMapEvent,
    EventType.node_subscription_granted: NodeSubscriptionGrantedMapEvent,
    EventType.node_subscription_denied: NodeSubscriptionDeniedMapEvent,
}


def any_event_to_typed(event: AnyMapEvent) -> TypedMapEvent:
    typed_event = event_type_to_typed_event[event.type]
    return typed_event(**event.dict())


def parse_compound_event(json: dict) -> List[TypedMapEvent]:
    compound_event = CompoundMapEvent(**json)
    additional = compound_event.additional or []

    return [
        any_event_to_typed(compound_event),
        *(any_event_to_typed(e) for e in additional),
    ]
