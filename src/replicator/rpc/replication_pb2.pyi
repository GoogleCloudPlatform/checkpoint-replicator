from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class RegisterCoordinatorRequest(_message.Message):
    __slots__ = ("job_name", "ip")
    JOB_NAME_FIELD_NUMBER: _ClassVar[int]
    IP_FIELD_NUMBER: _ClassVar[int]
    job_name: str
    ip: str
    def __init__(self, job_name: _Optional[str] = ..., ip: _Optional[str] = ...) -> None: ...

class RegisterCoordinatorResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetCoordinatorRequest(_message.Message):
    __slots__ = ("job_name",)
    JOB_NAME_FIELD_NUMBER: _ClassVar[int]
    job_name: str
    def __init__(self, job_name: _Optional[str] = ...) -> None: ...

class GetCoordinatorResponse(_message.Message):
    __slots__ = ("ip",)
    IP_FIELD_NUMBER: _ClassVar[int]
    ip: str
    def __init__(self, ip: _Optional[str] = ...) -> None: ...

class UnregisterCoordinatorRequest(_message.Message):
    __slots__ = ("job_name", "ip")
    JOB_NAME_FIELD_NUMBER: _ClassVar[int]
    IP_FIELD_NUMBER: _ClassVar[int]
    job_name: str
    ip: str
    def __init__(self, job_name: _Optional[str] = ..., ip: _Optional[str] = ...) -> None: ...

class UnregisterCoordinatorResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class SetReplicationPeerRequest(_message.Message):
    __slots__ = ("local_mountpoint", "target_ip")
    LOCAL_MOUNTPOINT_FIELD_NUMBER: _ClassVar[int]
    TARGET_IP_FIELD_NUMBER: _ClassVar[int]
    local_mountpoint: str
    target_ip: str
    def __init__(self, local_mountpoint: _Optional[str] = ..., target_ip: _Optional[str] = ...) -> None: ...

class SetReplicationPeerResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class MountGCSBucketRequest(_message.Message):
    __slots__ = ("local_mountpoint",)
    LOCAL_MOUNTPOINT_FIELD_NUMBER: _ClassVar[int]
    local_mountpoint: str
    def __init__(self, local_mountpoint: _Optional[str] = ...) -> None: ...

class MountGCSBucketResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class UnmountPeerRequest(_message.Message):
    __slots__ = ("local_mountpoint",)
    LOCAL_MOUNTPOINT_FIELD_NUMBER: _ClassVar[int]
    local_mountpoint: str
    def __init__(self, local_mountpoint: _Optional[str] = ...) -> None: ...

class UnmountPeerResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class UnmountAllPeersRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class UnmountAllPeersResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
