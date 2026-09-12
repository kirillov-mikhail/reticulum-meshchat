from datetime import datetime, timezone

from peewee import (
    BigAutoField,
    BooleanField,
    CharField,
    DatabaseProxy,
    DateTimeField,
    FloatField,
    IntegerField,
    Model,
    TextField,
)
from playhouse.migrate import SqliteMigrator
from playhouse.migrate import migrate as migrate_database

__all__ = [
    "latest_version",
    "database",
    "migrate",
    "BaseModel",
    "Config",
    "Announce",
    "CustomDestinationDisplayName",
    "FavouriteDestination",
    "LxmfMessage",
    "LxmfConversationReadState",
    "LxmfUserIcon",
]

latest_version = 5
database = DatabaseProxy()
migrator = None


def _table_columns(db, table_name):
    try:
        cursor = db.execute_sql(f'PRAGMA table_info("{table_name}")')
        return {row[1] for row in cursor.fetchall()}
    except Exception:
        return set()


def migrate(current_version):
    global migrator
    if current_version is None:
        current_version = 1
    try:
        current_version = int(current_version)
    except (TypeError, ValueError):
        current_version = 1
    if current_version >= latest_version:
        return latest_version
    if migrator is None:
        migrator = SqliteMigrator(database)

    def _add_column_safe(table_name, column_name, field):
        if column_name in _table_columns(database, table_name):
            return
        try:
            migrate_database(migrator.add_column(table_name, column_name, field))
        except Exception:
            if column_name not in _table_columns(database, table_name):
                raise

    if current_version < 2:
        _add_column_safe("lxmf_messages", "delivery_attempts", LxmfMessage.delivery_attempts)
        _add_column_safe("lxmf_messages", "next_delivery_attempt_at", LxmfMessage.next_delivery_attempt_at)

    if current_version < 3:
        _add_column_safe("lxmf_messages", "rssi", LxmfMessage.rssi)
        _add_column_safe("lxmf_messages", "snr", LxmfMessage.snr)
        _add_column_safe("lxmf_messages", "quality", LxmfMessage.quality)

    if current_version < 4:
        _add_column_safe("lxmf_messages", "method", LxmfMessage.method)

    if current_version < 5:
        _add_column_safe("announces", "rssi", Announce.rssi)
        _add_column_safe("announces", "snr", Announce.snr)
        _add_column_safe("announces", "quality", Announce.quality)

    return latest_version

class BaseModel(Model):
    class Meta:
        database = database

class Config(BaseModel):

    id = BigAutoField()
    key = CharField(unique=True)
    value = TextField()
    created_at = DateTimeField(default=lambda: datetime.now(timezone.utc))
    updated_at = DateTimeField(default=lambda: datetime.now(timezone.utc))

    class Meta:
        table_name = "config"

class Announce(BaseModel):

    id = BigAutoField()
    destination_hash = CharField(unique=True)
    aspect = TextField(index=True)
    identity_hash = CharField(index=True)
    identity_public_key = CharField()
    app_data = TextField(null=True)
    rssi = IntegerField(null=True)
    snr = FloatField(null=True)
    quality = FloatField(null=True)

    created_at = DateTimeField(default=lambda: datetime.now(timezone.utc))
    updated_at = DateTimeField(default=lambda: datetime.now(timezone.utc))

    class Meta:
        table_name = "announces"

class CustomDestinationDisplayName(BaseModel):

    id = BigAutoField()
    destination_hash = CharField(unique=True)
    display_name = CharField()

    created_at = DateTimeField(default=lambda: datetime.now(timezone.utc))
    updated_at = DateTimeField(default=lambda: datetime.now(timezone.utc))

    class Meta:
        table_name = "custom_destination_display_names"

class FavouriteDestination(BaseModel):

    id = BigAutoField()
    destination_hash = CharField(unique=True)
    display_name = CharField()
    aspect = CharField()

    created_at = DateTimeField(default=lambda: datetime.now(timezone.utc))
    updated_at = DateTimeField(default=lambda: datetime.now(timezone.utc))

    class Meta:
        table_name = "favourite_destinations"

class LxmfMessage(BaseModel):

    id = BigAutoField()
    hash = CharField(unique=True)
    source_hash = CharField(index=True)
    destination_hash = CharField(index=True)
    state = CharField()
    progress = FloatField()
    is_incoming = BooleanField()
    method = CharField(null=True)
    delivery_attempts = IntegerField(default=0)
    next_delivery_attempt_at = FloatField(null=True)
    title = TextField()
    content = TextField()
    fields = TextField()
    timestamp = FloatField()
    rssi = IntegerField(null=True)
    snr = FloatField(null=True)
    quality = FloatField(null=True)
    created_at = DateTimeField(default=lambda: datetime.now(timezone.utc))
    updated_at = DateTimeField(default=lambda: datetime.now(timezone.utc))

    class Meta:
        table_name = "lxmf_messages"

class LxmfConversationReadState(BaseModel):

    id = BigAutoField()
    destination_hash = CharField(unique=True)
    last_read_at = DateTimeField()

    created_at = DateTimeField(default=lambda: datetime.now(timezone.utc))
    updated_at = DateTimeField(default=lambda: datetime.now(timezone.utc))

    class Meta:
        table_name = "lxmf_conversation_read_state"

class LxmfUserIcon(BaseModel):

    id = BigAutoField()
    destination_hash = CharField(unique=True)
    icon_name = CharField()
    foreground_colour = CharField()
    background_colour = CharField()

    created_at = DateTimeField(default=lambda: datetime.now(timezone.utc))
    updated_at = DateTimeField(default=lambda: datetime.now(timezone.utc))

    class Meta:
        table_name = "lxmf_user_icons"
