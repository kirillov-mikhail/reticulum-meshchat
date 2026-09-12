import asyncio
import base64
import json
import os
import re
import stat
import time
from datetime import datetime, timezone
from typing import Optional

import RNS
import LXMF
from peewee import SqliteDatabase

import database

MAX_ATTACHMENT_SIZE = 100 * 1024 * 1024
MAX_DISPLAY_NAME_LENGTH = 64
MAX_MESSAGE_CONTENT_LENGTH = 1024 * 1024
MAX_TITLE_LENGTH = 512
MAX_FILENAME_LENGTH = 255

SEND_DIRECTORIES_ENV_VAR = "RETICULUM_MESHCHAT_SEND_DIRECTORIES"
SEND_DIRECTORIES_SEPARATOR = os.pathsep


class AnnounceHandler:
    def __init__(self, aspect_filter, received_announce_callback):
        self.aspect_filter = aspect_filter
        self.received_announce_callback = received_announce_callback

    def received_announce(self, destination_hash, announced_identity, app_data, announce_packet_hash):
        try:
            self.received_announce_callback(
                self.aspect_filter,
                destination_hash,
                announced_identity,
                app_data,
                announce_packet_hash,
            )
        except Exception as error:
            RNS.log("announce handler error: " + str(error), RNS.LOG_ERROR)


class Config:
    @staticmethod
    def get(key, default_value=None):
        config_item = database.Config.get_or_none(database.Config.key == key)
        if config_item is not None:
            return config_item.value
        return default_value

    @staticmethod
    def set(key, value):
        if value is None:
            database.Config.delete().where(database.Config.key == key).execute()
            return
        data = {"key": key, "value": value, "updated_at": datetime.now(timezone.utc)}
        query = database.Config.insert(data)
        query = query.on_conflict(conflict_target=[database.Config.key], update=data)
        query.execute()

    class StringConfig:
        def __init__(self, key, default_value: Optional[str] = None):
            self.key = key
            self.default_value = default_value

        def get(self, default_value: Optional[str] = None) -> Optional[str]:
            effective_default = default_value if default_value is not None else self.default_value
            value = Config.get(self.key, default_value=effective_default)
            if value is None:
                return None
            value = str(value)
            if len(value) > 4096:
                value = value[:4096]
            return value

        def set(self, value):
            if value is not None:
                value = str(value)
                if len(value) > 4096:
                    raise ValueError("config value too long")
            Config.set(self.key, value)

    class BoolConfig:
        def __init__(self, key, default_value=False):
            self.key = key
            self.default_value = default_value

        def get(self):
            config_value = Config.get(self.key, default_value=None)
            if config_value is None:
                return self.default_value
            return config_value == "true"

        def set(self, value):
            Config.set(self.key, "true" if value else "false")

    class IntConfig:
        def __init__(self, key, default_value: Optional[int] = 0):
            self.key = key
            self.default_value = default_value

        def get(self) -> Optional[int]:
            config_value = Config.get(self.key, default_value=None)
            if config_value is None:
                return self.default_value
            try:
                parsed = int(str(config_value).strip())
            except (TypeError, ValueError):
                return self.default_value
            if parsed < 0:
                return 0
            if parsed > 2**31 - 1:
                return 2**31 - 1
            return parsed

        def set(self, value):
            try:
                value = int(value)
            except (TypeError, ValueError):
                raise ValueError("invalid integer config value")
            if value < 0:
                value = 0
            if value > 2**31 - 1:
                value = 2**31 - 1
            Config.set(self.key, str(value))

    database_version = IntConfig("database_version", None)
    display_name = StringConfig("display_name", "Anonymous Peer")
    auto_announce_enabled = BoolConfig("auto_announce_enabled", False)
    auto_announce_interval_seconds = IntConfig("auto_announce_interval_seconds", 0)
    last_announced_at = IntConfig("last_announced_at", None)
    auto_send_failed_messages_to_propagation_node = BoolConfig("auto_send_failed_messages_to_propagation_node", False)
    lxmf_delivery_transfer_limit_in_bytes = IntConfig("lxmf_delivery_transfer_limit_in_bytes", 1000 * 1000 * 10)
    lxmf_preferred_propagation_node_destination_hash = StringConfig("lxmf_preferred_propagation_node_destination_hash", None)
    lxmf_preferred_propagation_node_auto_sync_interval_seconds = IntConfig("lxmf_preferred_propagation_node_auto_sync_interval_seconds", 0)
    lxmf_preferred_propagation_node_last_synced_at = IntConfig("lxmf_preferred_propagation_node_last_synced_at", None)
    lxmf_local_propagation_node_enabled = BoolConfig("lxmf_local_propagation_node_enabled", False)
    send_directories = StringConfig("send_directories", None)

def path_is_within_send_directory(path, send_directories):
    if path is None:
        return False
    resolved = os.path.realpath(path)
    for send_dir in send_directories:
        try:
            if os.path.commonpath([os.path.realpath(send_dir), resolved]) == os.path.realpath(send_dir):
                return True
        except ValueError:
            continue
    return False


def _validate_destination_hash(destination_hash):
    if destination_hash is None:
        raise ValueError("invalid destination hash")
    normalised = str(destination_hash).strip().lower()
    if not normalised:
        raise ValueError("invalid destination hash")
    try:
        destination_bytes = bytes.fromhex(normalised)
    except (ValueError, TypeError):
        raise ValueError("invalid destination hash")
    if len(destination_bytes) != RNS.Identity.TRUNCATED_HASHLENGTH // 8:
        raise ValueError("invalid destination hash length")
    return normalised


def _validate_display_name(name, default_value="Anonymous Peer"):
    if name is None:
        return default_value
    name = str(name).strip()
    if not name:
        return default_value
    name = "".join(ch for ch in name if ch == "\t" or (ord(ch) >= 32 and ord(ch) != 127))
    name = name.strip()
    if not name:
        return default_value
    if len(name) > MAX_DISPLAY_NAME_LENGTH:
        name = name[:MAX_DISPLAY_NAME_LENGTH]
    return name


def load_or_create_identity(storage_dir):
    os.makedirs(storage_dir, mode=0o700, exist_ok=True)
    try:
        os.chmod(storage_dir, 0o700)
    except OSError as error:
        RNS.log("failed to set permissions on storage directory " + storage_dir + ": " + str(error), RNS.LOG_WARNING)
    identity_file = os.path.join(storage_dir, "identity")
    if not os.path.lexists(identity_file):
        identity = RNS.Identity(create_keys=True)
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(identity_file, flags, 0o600)
        try:
            with os.fdopen(fd, "wb") as file:
                file.write(identity.get_private_key())
        except BaseException:
            try:
                os.unlink(identity_file)
            except OSError as error:
                RNS.log("failed to remove partially written identity file " + identity_file + ": " + str(error), RNS.LOG_ERROR)
            raise
        try:
            os.chmod(identity_file, 0o600)
        except OSError as error:
            RNS.log("failed to set permissions on identity file " + identity_file + ": " + str(error), RNS.LOG_WARNING)
        print("Reticulum Identity <" + identity.hash.hex() + "> was generated and saved to " + identity_file)
    else:
        if os.path.islink(identity_file):
            raise ValueError("identity file is a symlink, refusing to load: " + identity_file)
        try:
            mode = os.stat(identity_file).st_mode
            if mode & 0o077:
                RNS.log(
                    "identity file " + identity_file + " has overly permissive permissions "
                    "(expected 0600); consider running: chmod 600 " + identity_file,
                    RNS.LOG_WARNING,
                )
        except OSError as error:
            RNS.log("failed to stat identity file " + identity_file + ": " + str(error), RNS.LOG_WARNING)
        identity = RNS.Identity(create_keys=False)
        identity.load(identity_file)
        print("Reticulum Identity <" + identity.hash.hex() + "> was loaded from " + identity_file)
    return identity


def parse_lxmf_display_name(app_data_base64, default_value: Optional[str] = "Anonymous Peer") -> Optional[str]:
    try:
        app_data_bytes = base64.b64decode(app_data_base64, validate=True)
        display_name = LXMF.display_name_from_app_data(app_data_bytes)
        if display_name is not None:
            display_name = str(display_name)
            display_name = "".join(
                ch for ch in display_name if ch == "\t" or (ord(ch) >= 32 and ord(ch) != 127)
            )
            display_name = re.sub(r'\s+', ' ', display_name).strip()
            if not display_name:
                return default_value
            if len(display_name) > MAX_DISPLAY_NAME_LENGTH:
                display_name = display_name[:MAX_DISPLAY_NAME_LENGTH]
            return display_name
    except Exception as error:
        RNS.log("failed to parse LXMF display name: " + str(error), RNS.LOG_DEBUG)
    return default_value


class MeshChatCore:

    def __init__(self, identity=None, storage_dir=None, reticulum_config_dir=None):
        if storage_dir is not None:
            storage_dir = str(storage_dir)
            if not storage_dir or len(storage_dir) > 4096:
                raise ValueError("invalid storage_dir")
        self.storage_dir = storage_dir or os.path.join("storage")
        if identity is None:
            identity = load_or_create_identity(self.storage_dir)
        self.identity = identity
        self.event_callback = None

        try:
            identity_hex = identity.hash.hex()
        except Exception:
            raise ValueError("invalid identity object")
        if not re.fullmatch(r"[0-9a-fA-F]+", identity_hex or ""):
            raise ValueError("invalid identity hash")
        identity_hex = identity_hex.lower()
        self.storage_path = os.path.join(self.storage_dir, "identities", identity_hex)
        print("Using Storage Path: " + self.storage_path)
        os.makedirs(self.storage_path, mode=0o700, exist_ok=True)
        try:
            os.chmod(self.storage_path, 0o700)
        except OSError as error:
            RNS.log("failed to set permissions on storage path " + self.storage_path + ": " + str(error), RNS.LOG_WARNING)

        self.database_path = os.path.join(self.storage_path, "database.db")
        lxmf_router_path = os.path.join(self.storage_path, "lxmf_router")

        self.init_database()

        self.send_directories = self.get_send_directories()
        for send_dir in self.send_directories:
            os.makedirs(send_dir, exist_ok=True)

        self.reticulum = RNS.Reticulum(reticulum_config_dir)
        self.message_router = LXMF.LXMRouter(identity=self.identity, storagepath=lxmf_router_path)
        self.message_router.PROCESSING_INTERVAL = 1

        delivery_transfer_limit = self.config.lxmf_delivery_transfer_limit_in_bytes.get()
        if delivery_transfer_limit is not None:
            try:
                self.message_router.delivery_per_transfer_limit = delivery_transfer_limit / 1000
            except Exception as error:
                RNS.log("failed to set delivery per transfer limit: " + str(error), RNS.LOG_WARNING)

        self.local_lxmf_destination = self.message_router.register_delivery_identity(
            identity=self.identity,
            display_name=self.config.display_name.get(),
        )

        self.message_router.register_delivery_callback(self.message_received)

        self.set_active_propagation_node(self.config.lxmf_preferred_propagation_node_destination_hash.get())

        if self.config.lxmf_local_propagation_node_enabled.get():
            self.enable_local_propagation_node()

        RNS.Transport.register_announce_handler(AnnounceHandler("lxmf.delivery", self.on_lxmf_announce_received))
        RNS.Transport.register_announce_handler(AnnounceHandler("lxmf.propagation", self.on_lxmf_propagation_announce_received))

    def init_database(self):
        database_already_exists = os.path.exists(self.database_path)
        sqlite_database = SqliteDatabase(
            self.database_path,
            timeout=30,
            pragmas={
                "journal_mode": "wal",
                "foreign_keys": 1,
            },
        )
        database.database.initialize(sqlite_database)
        self.db = database.database
        self.db.connect()
        try:
            os.chmod(self.database_path, 0o600)
        except OSError as error:
            RNS.log("failed to set permissions on database file " + self.database_path + ": " + str(error), RNS.LOG_WARNING)
        for suffix in ("-wal", "-shm", "-journal"):
            try:
                sidecar = self.database_path + suffix
                if os.path.exists(sidecar):
                    os.chmod(sidecar, 0o600)
            except OSError as error:
                RNS.log("failed to set permissions on database sidecar file " + sidecar + ": " + str(error), RNS.LOG_WARNING)
        self.db.create_tables([
            database.Config,
            database.Announce,
            database.CustomDestinationDisplayName,
            database.FavouriteDestination,
            database.LxmfMessage,
            database.LxmfConversationReadState,
            database.LxmfUserIcon,
        ])

        self.config = Config()

        if database_already_exists and self.config.database_version.get() is None:
            self.config.database_version.set(1)

        if not database_already_exists:
            self.config.database_version.set(database.latest_version)

        current_database_version = self.config.database_version.get()
        migrated_database_version = database.migrate(current_version=current_database_version)
        self.config.database_version.set(migrated_database_version)

        self.db.execute_sql("VACUUM")

        database.LxmfMessage.update(state="failed").where(
            (database.LxmfMessage.state == "outbound") |
            ((database.LxmfMessage.state == "sent") & (database.LxmfMessage.method == "opportunistic")) |
            (database.LxmfMessage.state == "sending")
        ).execute()

    def set_event_callback(self, callback):
        self.event_callback = callback

    def _emit_event(self, event_type, payload):
        if self.event_callback is None:
            return
        try:
            self.event_callback(event_type, payload)
        except Exception as error:
            RNS.log("event callback error for " + str(event_type) + ": " + str(error), RNS.LOG_ERROR)

    def set_active_propagation_node(self, destination_hash):
        if destination_hash is not None and destination_hash != "":
            try:
                normalised = _validate_destination_hash(destination_hash)
            except ValueError:
                self.remove_active_propagation_node()
                return
            try:
                self.message_router.set_outbound_propagation_node(bytes.fromhex(normalised))
            except Exception:
                self.remove_active_propagation_node()
        else:
            self.remove_active_propagation_node()

    def stop_propagation_node_sync(self):
        try:
            self.message_router.cancel_propagation_node_requests()
        except Exception as error:
            RNS.log("failed to cancel propagation node requests: " + str(error), RNS.LOG_DEBUG)

    def remove_active_propagation_node(self):
        self.stop_propagation_node_sync()
        self.message_router.outbound_propagation_node = None

    def enable_local_propagation_node(self, enabled=True):
        try:
            if enabled:
                self.message_router.enable_propagation()
            else:
                self.message_router.disable_propagation()
        except Exception as e:
            print("failed to enable or disable propagation node: " + str(e))

    def announce(self):
        self.config.last_announced_at.set(int(time.time()))
        self.local_lxmf_destination.display_name = self.config.display_name.get()
        self.message_router.announce(destination_hash=self.local_lxmf_destination.hash)
        if self.config.lxmf_local_propagation_node_enabled.get():
            self.message_router.announce_propagation_node()

    def on_lxmf_announce_received(self, aspect, destination_hash, announced_identity, app_data, announce_packet_hash):
        try:
            self.db_upsert_announce(announced_identity, destination_hash, aspect, app_data, announce_packet_hash)
            self._emit_event("announce", {
                "aspect": aspect,
                "destination_hash": destination_hash.hex(),
            })
        except Exception as e:
            print("lxmf announce handler error: " + str(e))

    def on_lxmf_propagation_announce_received(self, aspect, destination_hash, announced_identity, app_data, announce_packet_hash):
        try:
            self.db_upsert_announce(announced_identity, destination_hash, aspect, app_data, announce_packet_hash)
            self._emit_event("announce", {
                "aspect": aspect,
                "destination_hash": destination_hash.hex(),
            })
        except Exception as e:
            print("lxmf propagation announce handler error: " + str(e))

    def message_received(self, lxmf_message):
        try:
            self.db_upsert_lxmf_message(lxmf_message)
            self._emit_event("message", self.convert_lxmf_message_to_dict(lxmf_message))
        except Exception as e:
            print("lxmf_delivery error: " + str(e))

    def delivery_receipt_received(self, lxmf_message):
        try:
            if lxmf_message.state == LXMF.LXMessage.FAILED and getattr(lxmf_message, "try_propagation_on_fail", False):
                self.send_failed_message_via_propagation_node(lxmf_message)
            self.db_upsert_lxmf_message(lxmf_message)
            self._emit_event("delivery_receipt", self.convert_lxmf_message_to_dict(lxmf_message))
        except Exception as e:
            print("delivery_receipt error: " + str(e))

    def send_failed_message_via_propagation_node(self, lxmf_message):
        lxmf_message.packed = None
        lxmf_message.delivery_attempts = 0
        if hasattr(lxmf_message, "next_delivery_attempt"):
            del lxmf_message.next_delivery_attempt
        lxmf_message.desired_method = LXMF.LXMessage.PROPAGATED
        lxmf_message.try_propagation_on_fail = False
        self.message_router.handle_outbound(lxmf_message)


    def convert_lxmf_message_to_dict(self, lxmf_message):
        fields = {}
        try:
            message_fields = lxmf_message.get_fields()
        except Exception:
            message_fields = {}
        if not isinstance(message_fields, dict):
            message_fields = {}
        for field_type in message_fields:
            try:
                value = message_fields[field_type]
            except Exception:
                continue
            if field_type == LXMF.FIELD_FILE_ATTACHMENTS:
                file_attachments = []
                if not isinstance(value, (list, tuple)):
                    continue
                for file_attachment in value:
                    try:
                        if not isinstance(file_attachment, (list, tuple)) or len(file_attachment) < 2:
                            continue
                        file_name = self.sanitize_filename(str(file_attachment[0]))
                        file_bytes_raw = file_attachment[1]
                        if not isinstance(file_bytes_raw, (bytes, bytearray)):
                            continue
                        if len(file_bytes_raw) > MAX_ATTACHMENT_SIZE:
                            continue
                        file_bytes = base64.b64encode(bytes(file_bytes_raw)).decode("utf-8")
                        file_attachments.append({"file_name": file_name, "file_bytes": file_bytes})
                    except Exception:
                        continue
                fields["file_attachments"] = file_attachments
            elif field_type == LXMF.FIELD_IMAGE:
                try:
                    if not isinstance(value, (list, tuple)) or len(value) < 2:
                        continue
                    image_type = str(value[0])[:64]
                    image_bytes_raw = value[1]
                    if not isinstance(image_bytes_raw, (bytes, bytearray)):
                        continue
                    if len(image_bytes_raw) > MAX_ATTACHMENT_SIZE:
                        continue
                    image_bytes = base64.b64encode(bytes(image_bytes_raw)).decode("utf-8")
                    fields["image"] = {"image_type": image_type, "image_bytes": image_bytes}
                except Exception:
                    continue

        try:
            progress_value = float(lxmf_message.progress)
            if progress_value != progress_value or progress_value < 0:  # NaN / negative guard
                raise ValueError("bad progress")
            progress_percentage = round(min(progress_value, 1.0) * 100, 2)
        except Exception:
            progress_percentage = 0

        try:
            rssi_value = lxmf_message.rssi
        except Exception:
            rssi_value = None
        try:
            snr_value = lxmf_message.snr
        except Exception:
            snr_value = None
        try:
            q_value = lxmf_message.q
        except Exception:
            q_value = None
        try:
            message_hash_for_metrics = lxmf_message.hash
        except Exception:
            message_hash_for_metrics = None

        rssi = self._safe_packet_metric(rssi_value, "get_packet_rssi", message_hash_for_metrics)
        snr = self._safe_packet_metric(snr_value, "get_packet_snr", message_hash_for_metrics)
        quality = self._safe_packet_metric(q_value, "get_packet_q", message_hash_for_metrics)

        try:
            title = lxmf_message.title.decode("utf-8", errors="replace")
        except Exception:
            title = ""
        title = str(title)[:MAX_TITLE_LENGTH]

        try:
            content = lxmf_message.content.decode("utf-8", errors="replace")
        except Exception:
            content = ""
        content = str(content)
        if len(content) > MAX_MESSAGE_CONTENT_LENGTH:
            content = content[:MAX_MESSAGE_CONTENT_LENGTH]

        try:
            source_hash = lxmf_message.source_hash.hex()
        except Exception:
            source_hash = ""

        try:
            destination_hash = lxmf_message.destination_hash.hex()
        except Exception:
            destination_hash = ""

        try:
            message_hash = lxmf_message.hash.hex()
        except Exception:
            message_hash = ""

        return {
            "hash": message_hash,
            "source_hash": source_hash,
            "destination_hash": destination_hash,
            "is_incoming": lxmf_message.incoming,
            "state": self.convert_lxmf_state_to_string(lxmf_message),
            "progress": progress_percentage,
            "method": self.convert_lxmf_method_to_string(lxmf_message),
            "delivery_attempts": getattr(lxmf_message, "delivery_attempts", 0),
            "next_delivery_attempt_at": getattr(lxmf_message, "next_delivery_attempt", None),
            "title": title,
            "content": content,
            "fields": fields,
            "timestamp": getattr(lxmf_message, "timestamp", 0),
            "rssi": rssi,
            "snr": snr,
            "quality": quality,
        }

    def _safe_packet_metric(self, current_value, method_name, packet_hash):
        if current_value is not None:
            return current_value
        try:
            return getattr(self.reticulum, method_name)(packet_hash)
        except Exception:
            return None

    def convert_lxmf_state_to_string(self, lxmf_message):
        try:
            lxmf_message_state = "unknown"
            if lxmf_message.state == LXMF.LXMessage.GENERATING:
                lxmf_message_state = "generating"
            elif lxmf_message.state == LXMF.LXMessage.OUTBOUND:
                lxmf_message_state = "outbound"
            elif lxmf_message.state == LXMF.LXMessage.SENDING:
                lxmf_message_state = "sending"
            elif lxmf_message.state == LXMF.LXMessage.SENT:
                lxmf_message_state = "sent"
            elif lxmf_message.state == LXMF.LXMessage.DELIVERED:
                lxmf_message_state = "delivered"
            elif lxmf_message.state == LXMF.LXMessage.REJECTED:
                lxmf_message_state = "rejected"
            elif lxmf_message.state == LXMF.LXMessage.CANCELLED:
                lxmf_message_state = "cancelled"
            elif lxmf_message.state == LXMF.LXMessage.FAILED:
                lxmf_message_state = "failed"
            return lxmf_message_state
        except Exception:
            return "unknown"

    def convert_lxmf_method_to_string(self, lxmf_message):
        try:
            lxmf_message_method = "unknown"
            if lxmf_message.method == LXMF.LXMessage.OPPORTUNISTIC:
                lxmf_message_method = "opportunistic"
            elif lxmf_message.method == LXMF.LXMessage.DIRECT:
                lxmf_message_method = "direct"
            elif lxmf_message.method == LXMF.LXMessage.PROPAGATED:
                lxmf_message_method = "propagated"
            elif lxmf_message.method == LXMF.LXMessage.PAPER:
                lxmf_message_method = "paper"
            return lxmf_message_method
        except Exception:
            return "unknown"

    def db_upsert_lxmf_message(self, lxmf_message):
        lxmf_message_dict = self.convert_lxmf_message_to_dict(lxmf_message)
        db_fields = {}
        for k, v in lxmf_message_dict["fields"].items():
            if k == "file_attachments":
                clean_attachments = []
                for att in v:
                    clean_att = {"file_name": att.get("file_name", "?")}
                    if "file_bytes" in att:
                        clean_att["size"] = len(att["file_bytes"])
                    clean_attachments.append(clean_att)
                db_fields[k] = clean_attachments
            elif k == "image":
                db_fields[k] = {"image_type": v.get("image_type", "unknown")}
                if "image_bytes" in v:
                    db_fields[k]["size"] = len(v["image_bytes"])
            else:
                db_fields[k] = v

        data = {
            "hash": lxmf_message_dict["hash"],
            "source_hash": lxmf_message_dict["source_hash"],
            "destination_hash": lxmf_message_dict["destination_hash"],
            "is_incoming": lxmf_message_dict["is_incoming"],
            "state": lxmf_message_dict["state"],
            "progress": lxmf_message_dict["progress"],
            "method": lxmf_message_dict["method"],
            "delivery_attempts": lxmf_message_dict["delivery_attempts"],
            "next_delivery_attempt_at": lxmf_message_dict["next_delivery_attempt_at"],
            "title": lxmf_message_dict["title"],
            "content": lxmf_message_dict["content"],
            "fields": json.dumps(db_fields),  # Сохраняем очищенные поля
            "timestamp": lxmf_message_dict["timestamp"],
            "rssi": lxmf_message_dict["rssi"],
            "snr": lxmf_message_dict["snr"],
            "quality": lxmf_message_dict["quality"],
            "updated_at": datetime.now(timezone.utc),
        }
        query = database.LxmfMessage.insert(data)
        query = query.on_conflict(conflict_target=[database.LxmfMessage.hash], update=data)
        query.execute()
    def db_upsert_announce(self, identity, destination_hash, aspect, app_data, announce_packet_hash):
        rssi = self._safe_packet_metric(None, "get_packet_rssi", announce_packet_hash)
        snr = self._safe_packet_metric(None, "get_packet_snr", announce_packet_hash)
        quality = self._safe_packet_metric(None, "get_packet_q", announce_packet_hash)
        try:
            destination_hash_hex = destination_hash.hex()
        except Exception:
            raise ValueError("invalid destination hash object")
        try:
            identity_hash_hex = identity.hash.hex()
            identity_public_key = base64.b64encode(identity.get_public_key()).decode("utf-8")
        except Exception:
            raise ValueError("invalid identity object")
        _validate_destination_hash(destination_hash_hex)
        aspect = str(aspect)[:128]
        data = {
            "destination_hash": destination_hash_hex,
            "aspect": aspect,
            "identity_hash": identity_hash_hex,
            "identity_public_key": identity_public_key,
            "rssi": rssi,
            "snr": snr,
            "quality": quality,
            "updated_at": datetime.now(timezone.utc),
        }
        if app_data is not None:
            if not isinstance(app_data, (bytes, bytearray)):
                raise ValueError("invalid app_data")
            if len(app_data) > 4096:
                raise ValueError("app_data too large")
            data["app_data"] = base64.b64encode(bytes(app_data)).decode("utf-8")
        query = database.Announce.insert(data)
        query = query.on_conflict(conflict_target=[database.Announce.destination_hash], update=data)
        query.execute()

    def db_upsert_custom_destination_display_name(self, destination_hash, display_name):
        destination_hash = _validate_destination_hash(destination_hash)
        display_name = _validate_display_name(display_name)
        data = {
            "destination_hash": destination_hash,
            "display_name": display_name,
            "updated_at": datetime.now(timezone.utc),
        }
        query = database.CustomDestinationDisplayName.insert(data)
        query = query.on_conflict(conflict_target=[database.CustomDestinationDisplayName.destination_hash], update=data)
        query.execute()

    def db_mark_lxmf_conversation_as_read(self, destination_hash):
        destination_hash = _validate_destination_hash(destination_hash)
        data = {
            "destination_hash": destination_hash,
            "last_read_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc),
        }
        query = database.LxmfConversationReadState.insert(data)
        query = query.on_conflict(conflict_target=[database.LxmfConversationReadState.destination_hash], update=data)
        query.execute()

    def get_peers(self, limit=100):
        try:
            limit = int(limit)
        except (TypeError, ValueError):
            limit = 100
        limit = max(1, min(limit, 1000))
        query = database.Announce.select().order_by(database.Announce.updated_at.desc()).limit(limit)
        peers = []
        for announce in query:
            display_name = None
            if announce.app_data:
                display_name = parse_lxmf_display_name(announce.app_data, None)
            updated_at = None
            if announce.updated_at:
                try:
                    updated_at = int(announce.updated_at.timestamp())
                except (AttributeError, TypeError, OSError):
                    try:
                        from datetime import datetime as dt
                        parsed = dt.fromisoformat(str(announce.updated_at))
                        updated_at = int(parsed.timestamp())
                    except Exception:
                        updated_at = None
            peers.append({
                "destination_hash": announce.destination_hash,
                "aspect": announce.aspect,
                "display_name": display_name,
                "updated_at": updated_at,
            })
        return peers

    def get_message_history(self, destination_hash):
        destination_hash = _validate_destination_hash(destination_hash)
        query = (database.LxmfMessage
                 .select()
                 .where(
                     (database.LxmfMessage.source_hash == destination_hash) |
                     (database.LxmfMessage.destination_hash == destination_hash))
                 .order_by(database.LxmfMessage.timestamp.asc()))
        history = []
        for message in query:
            history.append({
                "is_incoming": message.is_incoming,
                "state": message.state,
                "method": message.method,
                "content": message.content,
                "title": message.title,
                "fields": message.fields,
                "hash": message.hash,
                "timestamp": message.timestamp,
            })
        return history

    @staticmethod
    def sanitize_filename(file_name):
        file_name = str(file_name)[:MAX_FILENAME_LENGTH + 64]
        file_name = file_name.replace("\\", "/")
        file_name = os.path.basename(file_name)
        file_name = file_name.replace("\x00", "")
        file_name = "".join(
            ch for ch in file_name if ch not in "\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f\x7f"
        )
        file_name = file_name.strip().strip(".")
        if file_name in ("", ".", ".."):
            return "attachment"
        if len(file_name) > MAX_FILENAME_LENGTH:
            base_name, extension = os.path.splitext(file_name)
            extension = extension[:16]
            file_name = base_name[:MAX_FILENAME_LENGTH - len(extension)] + extension
        if file_name in ("", ".", ".."):
            return "attachment"
        return file_name

    def save_received_attachments(self, message_fields, save_dir):
        saved_paths = []
        if not isinstance(message_fields, dict):
            return saved_paths
        try:
            attachments = message_fields.get("file_attachments") or []
        except Exception:
            attachments = []
        if not isinstance(attachments, (list, tuple)):
            return saved_paths
        try:
            safe_save_dir = os.path.realpath(save_dir)
        except Exception:
            return saved_paths
        for attachment in attachments:
            try:
                if not isinstance(attachment, dict):
                    continue
                file_name = self.sanitize_filename(attachment.get("file_name", "attachment"))
                file_bytes = base64.b64decode(str(attachment.get("file_bytes", "")), validate=True)
            except Exception:
                continue
            if len(file_bytes) == 0 or len(file_bytes) > MAX_ATTACHMENT_SIZE:
                continue
            try:
                os.makedirs(safe_save_dir, mode=0o700, exist_ok=True)
                if len(saved_paths) >= 32:
                    break
                target_path = None
                base_name, extension = os.path.splitext(file_name)
                extension = extension[:16]
                for counter in range(0, 1000):
                    candidate = file_name if counter == 0 else f"{base_name}_{counter}{extension}"
                    candidate_path = os.path.join(safe_save_dir, candidate)
                    try:
                        if os.path.commonpath([safe_save_dir, os.path.realpath(candidate_path)]) != safe_save_dir:
                            continue
                    except ValueError:
                        continue
                    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
                    try:
                        fd = os.open(candidate_path, flags, 0o600)
                    except FileExistsError:
                        continue
                    except OSError:
                        break
                    try:
                        with os.fdopen(fd, "wb") as file:
                            file.write(file_bytes)
                    except BaseException:
                        try:
                            os.unlink(candidate_path)
                        except OSError as error:
                            RNS.log("failed to remove partially written file " + candidate_path + ": " + str(error), RNS.LOG_ERROR)
                        raise
                    try:
                        os.chmod(candidate_path, 0o600)
                    except OSError as error:
                        RNS.log("failed to set permissions on file " + candidate_path + ": " + str(error), RNS.LOG_WARNING)
                    target_path = candidate_path
                    break
                if target_path is None:
                    continue
                saved_paths.append(target_path)
            except Exception:
                continue
        return saved_paths

    async def send_message(self, destination_hash, content, delivery_method=None, fields=None, title=None):
        destination_hash = _validate_destination_hash(destination_hash)
        destination_bytes = bytes.fromhex(destination_hash)
        if content is None:
            content = ""
        content = str(content)
        if len(content) > MAX_MESSAGE_CONTENT_LENGTH:
            raise ValueError(
                "message too large (max %d characters)" % MAX_MESSAGE_CONTENT_LENGTH
            )
        if title is not None:
            title = str(title)[:MAX_TITLE_LENGTH]
        if fields is not None and not isinstance(fields, dict):
            raise ValueError("invalid fields")

        timeout_after_seconds = time.time() + 10
        if not RNS.Transport.has_path(destination_bytes):
            RNS.Transport.request_path(destination_bytes)
            while not RNS.Transport.has_path(destination_bytes) and time.time() < timeout_after_seconds:
                await asyncio.sleep(0.1)

        destination_identity = RNS.Identity.recall(destination_bytes)
        if destination_identity is None:
            raise Exception("could not recall destination identity. try again later.")

        lxmf_destination = RNS.Destination(destination_identity, RNS.Destination.OUT, RNS.Destination.SINGLE, "lxmf", "delivery")

        desired_delivery_method = None
        if delivery_method == "direct":
            desired_delivery_method = LXMF.LXMessage.DIRECT
        elif delivery_method == "opportunistic":
            desired_delivery_method = LXMF.LXMessage.OPPORTUNISTIC
        elif delivery_method == "propagated":
            desired_delivery_method = LXMF.LXMessage.PROPAGATED

        if desired_delivery_method is None:
            desired_delivery_method = LXMF.LXMessage.DIRECT
            try:
                if not self.message_router.delivery_link_available(destination_bytes) and RNS.Identity.current_ratchet_id(destination_bytes) is not None:
                    desired_delivery_method = LXMF.LXMessage.OPPORTUNISTIC
            except Exception as error:
                RNS.log("failed to check ratchet for opportunistic delivery: " + str(error), RNS.LOG_DEBUG)

        lxmf_message = LXMF.LXMessage(
            lxmf_destination,
            self.local_lxmf_destination,
            content,
            title=title,
            desired_method=desired_delivery_method,
            fields=fields,
        )
        lxmf_message.try_propagation_on_fail = self.config.auto_send_failed_messages_to_propagation_node.get()

        lxmf_message.register_delivery_callback(self.delivery_receipt_received)
        lxmf_message.register_failed_callback(self.delivery_receipt_received)

        self.message_router.handle_outbound(lxmf_message)
        self.db_upsert_lxmf_message(lxmf_message)

        return lxmf_message

    def get_send_directories(self):
        configured = None
        env_directories = os.environ.get(SEND_DIRECTORIES_ENV_VAR)
        if env_directories is not None:
            configured = env_directories
        else:
            configured = self.config.send_directories.get()

        directories = []
        if configured:
            directories = [d.strip() for d in configured.split(SEND_DIRECTORIES_SEPARATOR) if d.strip()]
        if not directories:
            directories = [os.path.join(self.storage_path, "attachments")]
        return [os.path.realpath(d) for d in directories]

    def resolve_allowed_file_path(self, file_path):
        if file_path is None or not str(file_path).strip():
            raise ValueError("invalid file path")
        path = str(file_path).strip().strip("'").strip('"')
        resolved_path = os.path.realpath(os.path.expanduser(path))
        if not os.path.isfile(resolved_path):
            raise ValueError("file not found: " + str(file_path))
        if not path_is_within_send_directory(resolved_path, self.get_send_directories()):
            raise ValueError("file is outside the allowed send directories: " + str(file_path))
        return resolved_path

    def read_sendable_file(self, resolved_path):
        open_flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NOCTTY", 0)
        fd = None
        try:
            fd = os.open(resolved_path, open_flags)
        except OSError as error:
            raise ValueError("could not open file: " + str(resolved_path)) from error

        try:
            file_stat = os.fstat(fd)
            if not stat.S_ISREG(file_stat.st_mode):
                raise ValueError("file is not a regular file: " + str(resolved_path))
            if not path_is_within_send_directory(resolved_path, self.get_send_directories()):
                raise ValueError("file is outside the allowed send directories: " + str(resolved_path))
            file_size = file_stat.st_size
            if file_size < 1:
                raise ValueError("file is empty: " + str(resolved_path))
            if file_size > MAX_ATTACHMENT_SIZE:
                raise ValueError("file is too large: " + str(resolved_path) + " (max " + str(MAX_ATTACHMENT_SIZE) + " bytes)")
            with os.fdopen(fd, "rb") as file:
                fd = None
                return file.read()
        finally:
            if fd is not None:
                try:
                    os.close(fd)
                except OSError as error:
                    RNS.log("failed to close file descriptor: " + str(error), RNS.LOG_DEBUG)

    async def send_file(self, destination_hash, file_path, title=None):
        resolved_path = self.resolve_allowed_file_path(file_path)

        try:
            file_bytes = await asyncio.to_thread(self.read_sendable_file, resolved_path)
        except ValueError:
            raise
        except Exception as error:
            raise ValueError("could not read file: " + str(file_path)) from error

        file_name = self.sanitize_filename(os.path.basename(resolved_path))
        if title is None:
            title = "file: " + file_name
        else:
            title = str(title)[:MAX_TITLE_LENGTH]
        fields = {LXMF.FIELD_FILE_ATTACHMENTS: [[file_name, file_bytes]]}
        return await self.send_message(destination_hash, title, fields=fields, title=title)

    def sync_with_propagation_node(self, destination_hash):
        destination_hash = _validate_destination_hash(destination_hash)
        destination_bytes = bytes.fromhex(destination_hash)
        self.set_active_propagation_node(destination_hash)
        self.config.lxmf_preferred_propagation_node_destination_hash.set(destination_hash)
        self.config.lxmf_preferred_propagation_node_last_synced_at.set(int(time.time()))
        self.message_router.request_messages_from_propagation_node(self.identity)

    async def announce_loop(self):
        while True:
            should_announce = False
            if self.config.auto_announce_enabled.get():
                last_announced_at = self.config.last_announced_at.get()
                if last_announced_at is None:
                    should_announce = True
                else:
                    interval_seconds = self.config.auto_announce_interval_seconds.get()
                    if interval_seconds and time.time() > last_announced_at + interval_seconds:
                        should_announce = True
            if should_announce:
                self.announce()
            await asyncio.sleep(1)

    def stop(self):
        try:
            RNS.Transport.detach_interfaces()
            self.reticulum.exit_handler()
            RNS.exit()
        except Exception as e:
            print("error while stopping core: " + str(e))
        try:
            self.db.close()
        except Exception as error:
            RNS.log("failed to close database: " + str(error), RNS.LOG_ERROR)